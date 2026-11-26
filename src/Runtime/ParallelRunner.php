<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Builders\ParallelStepBuilder;
use DiscoveryUkraine\SagaLaraFlow\Contracts\ActionRunRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Data\ActionSchedule;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\ParallelFailurePolicy;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ActionFailedException;
use DiscoveryUkraine\SagaLaraFlow\Jobs\RunParallelActionJob;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Illuminate\Support\Facades\Bus;
use Throwable;

final readonly class ParallelRunner
{
    public function __construct(
        private HistoryContractGuard $guard,
        private ActionDispatcher $dispatcher,
        private ActionRecorder $recorder,
        private ActionRunRepository $actionRepository,
        private FlowSuspender $suspender,
        private Serializer $serializer,
    ) {}

    /**
     * @param  list<ParallelStepBuilder>  $steps
     * @return list<mixed>
     *
     * @throws Throwable
     */
    public function run(FlowRuntime $runtime, array $steps, ParallelFailurePolicy $policy): array
    {
        if ($steps === []) {
            return [];
        }

        $flowRun = $runtime->run();
        $groupId = $runtime->nextSagaGroupId();

        $slots = [];

        foreach ($steps as $step) {
            $sequence = $runtime->nextSequence();
            $row = $this->guard->expectAction($flowRun->id, $sequence, $step->actionClass());

            $slots[] = ['step' => $step, 'sequence' => $sequence, 'row' => $row];
        }

        $anyScheduled = array_any($slots, fn (array $slot): bool => $slot['row'] !== null);

        if (! $anyScheduled) {
            // Compensation-only planning never starts new work: stop at the frontier.
            if ($runtime->isCollecting()) {
                $this->suspender->suspend('parallel', $slots[0]['sequence']);
            }

            return match (true) {
                $runtime->mode() === RunMode::Sync => $this->dispatchSync($flowRun, $slots, $policy, $groupId),
                $this->onSyncConnection($flowRun) => $this->dispatchEachOnSync($flowRun, $slots, $policy, $groupId),
                default => $this->dispatchQueued($flowRun, $slots, $policy, $groupId),
            };
        }

        return $this->join($runtime, $slots, $groupId);
    }

    /**
     * Never throws: the replay's join pushes completed steps' compensations before failing.
     *
     * @param  list<array{step: ParallelStepBuilder, sequence: int, row: ?ActionRun}>  $slots
     * @return list<mixed>
     *
     * @throws Throwable
     */
    private function dispatchSync(
        FlowRun $flowRun,
        array $slots,
        ParallelFailurePolicy $policy,
        int $groupId
    ): array {
        foreach ($slots as $slot) {
            $step = $slot['step'];

            try {
                $this->dispatcher->runInline(
                    $flowRun,
                    $slot['sequence'],
                    $this->scheduleFor($step, $groupId),
                );
            } catch (Throwable $exception) {
                if ($step->isOptional()) {
                    $this->markOptionalFailed($flowRun->id, $slot['sequence']);

                    continue;
                }

                if ($policy === ParallelFailurePolicy::FailFast) {
                    break;
                }
            }
        }

        $this->suspender->suspendInline('parallel', $slots[0]['sequence']);
    }

    /**
     * @param  list<array{step: ParallelStepBuilder, sequence: int, row: ?ActionRun}>  $slots
     * @return list<mixed>
     *
     * @throws Throwable
     */
    private function dispatchQueued(
        FlowRun $flowRun,
        array $slots,
        ParallelFailurePolicy $policy,
        int $groupId
    ): array {
        $batch = Bus::batch($this->scheduleMembers($flowRun, $slots, $policy, $groupId))
            ->name("parallel:{$flowRun->id}:{$groupId}")
            ->allowFailures()
            ->finally(new ResumeParallelBlock($flowRun->id));

        if ($flowRun->connection !== null) {
            $batch->onConnection($flowRun->connection);
        }

        if ($flowRun->queue !== null) {
            $batch->onQueue($flowRun->queue);
        }

        $batch->dispatch();

        $this->suspender->suspend('parallel', $slots[0]['sequence']);
    }

    /**
     * A sync queue runs a batch inside one transaction of its own and stops at the first
     * member that throws, so each member goes as its own job and the replay joins them.
     *
     * @param  list<array{step: ParallelStepBuilder, sequence: int, row: ?ActionRun}>  $slots
     * @return list<mixed>
     *
     * @throws Throwable
     */
    private function dispatchEachOnSync(
        FlowRun $flowRun,
        array $slots,
        ParallelFailurePolicy $policy,
        int $groupId
    ): array {
        foreach ($this->scheduleMembers($flowRun, $slots, $policy, $groupId) as $job) {
            if ($flowRun->connection !== null) {
                $job->onConnection($flowRun->connection);
            }

            if ($flowRun->queue !== null) {
                $job->onQueue($flowRun->queue);
            }

            try {
                Bus::dispatch($job);
            } catch (Throwable $thrown) {
                $status = $this->recordedFailure($job->actionRunId);

                if ($status === null) {
                    throw $thrown;
                }

                if ($status === ActionStatus::Failed && $policy === ParallelFailurePolicy::FailFast) {
                    break;
                }
            }
        }

        $this->suspender->suspendInline('parallel', $slots[0]['sequence']);
    }

    /**
     * @param  list<array{step: ParallelStepBuilder, sequence: int, row: ?ActionRun}>  $slots
     * @return list<RunParallelActionJob>
     */
    private function scheduleMembers(
        FlowRun $flowRun,
        array $slots,
        ParallelFailurePolicy $policy,
        int $groupId
    ): array {
        $jobs = [];

        foreach ($slots as $slot) {
            $step = $slot['step'];

            $actionRun = $this->recorder->scheduleAction(
                $flowRun,
                $slot['sequence'],
                $this->scheduleFor($step, $groupId),
            );

            $jobs[] = new RunParallelActionJob($actionRun->id, $step->actionClass(), $policy);
        }

        return $jobs;
    }

    private function onSyncConnection(FlowRun $flowRun): bool
    {
        $connection = $flowRun->connection ?? config('queue.default');

        return config("queue.connections.{$connection}.driver") === 'sync';
    }

    private function recordedFailure(string $actionRunId): ?ActionStatus
    {
        /** @var class-string<ActionRun> $model */
        $model = config('saga-lara-flow.models.action_run');

        $row = $model::query()->useWritePdo()->find($actionRunId);

        return match ($row?->status) {
            ActionStatus::OptionalFailed => ActionStatus::OptionalFailed,
            ActionStatus::Failed => $row->continue_on_failure ? null : ActionStatus::Failed,
            default => null,
        };
    }

    /**
     * @param  list<array{step: ParallelStepBuilder, sequence: int, row: ?ActionRun}>  $slots
     * @return list<mixed>
     *
     * @throws Throwable
     */
    private function join(FlowRuntime $runtime, array $slots, int $groupId): array
    {
        $results = [];
        $pending = false;

        /** @var array{class: string, sequence: int, message: string}|null $hardFailed */
        $hardFailed = null;

        foreach ($slots as $index => $slot) {
            $step = $slot['step'];
            $sequence = $slot['sequence'];
            $row = $slot['row'];

            if ($row === null) {
                // Skipped by a FailFast break; the failure below ends the block.
                $pending = true;

                continue;
            }

            switch ($row->status) {
                case ActionStatus::Completed:
                    $this->pushCompensation($runtime, $step, $row, $sequence, $groupId);
                    $results[$index] = $this->serializer->deserialize($row->result);

                    break;
                case ActionStatus::OptionalFailed:
                    if ($this->shouldCompensateFailedStep($step)) {
                        $this->pushCompensation($runtime, $step, $row, $sequence, $groupId);
                    }

                    $results[$index] = $step->fallbackResult();

                    break;
                case ActionStatus::Failed:
                case ActionStatus::Expired:
                    if ($this->shouldCompensateFailedStep($step)) {
                        $this->pushCompensation($runtime, $step, $row, $sequence, $groupId);
                    }

                    if ($row->status === ActionStatus::Expired && $step->isOptional()) {
                        $results[$index] = $step->fallbackResult();

                        break;
                    }

                    $hardFailed ??= [
                        'class' => $row->action_class,
                        'sequence' => $sequence,
                        'message' => $this->failureMessage($row),
                    ];

                    break;
                default:
                    $pending = true;
            }
        }

        if ($hardFailed !== null) {
            throw $runtime->raising(ActionFailedException::forAction(
                $hardFailed['class'],
                $hardFailed['sequence'],
                $hardFailed['message'],
            ));
        }

        if ($pending) {
            $this->suspender->suspend('parallel', $slots[0]['sequence']);
        }

        ksort($results);

        return array_values($results);
    }

    private function pushCompensation(
        FlowRuntime $runtime,
        ParallelStepBuilder $step,
        ActionRun $row,
        int $sequence,
        int $groupId
    ): void {
        $definition = $step->compensation();

        if ($definition === null) {
            return;
        }

        $runtime->sagaStack()->push(new CompensationEntry(
            $row->id,
            $sequence,
            $definition,
            $step->compensationFailurePolicy(),
            null,
            $groupId,
        ));
    }

    private function shouldCompensateFailedStep(ParallelStepBuilder $step): bool
    {
        return $step->compensateOnSelfFailure()
            ?? (bool) config('saga-lara-flow.sagas.compensate_failed_step');
    }

    private function scheduleFor(ParallelStepBuilder $step, int $groupId): ActionSchedule
    {
        return new ActionSchedule(
            actionClass: $step->actionClass(),
            arguments: $step->arguments(),
            hasCompensation: $step->compensation() !== null,
            continueOnFailure: $step->isOptional(),
            parallelGroup: $groupId,
            expiresAt: $step->expiry(),
        );
    }

    private function markOptionalFailed(string $flowRunId, int $sequence): void
    {
        $step = $this->actionRepository->find($flowRunId, $sequence);

        if ($step !== null) {
            $this->recorder->optionalFail($step);
        }
    }

    private function failureMessage(ActionRun $step): string
    {
        $exception = $step->exception;

        if (is_array($exception) && isset($exception['message']) && is_string($exception['message'])) {
            return $exception['message'];
        }

        return 'unknown error';
    }
}
