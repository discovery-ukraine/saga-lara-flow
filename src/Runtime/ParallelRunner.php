<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Builders\ParallelStepBuilder;
use DiscoveryUkraine\SagaLaraFlow\Contracts\ActionRunRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\ParallelFailurePolicy;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ActionFailedException;
use DiscoveryUkraine\SagaLaraFlow\Jobs\RunParallelActionJob;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Illuminate\Support\Facades\Bus;
use Throwable;

/**
 * Runs a parallel() block under the same replay model as a single action: it
 * assigns each step a deterministic (flow_run_id, sequence) ordinal, dispatches
 * them all together (a Bus::batch of RunParallelActionJob in queued mode, inline
 * in sync), then joins — suspending until every step is terminal, then returning
 * the ordered results or surfacing the first hard failure.
 *
 * The whole block shares one parallel group id (written to action_runs.parallel_group
 * and carried on each CompensationEntry), so completed steps roll back together as a
 * single level via SagaRunner::levels().
 */
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

        // Assign every step its deterministic sequence and resolve its recorded row.
        // expectAction enforces the history contract for the step's slot.
        $slots = [];

        foreach ($steps as $step) {
            $sequence = $runtime->nextSequence();
            $row = $this->guard->expectAction($flowRun->id, $sequence, $step->actionClass());

            $slots[] = ['step' => $step, 'sequence' => $sequence, 'row' => $row];
        }

        $anyScheduled = array_any($slots, fn (array $slot): bool => $slot['row'] !== null);

        // First encounter (nothing recorded yet): dispatch the whole block, then suspend.
        if (! $anyScheduled) {
            return $runtime->mode() === RunMode::Sync
                ? $this->dispatchSync($flowRun, $slots, $policy, $groupId)
                : $this->dispatchQueued($flowRun, $slots, $policy, $groupId);
        }

        return $this->join($runtime, $slots, $groupId);
    }

    /**
     * Sync mode: run each step inline. FailFast stops launching after the first hard
     * failure; WaitAllThenFail runs them all. We never throw here — suspendInline
     * replays so the join pushes completed steps' compensations before failing.
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
                    $step->actionClass(),
                    $step->arguments(),
                    $step->compensation() !== null,
                    $step->isOptional(),
                    $groupId,
                );
            } catch (Throwable $exception) {
                // Optional inline failure (no retries inline): give up now and keep going.
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
     * Queued mode: persist every step (pending) and dispatch them as one Bus::batch;
     * the finally callback resumes the workflow once exactly. Then suspend.
     *
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
        $jobs = [];

        foreach ($slots as $slot) {
            $step = $slot['step'];

            $actionRun = $this->recorder->scheduleAction(
                $flowRun,
                $slot['sequence'],
                $step->actionClass(),
                $step->arguments(),
                $step->compensation() !== null,
                $step->isOptional(),
                $groupId,
            );

            $jobs[] = new RunParallelActionJob($actionRun->id, $step->actionClass(), $policy);
        }

        $batch = Bus::batch($jobs)
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
     * Resolve every step from its recorded row (shared by the sync replay and the
     * queued resume): collect results, push completed steps' compensations, and
     * decide — wait if any step is still in flight, fail on the first hard failure,
     * otherwise return the ordered results.
     *
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
                // Not started (a FailFast sync break skipped it); the failure below
                // is what terminates the block, so this slot is irrelevant.
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
                    if ($this->shouldCompensateFailedStep($step)) {
                        $this->pushCompensation($runtime, $step, $row, $sequence, $groupId);
                    }

                    $hardFailed ??= [
                        'class' => $row->action_class,
                        'sequence' => $sequence,
                        'message' => $this->failureMessage($row),
                    ];

                    break;
                default:
                    // Pending / Running: still in flight.
                    $pending = true;
            }
        }

        if ($hardFailed !== null) {
            throw ActionFailedException::forAction(
                $hardFailed['class'],
                $hardFailed['sequence'],
                $hardFailed['message'],
            );
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
