<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use Closure;
use DateTimeInterface;
use DiscoveryUkraine\SagaLaraFlow\Concerns\NormalizesExceptions;
use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Data\ActionSchedule;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionAwaitingRetry;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionCompleted;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionFailed;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionOutcomeRejected;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionRedispatched;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionRetried;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionStarted;
use DiscoveryUkraine\SagaLaraFlow\Events\OptionalActionFailed;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\DB;
use Throwable;

final readonly class ActionRecorder
{
    use NormalizesExceptions;

    public function __construct(
        private EventLog $events,
        private Serializer $serializer,
        private AnomalyLog $anomalies,
    ) {}

    public function scheduleAction(FlowRun $flowRun, int $sequence, ActionSchedule $schedule): ActionRun
    {
        /** @var class-string<ActionRun> $model */
        $model = config('saga-lara-flow.models.action_run');

        $actionRun = new $model;

        $actionRun->fill([
            'flow_run_id' => $flowRun->id,
            'sequence' => $sequence,
            'action_class' => $schedule->actionClass,
            'action_name' => $schedule->actionName,
            'status' => ActionStatus::Pending,
            'has_compensation' => $schedule->hasCompensation,
            'continue_on_failure' => $schedule->continueOnFailure,
            'parallel_group' => $schedule->parallelGroup,
            'expires_at' => $schedule->expiresAt ?? $this->defaultExpiry(),
            'arguments' => $this->serializer->serialize($schedule->arguments),
            'attempts' => 0,
            'queue_attempts_exhausted' => false,
            'retry_signal' => $schedule->retrySignal,
            'retry_signal_attempts' => 0,
            'retry_signal_max_attempts' => $schedule->retrySignalMaxAttempts,
            'reclaim_stale_after_seconds' => $this->resolveReclaimStaleAfterSeconds(
                $schedule->reclaimStaleAfterSeconds,
                $schedule->reclaimStaleEnabled,
            ),
        ]);

        $actionRun->save();

        $this->events->record($flowRun, FlowEventType::ActionScheduled, $sequence, $actionRun, [
            'action_class' => $schedule->actionClass,
        ]);

        return $actionRun;
    }

    private function repairGrace(): int
    {
        return (int) config('saga-lara-flow.repair.grace_seconds', 60);
    }

    private function defaultExpiry(): ?DateTimeInterface
    {
        $seconds = config('saga-lara-flow.monitor.expiration.defaults.action');

        return $seconds === null ? null : Carbon::now()->addSeconds((int) $seconds);
    }

    /**
     * Decided once, at schedule time. A builder override wins over config in both directions.
     */
    private function resolveReclaimStaleAfterSeconds(?int $seconds, ?bool $enabled): ?int
    {
        if ($enabled === false) {
            return null;
        }

        if ($seconds !== null) {
            return $seconds;
        }

        if ($enabled === true) {
            return (int) config('saga-lara-flow.actions.reclaim.stale_running.after_seconds');
        }

        return config('saga-lara-flow.actions.reclaim.stale_running.enabled')
            ? (int) config('saga-lara-flow.actions.reclaim.stale_running.after_seconds')
            : null;
    }

    public function actionRedispatched(ActionRun $actionRun): void
    {
        $this->events->record(
            $actionRun->flowRun,
            FlowEventType::ActionRedispatched,
            $actionRun->sequence,
            $actionRun
        );

        event(new ActionRedispatched($actionRun));
    }

    /**
     * Failed is claimable: it is what the row shows between two of the action's native $tries. The
     * condition lives in the UPDATE, so no Eloquent model events are raised.
     *
     * @throws Throwable
     */
    public function startAction(ActionRun $actionRun, ?int $expectedRetryGeneration = null): bool
    {
        $claimed = $actionRun->getConnection()->transaction(
            function () use ($actionRun, $expectedRetryGeneration): bool {
                $now = Carbon::now();

                // Fixed at schedule time; the deadline derived from it moves, so it is compared in SQL.
                $staleAfter = $actionRun->reclaim_stale_after_seconds;

                $claimed = $actionRun->newQuery()
                    ->whereKey($actionRun->getKey())
                    ->whereHas('flowRun', FlowRun::mayStartWork(...))
                    ->when(
                        $expectedRetryGeneration !== null,
                        fn ($query) => $query->where('retry_signal_attempts', $expectedRetryGeneration),
                    )
                    ->where(function ($query) use ($now): void {
                        $query->whereIn('status', [ActionStatus::Pending, ActionStatus::Failed])
                            ->orWhere(function ($query) use ($now): void {
                                $query->where('status', ActionStatus::Running)
                                    ->whereNotNull('reclaim_stale_at')
                                    ->where('reclaim_stale_at', '<=', $now);
                            });
                    })
                    ->update([
                        'status' => ActionStatus::Running,
                        'attempts' => DB::raw('attempts + 1'),
                        'started_at' => $now,
                        'reclaim_stale_at' => $staleAfter === null
                            ? null
                            : $now->copy()->addSeconds($staleAfter),
                    ]) === 1;

                if (! $claimed) {
                    $this->anomalies->log(AnomalyLog::REASON_CLAIM_LOST, [
                        'entity' => 'action',
                        'flow_run_id' => $actionRun->flow_run_id,
                        'action_run_id' => $actionRun->id,
                        'sequence' => $actionRun->sequence,
                        'action_class' => $actionRun->action_class,
                        'expected_retry_generation' => $expectedRetryGeneration,
                    ]);

                    return false;
                }

                $actionRun->refresh();

                $this->events->record(
                    $actionRun->flowRun,
                    FlowEventType::ActionStarted,
                    $actionRun->sequence,
                    $actionRun
                );

                event(new ActionStarted($actionRun));

                return true;
            }
        );

        return $claimed && $this->claimSurvivedCommit($actionRun);
    }

    /**
     * On PostgreSQL a failure swallowed inside the transaction turns its COMMIT into a rollback. The
     * row answers, not a token, so a claim lost to a second worker after commit is only narrowed.
     */
    private function claimSurvivedCommit(ActionRun $actionRun): bool
    {
        $claimedAttempts = $actionRun->attempts;

        $stored = $actionRun->newQuery()
            ->useWritePdo()
            ->whereKey($actionRun->getKey())
            ->first(['status', 'attempts']);

        if ($stored?->status === ActionStatus::Running && $stored->attempts === $claimedAttempts) {
            return true;
        }

        // `attempts` only grows, so a stored count below this claim's can only be the claim undone.
        $this->anomalies->log(
            $stored === null || $stored->attempts < $claimedAttempts
                ? AnomalyLog::REASON_CLAIM_NOT_COMMITTED
                : AnomalyLog::REASON_CLAIM_LOST,
            [
                'entity' => 'action',
                'flow_run_id' => $actionRun->flow_run_id,
                'action_run_id' => $actionRun->id,
                'sequence' => $actionRun->sequence,
                'action_class' => $actionRun->action_class,
                'claimed_attempts' => $claimedAttempts,
                'stored_attempts' => $stored?->attempts,
                'stored_status' => $stored?->status->value,
            ]
        );

        return false;
    }

    public function completeAction(ActionRun $actionRun, mixed $result): bool
    {
        $claimedAttempts = $actionRun->attempts;

        $actionRun->status = ActionStatus::Completed;
        $actionRun->result = $this->serializer->serialize($result);
        $actionRun->finished_at = Carbon::now();

        if (! $this->writeOutcome($actionRun, $claimedAttempts, FlowEventType::ActionCompleted, result: $actionRun->result)) {
            return false;
        }

        $this->events->record(
            $actionRun->flowRun,
            FlowEventType::ActionCompleted,
            $actionRun->sequence,
            $actionRun
        );

        event(new ActionCompleted($actionRun));

        return true;
    }

    public function failAction(ActionRun $actionRun, Throwable $exception, bool $inline = false): bool
    {
        $claimedAttempts = $actionRun->attempts;
        $exceptionArray = $this->exceptionToArray($exception);

        $actionRun->status = ActionStatus::Failed;
        $actionRun->exception = $exceptionArray;
        $actionRun->finished_at = Carbon::now();

        // No job retries an inline attempt, so a replay must not wait for one.
        if ($inline) {
            $actionRun->queue_attempts_exhausted = true;
        }

        if (! $this->writeOutcome($actionRun, $claimedAttempts, FlowEventType::ActionFailed, exception: $exception)) {
            return false;
        }

        $this->events->record(
            $actionRun->flowRun,
            FlowEventType::ActionFailed,
            $actionRun->sequence,
            $actionRun,
            [
                'exception' => $exceptionArray,
            ]
        );

        event(new ActionFailed($actionRun, $exception));

        return true;
    }

    /**
     * `attempts` fences against a reclaiming worker; `status` against a transition that never
     * claimed the row, such as the monitor expiring it.
     */
    private function writeOutcome(
        ActionRun $actionRun,
        int $claimedAttempts,
        FlowEventType $outcome,
        mixed $result = null,
        ?Throwable $exception = null,
    ): bool {
        $written = $actionRun->newQuery()
            ->whereKey($actionRun->getKey())
            ->where('attempts', $claimedAttempts)
            ->where('status', ActionStatus::Running)
            ->update($actionRun->getDirty()) === 1;

        if (! $written) {
            $this->anomalies->log(AnomalyLog::REASON_OUTCOME_REJECTED, [
                'entity' => 'action',
                'flow_run_id' => $actionRun->flow_run_id,
                'action_run_id' => $actionRun->id,
                'sequence' => $actionRun->sequence,
                'action_class' => $actionRun->action_class,
                'outcome' => $outcome->value,
                'claimed_attempts' => $claimedAttempts,
            ]);

            // The log line carries no payload: the default channel was not chosen for business data.
            $actionRun->discardChanges();

            $this->announceRejection(
                new ActionOutcomeRejected($actionRun, $outcome, $result, $exception),
                $actionRun,
            );

            return false;
        }

        $actionRun->syncOriginal();

        return true;
    }

    /**
     * A listener that throws must not fail the job: failed() would write into a row this worker no
     * longer owns.
     */
    private function announceRejection(ActionOutcomeRejected $rejection, ActionRun $actionRun): void
    {
        try {
            event($rejection);
        } catch (Throwable $undelivered) {
            $this->anomalies->log(AnomalyLog::REASON_REJECTION_UNDELIVERED, [
                'entity' => 'action',
                'flow_run_id' => $actionRun->flow_run_id,
                'action_run_id' => $actionRun->id,
                'sequence' => $actionRun->sequence,
                'action_class' => $actionRun->action_class,
                'outcome' => $rejection->outcome->value,
                'exception' => $undelivered::class,
                'message' => $undelivered->getMessage(),
            ]);
        }
    }

    /**
     * Every $expected key the write also sets names the value being replaced, so a matching row
     * always changes and MySQL reports it.
     *
     * @param  array<string, mixed|list<mixed>>  $expected
     * @param  array<string, mixed>  $context
     * @param  (Closure(Builder<Model>): void)|null  $runFence
     */
    private function writeFenced(
        ActionRun $actionRun,
        array $expected,
        string $site,
        array $context = [],
        ?Closure $runFence = null,
    ): bool {
        $query = $actionRun->newQuery()
            ->whereKey($actionRun->getKey())
            ->whereHas('flowRun', $runFence ?? FlowRun::live(...));

        foreach ($expected as $column => $value) {
            match (true) {
                is_array($value) => $query->whereIn($column, $value),
                $value === null => $query->whereNull($column),
                default => $query->where($column, $value),
            };
        }

        $written = $query->update($actionRun->getDirty()) === 1;

        if (! $written) {
            $this->anomalies->log(AnomalyLog::REASON_WRITE_REFUSED, [
                'entity' => 'action',
                'site' => $site,
                'flow_run_id' => $actionRun->flow_run_id,
                'action_run_id' => $actionRun->id,
                'sequence' => $actionRun->sequence,
                'action_class' => $actionRun->action_class,
                ...$context,
            ]);

            // The caller resolves the step from this instance, so it says what the database says.
            $actionRun->discardChanges();

            return false;
        }

        $actionRun->syncOriginal();

        return true;
    }

    public function optionalFail(ActionRun $actionRun): bool
    {
        // The row as read, cycle included: a row can leave Failed for a retry and come back to it.
        $asRead = [
            'status' => $actionRun->getOriginal('status'),
            'retry_signal_attempts' => $actionRun->getOriginal('retry_signal_attempts'),
        ];

        $actionRun->status = ActionStatus::OptionalFailed;
        $actionRun->finished_at = Carbon::now();

        if (! $this->writeFenced($actionRun, $asRead, 'optional_fail')) {
            return false;
        }

        $this->events->record(
            $actionRun->flowRun,
            FlowEventType::ActionOptionalFailed,
            $actionRun->sequence,
            $actionRun,
            $actionRun->exception !== null ? ['exception' => $actionRun->exception] : []
        );

        event(new OptionalActionFailed($actionRun));

        return true;
    }

    /**
     * Read by the retry seam instead of comparing attempts with $tries, which can change under a
     * job in flight. Fenced on the cycle the caller read.
     */
    public function markQueueAttemptsExhausted(ActionRun $actionRun): bool
    {
        if ($actionRun->queue_attempts_exhausted) {
            return false;
        }

        $actionRun->queue_attempts_exhausted = true;

        return $this->writeFenced($actionRun, [
            'queue_attempts_exhausted' => false,
            // A claim does not move the cycle, so the status tells a spent Failed view from a live attempt.
            'status' => $actionRun->getOriginal('status'),
            'retry_signal_attempts' => $actionRun->getOriginal('retry_signal_attempts'),
        ], 'queue_attempts_exhausted');
    }

    /**
     * Fenced on the parked row: settlement may have closed it as Cancelled, and Failed written over
     * that would reopen it. No event: action.failed was recorded when the attempt failed.
     */
    public function settleAwaitingRetry(ActionRun $actionRun): bool
    {
        if ($actionRun->status !== ActionStatus::AwaitingRetry) {
            return false;
        }

        $actionRun->status = ActionStatus::Failed;

        return $this->writeFenced(
            $actionRun,
            ['status' => ActionStatus::AwaitingRetry],
            'settle_awaiting_retry',
        );
    }

    /**
     * Only `status`, and no event: the run's terminal event records the moment and the cause.
     */
    public function settleOpenSteps(FlowRun $flowRun): int
    {
        /** @var class-string<ActionRun> $model */
        $model = config('saga-lara-flow.models.action_run');

        return $model::query()
            ->where('flow_run_id', $flowRun->id)
            ->whereIn('status', [
                ActionStatus::Pending,
                ActionStatus::Running,
                ActionStatus::AwaitingRetry,
            ])
            ->update(['status' => ActionStatus::Cancelled]);
    }

    /**
     * Fenced on the failure and its cycle: a park after terminal settlement would wait for ever.
     * Returns the row for the caller to read back once its transaction closes; null when refused.
     */
    public function awaitRetry(ActionRun $actionRun, string $signal, ?int $maxAttempts = null): ?Model
    {
        $asRead = [
            'status' => $actionRun->getOriginal('status'),
            'retry_signal_attempts' => $actionRun->getOriginal('retry_signal_attempts'),
        ];

        $actionRun->status = ActionStatus::AwaitingRetry;
        $actionRun->retry_signal = $signal;

        // From here on the budget is read off the row, where an empty column means unbounded.
        $actionRun->retry_signal_max_attempts ??= $maxAttempts;

        if (! $this->writeFenced($actionRun, $asRead, 'await_retry')) {
            return null;
        }

        return $this->events->record(
            $actionRun->flowRun,
            FlowEventType::ActionAwaitingRetry,
            $actionRun->sequence,
            $actionRun,
            [
                'signal' => $signal,
                'retry_signal_attempts' => $actionRun->retry_signal_attempts,
                'retry_signal_max_attempts' => $actionRun->retry_signal_max_attempts,
            ]
        );
    }

    public function publishAwaitingRetry(ActionRun $actionRun, string $signal): void
    {
        event(new ActionAwaitingRetry($actionRun, $signal));
    }

    /**
     * `attempts` is untouched: it counts queue attempts within one cycle.
     */
    public function retryAction(
        ActionRun $actionRun,
        ?DateTimeInterface $expiresAt = null,
        bool $publish = true
    ): bool {
        $asRead = [
            'status' => $actionRun->getOriginal('status'),
            'retry_signal_attempts' => $actionRun->getOriginal('retry_signal_attempts'),
        ];

        $actionRun->retry_signal_attempts = $actionRun->retry_signal_attempts + 1;
        $actionRun->status = ActionStatus::Pending;
        $actionRun->started_at = null;
        $actionRun->finished_at = null;

        $actionRun->reclaim_stale_at = null;

        $actionRun->queue_attempts_exhausted = false;

        // The doctor reads a reused row's created_at as long past, so the hold keeps it off this job;
        // the count restarts so a budget spent on earlier cycles does not deny this one.
        $actionRun->repair_attempts = 0;
        $actionRun->repair_available_at = Carbon::now()->addSeconds($this->repairGrace());

        $deadline = $expiresAt ?? $this->defaultExpiry();

        $actionRun->expires_at = $deadline === null ? null : Carbon::instance($deadline);

        if (! $this->writeFenced($actionRun, $asRead, 'retry_action', runFence: FlowRun::mayStartWork(...))) {
            return false;
        }

        if ($publish) {
            $this->publishRetried($actionRun);
        }

        return true;
    }

    /**
     * Separate from the write: a caller inside its own transaction publishes once it has read the
     * rewind back, since on PostgreSQL a commit can report success over a rollback.
     */
    public function publishRetried(ActionRun $actionRun): void
    {
        $this->events->record(
            $actionRun->flowRun,
            FlowEventType::ActionRetried,
            $actionRun->sequence,
            $actionRun,
            [
                'signal' => $actionRun->retry_signal,
                'retry_signal_attempts' => $actionRun->retry_signal_attempts,
                'retry_signal_max_attempts' => $actionRun->retry_signal_max_attempts,
            ]
        );

        event(new ActionRetried($actionRun));
    }

    /**
     * Conditional: a worker can complete the row between the sweep's read and this write.
     *
     * @param  array<string, mixed>  $exception
     */
    public function expireAction(ActionRun $actionRun, array $exception): bool
    {
        $actionRun->status = ActionStatus::Expired;
        $actionRun->exception = $exception;
        $actionRun->finished_at = Carbon::now();

        $expired = $actionRun->newQuery()
            ->whereKey($actionRun->getKey())
            ->whereIn('status', [ActionStatus::Pending, ActionStatus::Running])
            ->update($actionRun->getDirty()) === 1;

        if (! $expired) {
            $this->anomalies->log(AnomalyLog::REASON_OUTCOME_REJECTED, [
                'entity' => 'action',
                'flow_run_id' => $actionRun->flow_run_id,
                'action_run_id' => $actionRun->id,
                'sequence' => $actionRun->sequence,
                'action_class' => $actionRun->action_class,
                'outcome' => FlowEventType::ActionExpired->value,
            ]);

            return false;
        }

        $actionRun->syncOriginal();

        $this->events->record(
            $actionRun->flowRun,
            FlowEventType::ActionExpired,
            $actionRun->sequence,
            $actionRun,
            ['exception' => $exception],
        );

        return true;
    }
}
