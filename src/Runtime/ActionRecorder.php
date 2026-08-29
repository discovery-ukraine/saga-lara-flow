<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DateTimeInterface;
use DiscoveryUkraine\SagaLaraFlow\Concerns\NormalizesExceptions;
use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Data\ActionSchedule;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionAwaitingRetry;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionCompleted;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionFailed;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionRedispatched;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionRetried;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionStarted;
use DiscoveryUkraine\SagaLaraFlow\Events\OptionalActionFailed;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\DB;
use Throwable;

/**
 * Persists an action step through its lifecycle (scheduled → started → completed
 * /failed), serializing arguments and results and appending the matching events.
 * It also settles the steps a finished run leaves behind (settleOpenSteps).
 */
final readonly class ActionRecorder
{
    use NormalizesExceptions;

    public function __construct(
        private EventLog $events,
        private Serializer $serializer,
        private AnomalyLog $anomalies,
    ) {}

    /**
     * Create the pending ActionRun for a scheduled step. The arguments are
     * serialized once here and become the durable source the executing job
     * (or inline run) reads back.
     */
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

    /**
     * How long the doctor leaves a freshly dispatched action alone before calling its
     * job lost.
     */
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
     * Resolve the per-row reclaim-stale threshold at schedule time, mirroring
     * defaultExpiry()'s "decide once, persist the concrete value" shape. An explicit
     * builder override wins over config in both directions: enabled === false forces
     * the mechanism off for this row regardless of config; enabled === true forces it
     * on using config's after_seconds when no explicit $seconds was also given. With
     * neither override, the row simply inherits config as it stands.
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

    /**
     * Record the doctor re-dispatching a stuck Pending action. The
     * action keeps its status/sequence — only a fresh RunActionJob is sent — so an
     * action.redispatched event is appended for visibility without altering history.
     */
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
     * Atomically claim the row and move it to Running, returning false when the row
     * is no longer claimable and the caller must not execute the step.
     *
     * The condition lives in the UPDATE itself — the same compare-and-swap shape as
     * SignalRecorder::claimWaiting(), the only form every supported driver enforces
     * atomically (lockForUpdate() compiles to nothing on SQLite). This transition
     * therefore raises no Eloquent model events; ActionStarted and the action.started
     * flow_events entry are the supported way to observe it.
     *
     * Claimable statuses are Pending, Failed, and Running past its reclaim deadline.
     * Failed is not terminal here: it is what the row shows between two of the
     * action's own native $tries, which Laravel redelivers as the very same job.
     *
     * `attempts` is incremented by the database, so two racing claims can never
     * persist the same count, and the value a claim produces identifies it.
     *
     * $expectedRetryGeneration constrains retry_signal_attempts, folding a caller's
     * stale-cycle check into this same atomic write so a cycle change landing after
     * the row was read still loses the claim. Null imposes no constraint.
     *
     * The claim and its two records share one transaction: a listener throwing on
     * ActionStarted would otherwise leave the row Running with nothing executing it.
     * A commit is not proof the claim survived one, so the row is read back afterwards
     * — see claimSurvivedCommit().
     *
     * @throws Throwable
     */
    public function startAction(ActionRun $actionRun, ?int $expectedRetryGeneration = null): bool
    {
        $claimed = $actionRun->getConnection()->transaction(
            function () use ($actionRun, $expectedRetryGeneration): bool {
                $now = Carbon::now();

                // Fixed at schedule time and never rewritten, so reading it off the
                // model is safe — unlike the deadline derived from it, which moves with
                // every claim and is therefore compared in the database.
                $staleAfter = $actionRun->reclaim_stale_after_seconds;

                $claimed = $actionRun->newQuery()
                    ->whereKey($actionRun->getKey())
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
     * Whether the claim is on record now the transaction that made it has closed. A
     * commit reporting success is not proof: PostgreSQL aborts a transaction on the
     * first failed statement and turns the eventual COMMIT into a rollback, reporting
     * success either way, so any caller code running inside it — a listener on
     * ActionStarted, a model observer on the flow_events insert — that runs a failing
     * query and swallows it discards the claim while the caller is told it holds one.
     * The row is the only answer that covers every such shape: the claim is visible or
     * it is not.
     *
     * Refusing a row a second worker legitimately took between the commit and this read
     * is the same answer for the same reason — it is no longer ours to execute. The row
     * is what answers, though, not a token: a claim of ours that rolled back and a second
     * worker's claim that replaced it read alike, so a claim lost to that race is
     * narrowed rather than caught. Telling those two apart needs a value per claim that a
     * rollback cannot reproduce, which the schema does not carry.
     *
     * What the read proves is that the claim is visible on the connection that wrote it,
     * which is durability only while the engine's transaction is the outermost one. A
     * host transaction wrapped around a run can still discard every row the run recorded
     * with its side effects already spent — on any driver, and whatever this check said —
     * which is why the documentation asks hosts not to open one.
     */
    private function claimSurvivedCommit(ActionRun $actionRun): bool
    {
        $claimedAttempts = $actionRun->attempts;

        // Read from the writer: this read decides whether the step body runs, and a
        // lagging replica would answer with the very state the claim replaced.
        $stored = $actionRun->newQuery()
            ->useWritePdo()
            ->whereKey($actionRun->getKey())
            ->first(['status', 'attempts']);

        if ($stored?->status === ActionStatus::Running && $stored->attempts === $claimedAttempts) {
            return true;
        }

        // `attempts` is only ever incremented, and by the database, so a stored count
        // below the one this claim produced can only be that increment undone. Anything
        // else is the ordinary race the row lost after it was won.
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

    /**
     * Record the step's result, but only if this executor still owns the row — see
     * writeOutcome(). Returns whether the outcome was recorded.
     */
    public function completeAction(ActionRun $actionRun, mixed $result): bool
    {
        $claimedAttempts = $actionRun->attempts;

        $actionRun->status = ActionStatus::Completed;
        $actionRun->result = $this->serializer->serialize($result);
        $actionRun->finished_at = Carbon::now();

        if (! $this->writeOutcome($actionRun, $claimedAttempts, FlowEventType::ActionCompleted)) {
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

    /**
     * Record the step's failure under the same ownership check as completeAction().
     */
    public function failAction(ActionRun $actionRun, Throwable $exception): bool
    {
        $claimedAttempts = $actionRun->attempts;
        $exceptionArray = $this->exceptionToArray($exception);

        $actionRun->status = ActionStatus::Failed;
        $actionRun->exception = $exceptionArray;
        $actionRun->finished_at = Carbon::now();

        if (! $this->writeOutcome($actionRun, $claimedAttempts, FlowEventType::ActionFailed)) {
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
     * Persist the pending attribute changes as a fenced conditional UPDATE. The values
     * come from getDirty(), so the model's own casts encode them exactly as save()
     * would; only the WHERE differs.
     *
     * Its two conditions guard two different rivals. `attempts` fences against another
     * executor: reclaim can hand the row to a second worker while the first is merely
     * slow, and the superseded one must not overwrite the live one's outcome. The
     * status check fences against a transition that never claimed the row — the
     * monitor expires an overdue step without touching `attempts`, so without it a
     * late worker would overwrite that Expired and leave the run carrying both
     * action.expired and action.completed.
     */
    private function writeOutcome(ActionRun $actionRun, int $claimedAttempts, FlowEventType $outcome): bool
    {
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

            return false;
        }

        $actionRun->syncOriginal();

        return true;
    }

    /**
     * Mark a failed optional (continueOnFailure) step as OptionalFailed once its
     * retries are exhausted. The flow is not failed; the recorded exception is
     * preserved and an optional_failed event/Laravel event is appended so the
     * give-up is visible in history.
     */
    public function optionalFail(ActionRun $actionRun): void
    {
        $actionRun->status = ActionStatus::OptionalFailed;
        $actionRun->finished_at = Carbon::now();
        $actionRun->save();

        $this->events->record(
            $actionRun->flowRun,
            FlowEventType::ActionOptionalFailed,
            $actionRun->sequence,
            $actionRun,
            $actionRun->exception !== null ? ['exception' => $actionRun->exception] : []
        );

        event(new OptionalActionFailed($actionRun));
    }

    /**
     * Record that the queue has finished retrying this step's current job, from the
     * one place that knows: Laravel's own failure hook. The retry seam reads this
     * instead of comparing the attempts counter with the action's $tries, which lives
     * in code and can change under a job already in flight. No event is appended —
     * this is queue bookkeeping, not a step in the flow's history.
     */
    public function markQueueAttemptsExhausted(ActionRun $actionRun): void
    {
        if ($actionRun->queue_attempts_exhausted) {
            return;
        }

        $actionRun->queue_attempts_exhausted = true;
        $actionRun->save();
    }

    /**
     * Close a parked step that has given up: put it back into the Failed state the
     * retry policy deferred, keeping the exception and finished_at of the attempt that
     * failed. No event is appended — action.failed was recorded back then, and the
     * give-up is visible from the flow's own failure and the timed-out wait-signal.
     */
    public function settleAwaitingRetry(ActionRun $actionRun): void
    {
        if ($actionRun->status !== ActionStatus::AwaitingRetry) {
            return;
        }

        $actionRun->status = ActionStatus::Failed;
        $actionRun->save();
    }

    /**
     * Close every step of a run that reached a terminal state without an outcome of its
     * own. Cancelled says exactly that: the step stopped because the run under it ended,
     * not because of anything the step itself did. Steps that already settled keep their
     * status, so a finished run still shows which of its steps actually ran.
     *
     * Only `status` is written: an AwaitingRetry row carries the finished_at of the
     * attempt that failed, and the moment of the closure is flow_runs.finished_at. No
     * event is appended, for the same reason settleAwaitingRetry() appends none — the
     * run's own terminal event records both the moment and the cause.
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
     * Park a failed step on its retry signal: flip it to AwaitingRetry and append an
     * action.awaiting_retry event. The step is NOT terminal — the recorded exception
     * and finished_at of the last attempt are kept so the seam can decide again once
     * the signal arrives (or the wait-signal times out).
     */
    public function awaitRetry(ActionRun $actionRun, string $signal, ?int $maxAttempts = null): void
    {
        $actionRun->status = ActionStatus::AwaitingRetry;
        $actionRun->retry_signal = $signal;

        // A row scheduled without a policy adopts one here, and the cap has to be
        // written with the signal: from this parking on the budget is read off the
        // row, so an empty column would silently mean unbounded.
        $actionRun->retry_signal_max_attempts ??= $maxAttempts;

        $actionRun->save();

        $this->events->record(
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

        event(new ActionAwaitingRetry($actionRun, $signal));
    }

    /**
     * Start another signal-gated retry cycle: spend one unit of the budget and rewind
     * the row to Pending so the very same (flow_run_id, sequence) ordinal runs again.
     * `attempts` is deliberately untouched — it counts queue attempts within one
     * execution — and the previous exception stands until the new attempt overwrites it.
     */
    public function retryAction(ActionRun $actionRun, ?DateTimeInterface $expiresAt = null): void
    {
        $actionRun->retry_signal_attempts = $actionRun->retry_signal_attempts + 1;
        $actionRun->status = ActionStatus::Pending;
        $actionRun->started_at = null;
        $actionRun->finished_at = null;

        // The deadline is derived from a start that no longer happened; the next
        // claim recomputes it from its own started_at.
        $actionRun->reclaim_stale_at = null;

        // A fresh job means a fresh native-attempt allowance: the queue has not given
        // up on this cycle yet, whatever it did to the previous one.
        $actionRun->queue_attempts_exhausted = false;

        // The doctor holds a fresh Pending row off for grace_seconds by comparing
        // created_at, which a reused row passed long ago. Without an explicit hold it
        // would treat this cycle's job as lost the instant it is dispatched and send a
        // second one, and the generation token cannot tell two jobs of the same cycle
        // apart. The attempt counter restarts with the cycle for the same reason: a
        // budget spent on earlier cycles must not deny this one its recovery.
        $actionRun->repair_attempts = 0;
        $actionRun->repair_available_at = Carbon::now()->addSeconds($this->repairGrace());

        $deadline = $expiresAt ?? $this->defaultExpiry();

        $actionRun->expires_at = $deadline === null ? null : Carbon::instance($deadline);
        $actionRun->save();

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
     * Mark a still-pending/running step Expired once its expires_at deadline passes
     * (monitor): record the expiry cause and append an action.expired event. On
     * replay the seam treats Expired as a failure (or, for an optional step, as a
     * give-up returning its fallback).
     *
     * Conditional, from the other side of the race writeOutcome() guards: the sweep
     * selects a row and writes to it a moment later, and an executing worker can settle
     * it in between. An unconditional save() would demote that Completed step to
     * Expired, failing a run over work that succeeded. Returns whether this call is the
     * one that expired the row, so the monitor counts and wakes only for a transition
     * it won.
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
