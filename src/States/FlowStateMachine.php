<?php

namespace DiscoveryUkraine\SagaLaraFlow\States;

use Closure;
use DiscoveryUkraine\SagaLaraFlow\Contracts\StateMachine;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ConcurrentFlowTransitionException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\InvalidTransitionException;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ActionRecorder;
use DiscoveryUkraine\SagaLaraFlow\Runtime\AnomalyLog;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ChildRecorder;
use DiscoveryUkraine\SagaLaraFlow\Runtime\SignalRecorder;
use Illuminate\Support\Arr;
use Throwable;

class FlowStateMachine implements StateMachine
{
    public function transition(FlowRun $run, FlowStatus $to): FlowRun
    {
        $from = $run->status;

        // No early return on a same-state move: a stale instance would slip past the write's guard.
        if ($from !== $to && ! $this->canTransition($from, $to)) {
            throw InvalidTransitionException::between($from, $to);
        }

        $restore = $this->snapshot($run);

        $now = now();

        $run->status = $to;

        if ($to === FlowStatus::Running && $run->started_at === null) {
            $run->started_at = $now;
        }

        // Written once: a duplicated landing must not move the moment the run ended.
        if ($to === FlowStatus::Cancelled && $run->cancelled_at === null) {
            $run->cancelled_at = $now;
        }

        if ($to->isTerminal() && $run->finished_at === null) {
            $run->finished_at = $now;
        }

        // On the model too: the builder sets it in the SQL only, and lifecycle events read the
        // model.
        $run->updated_at = $now;

        try {
            match (true) {
                $to->isTerminal() => $this->finish($run, $from, $to),
                $to === FlowStatus::Cancelling => $this->rollBack($run, $from, $to),
                default => $this->write($run, $from, $to),
            };
        } catch (Throwable $failure) {
            $restore();

            throw $failure;
        }

        $run->syncOriginal();

        return $run;
    }

    /**
     * An exit that leaves the row untouched leaves the instance untouched too.
     *
     * @return Closure(): void
     */
    private function snapshot(FlowRun $run): Closure
    {
        $attributes = $run->getAttributes();
        $original = $run->getRawOriginal();

        return static function () use ($run, $attributes, $original): void {
            $run->setRawAttributes($original, sync: true);
            $run->setRawAttributes($attributes, sync: false);
        };
    }

    /**
     * `status` is always in the SET: getDirty() can be empty, and update([]) reports zero rows.
     */
    private function write(FlowRun $run, FlowStatus $from, FlowStatus $to): void
    {
        $dirty = $run->getDirty();

        $affected = $run->newQuery()
            ->whereKey($run->getKey())
            ->where('status', $from)
            ->update($dirty + ['status' => $run->getAttributes()['status']]);

        if ($affected === 1) {
            return;
        }

        /** @var ?FlowStatus $actual */
        $actual = $run->newQuery()
            ->useWritePdo()
            ->whereKey($run->getKey())
            ->value('status');

        // MySQL reports zero for a same-state write that changes nothing (updated_at in the same
        // second).
        $ambiguous = $from === $to && Arr::except($dirty, $run->getUpdatedAtColumn()) === [];

        if ($ambiguous && $actual === $from) {
            return;
        }

        $this->refuse($run, $from, $to, $actual);
    }

    private function refuse(FlowRun $run, FlowStatus $from, FlowStatus $to, ?FlowStatus $actual): never
    {
        app(AnomalyLog::class)->log(AnomalyLog::REASON_TRANSITION_LOST, [
            'entity' => 'flow',
            'flow_run_id' => $run->id,
            'observed' => $from->value,
            'intended' => $to->value,
            'actual' => $actual?->value,
        ]);

        throw ConcurrentFlowTransitionException::for($run, $from, $to, $actual);
    }

    /**
     * The run is written inside the transaction, so a failed settle rolls the landing back for the
     * caller to try again; it is the run's own connection, which the package may keep apart.
     */
    private function finish(FlowRun $run, FlowStatus $from, FlowStatus $to): void
    {
        $run->getConnection()->transaction(function () use ($run, $from, $to): void {
            $this->write($run, $from, $to);

            if ($to !== FlowStatus::Completed) {
                app(ChildRecorder::class)->fenceLive($run, $this->closesWithCompensation($run, $from, $to));
            }

            app(ActionRecorder::class)->settleOpenSteps($run);
            app(SignalRecorder::class)->settleOpenWaits($run);
            app(ChildRecorder::class)->settleParked($run);
        });
    }

    /**
     * A run landing from a rollback closes its children as that rollback does; one landing
     * straight from a live state rolls them back unless it is being cancelled.
     */
    private function closesWithCompensation(FlowRun $run, FlowStatus $from, FlowStatus $to): bool
    {
        return $from === FlowStatus::Cancelling ? $run->rollback_compensates !== false : $to !== FlowStatus::Cancelled;
    }

    /**
     * One transaction, so no child can finish between the plan of this rollback and its close.
     */
    private function rollBack(FlowRun $run, FlowStatus $from, FlowStatus $to): void
    {
        $run->getConnection()->transaction(function () use ($run, $from, $to): void {
            $this->write($run, $from, $to);

            app(ChildRecorder::class)->fenceLive($run, $run->rollback_compensates !== false);
        });

        $this->requireSurvivedCommit($run, $from, $to);
    }

    /**
     * Read the move back once its transaction has closed: on PostgreSQL a failure swallowed
     * inside it turns the COMMIT that reports success into a rollback.
     */
    private function requireSurvivedCommit(FlowRun $run, FlowStatus $from, FlowStatus $to): void
    {
        $held = $run->newQuery()->useWritePdo()->whereKey($run->getKey())->first();

        if ($held?->status === $to && $held->rollback_token === $run->rollback_token) {
            return;
        }

        app(AnomalyLog::class)->log(AnomalyLog::REASON_CLAIM_NOT_COMMITTED, [
            'entity' => 'flow',
            'site' => 'rollback',
            'flow_run_id' => $run->id,
            'workflow_class' => $run->workflow_class,
            'actual' => $held?->status->value,
        ]);

        throw ConcurrentFlowTransitionException::for($run, $from, $to, $held?->status);
    }

    public function canTransition(FlowStatus $from, FlowStatus $to): bool
    {
        return in_array($to, $this->allowedFrom($from), true);
    }

    /**
     * @return list<FlowStatus>
     */
    private function allowedFrom(FlowStatus $from): array
    {
        return match ($from) {
            FlowStatus::Pending => [
                FlowStatus::Running,
                FlowStatus::Cancelling,
                FlowStatus::Cancelled,
                FlowStatus::Expired,
            ],
            FlowStatus::Running => [
                FlowStatus::Waiting,
                FlowStatus::Completed,
                FlowStatus::Failed,
                FlowStatus::Cancelling,
                FlowStatus::Cancelled,
                FlowStatus::Expired,
            ],
            FlowStatus::Waiting => [
                FlowStatus::Running,
                FlowStatus::Completed,
                FlowStatus::Failed,
                FlowStatus::Cancelling,
                FlowStatus::Cancelled,
                FlowStatus::Expired,
            ],
            FlowStatus::Cancelling => [
                FlowStatus::Cancelled,
                FlowStatus::Failed,
                FlowStatus::Expired,
            ],
            FlowStatus::Completed,
            FlowStatus::Failed,
            FlowStatus::Cancelled,
            FlowStatus::Expired => [],
        };
    }
}
