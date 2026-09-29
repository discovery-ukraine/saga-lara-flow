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

        // A same-state transition is legal by definition, but must not return early:
        // an instance whose stale status happens to equal the target is exactly the one
        // that would slip past the write's guard unchecked.
        if ($from !== $to && ! $this->canTransition($from, $to)) {
            throw InvalidTransitionException::between($from, $to);
        }

        $restore = $this->snapshot($run);

        $now = now();

        $run->status = $to;

        if ($to === FlowStatus::Running && $run->started_at === null) {
            $run->started_at = $now;
        }

        // Both are written once and never again. A same-state terminal transition is a
        // legal no-op that at-least-once delivery does reach — a duplicated batch
        // callback lands a run that is already Cancelled — and rewriting the marks there
        // would move the moment the run actually ended to whenever the duplicate arrived.
        if ($to === FlowStatus::Cancelled && $run->cancelled_at === null) {
            $run->cancelled_at = $now;
        }

        if ($to->isTerminal() && $run->finished_at === null) {
            $run->finished_at = $now;
        }

        // Set here rather than left to the builder, which fills the column in the SQL
        // without telling the model. Otherwise the instance — and every lifecycle event
        // raised off it a moment later — keeps the timestamp of the previous write.
        $run->updated_at = $now;

        try {
            $to->isTerminal()
                ? $this->finish($run, $from, $to)
                : $this->write($run, $from, $to);
        } catch (Throwable $failure) {
            $restore();

            throw $failure;
        }

        $run->syncOriginal();

        return $run;
    }

    /**
     * Capture the instance as the caller left it — attributes and the original snapshot
     * they are measured against — and return the closure that puts it back.
     *
     * The instance is mutated before the row confirms anything, so an exit that leaves
     * the row untouched has to leave the instance untouched too: a caller reading a
     * terminal status the database rejected would find cancel() refusing their retry.
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
     * Persist the transition, but only if the row still holds the status this instance
     * read before deciding on it. Nothing serialises an operator's cancel against a
     * worker driving the run: each holds its own FlowRun instance, and the queue's
     * per-run lock covers jobs, not the CLI or the monitor's inline sweep. The
     * conditional UPDATE is the same shape the action writes use, and for the same
     * reason — it is the only form every supported driver enforces atomically.
     *
     * `status` is always part of the SET, even when unchanged: getDirty() can come back
     * empty, and update([]) writes nothing and reports zero rows.
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

        // Read from the writer. This read decides whether the write counts and names the
        // winner in the log and the exception; a lagging replica would answer with the
        // very status the guard was fencing against.
        /** @var ?FlowStatus $actual */
        $actual = $run->newQuery()
            ->useWritePdo()
            ->whereKey($run->getKey())
            ->value('status');

        // Zero rows is not proof the guard failed: MySQL counts rows it changed, not
        // rows it matched, so an update whose every value already equals what is stored
        // reports zero there and one on SQLite and PostgreSQL. Only a same-state
        // transition with nothing of its own to write can land in that shape — status is
        // what it already is, and updated_at, stored to the second, is rewritten inside
        // that same second.
        $ambiguous = $from === $to && Arr::except($dirty, $run->getUpdatedAtColumn()) === [];

        if ($ambiguous && $actual === $from) {
            return;
        }

        $this->refuse($run, $from, $to, $actual);
    }

    /**
     * Report a transition the row refused. The status actually there goes into both the
     * log line and the exception: without it an operator knows only that they lost.
     */
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
     * Land a run in a terminal state and close the work it leaves behind in one
     * transaction: steps that never reached an outcome of their own, and wait-signals
     * nothing will resolve. Every terminal transition passes through here, so no
     * cancellation, expiry or child-close can mark a run finished while leaving its
     * steps looking live.
     *
     * The run row is written inside the transaction rather than before it, so a failed
     * settle rolls the run back for the caller to try again instead of leaving it
     * finished but unsettled, with nothing left to notice. The transaction is opened on
     * the run's own connection, so it still covers all three writes when the package
     * lives on a dedicated one.
     */
    private function finish(FlowRun $run, FlowStatus $from, FlowStatus $to): void
    {
        $run->getConnection()->transaction(function () use ($run, $from, $to): void {
            $this->write($run, $from, $to);

            app(ActionRecorder::class)->settleOpenSteps($run);
            app(SignalRecorder::class)->settleOpenWaits($run);
            app(ChildRecorder::class)->settleParked($run);
        });
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
