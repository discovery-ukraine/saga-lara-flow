<?php

namespace DiscoveryUkraine\SagaLaraFlow\Enums;

enum FlowStatus: string
{
    case Pending = 'pending';
    case Running = 'running';
    case Waiting = 'waiting';
    case Completed = 'completed';
    case Failed = 'failed';
    case Cancelling = 'cancelling';
    case Cancelled = 'cancelled';
    case Expired = 'expired';

    public function isTerminal(): bool
    {
        return in_array($this, self::terminal(), true);
    }

    public function canStartWork(): bool
    {
        return in_array($this, self::mayStartWork(), true);
    }

    /**
     * The statuses a run never leaves. Single source of truth for isTerminal() and
     * for the sweeps that must not pick up work belonging to a finished run.
     *
     * @return array<int, self>
     */
    public static function terminal(): array
    {
        return [self::Completed, self::Failed, self::Cancelled, self::Expired];
    }

    /**
     * The statuses a run may start new work in: everything it has not finished in,
     * minus the one it rolls back in. A rollback plans the stack it will undo once,
     * so a step that starts under Cancelling finishes outside every plan there is —
     * its compensation is in none of them and never runs, over a run reporting a
     * complete unwind.
     *
     * The same three statuses signalable() names, and still a separate set: that one
     * answers who may be handed a signal, this one whether the engine may start work of
     * its own. Single source of truth for canStartWork().
     *
     * @return array<int, self>
     */
    public static function mayStartWork(): array
    {
        return [self::Pending, self::Running, self::Waiting];
    }

    /**
     * Statuses in which a run can still be handed a signal: non-terminal and not
     * mid-rollback. This is the delivery boundary SignalDispatcher accepts, minus
     * Cancelling — a run rolling back would only store a floating signal nobody
     * consumes. Single source of truth for FlowQuery::active()/signalable().
     *
     * @return array<int, self>
     */
    public static function signalable(): array
    {
        return [self::Pending, self::Running, self::Waiting];
    }
}
