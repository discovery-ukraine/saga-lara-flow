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
        return in_array($this, [
            self::Completed,
            self::Failed,
            self::Cancelled,
            self::Expired,
        ], true);
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
