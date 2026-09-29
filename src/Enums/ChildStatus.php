<?php

namespace DiscoveryUkraine\SagaLaraFlow\Enums;

enum ChildStatus: string
{
    case Pending = 'pending';
    case Running = 'running';
    case Completed = 'completed';
    case Failed = 'failed';
    case Expired = 'expired';
    case Cancelled = 'cancelled';

    public function isTerminal(): bool
    {
        return in_array($this, [
            self::Completed,
            self::Failed,
            self::Expired,
            self::Cancelled,
        ], true);
    }
}
