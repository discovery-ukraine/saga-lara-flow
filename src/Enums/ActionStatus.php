<?php

namespace DiscoveryUkraine\SagaLaraFlow\Enums;

enum ActionStatus: string
{
    case Pending = 'pending';
    case Running = 'running';
    case Completed = 'completed';
    case Failed = 'failed';
    case OptionalFailed = 'optional_failed';
    case Expired = 'expired';
    case Cancelled = 'cancelled';
}
