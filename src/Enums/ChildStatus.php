<?php

namespace DiscoveryUkraine\SagaLaraFlow\Enums;

enum ChildStatus: string
{
    case Pending = 'pending';
    case Running = 'running';
    case Completed = 'completed';
    case Failed = 'failed';
    case Cancelled = 'cancelled';
}
