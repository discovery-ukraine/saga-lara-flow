<?php

namespace DiscoveryUkraine\SagaLaraFlow\Enums;

enum CompensationStatus: string
{
    case Pending = 'pending';
    case Running = 'running';
    case Completed = 'completed';
    case Failed = 'failed';
}
