<?php

namespace DiscoveryUkraine\SagaLaraFlow\Enums;

enum DispatchMode: string
{
    case Queue = 'queue';
    case Sync = 'sync';
}
