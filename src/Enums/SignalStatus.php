<?php

namespace DiscoveryUkraine\SagaLaraFlow\Enums;

enum SignalStatus: string
{
    case Waiting = 'waiting';
    case Received = 'received';
    case Consumed = 'consumed';
    case TimedOut = 'timed_out';
}
