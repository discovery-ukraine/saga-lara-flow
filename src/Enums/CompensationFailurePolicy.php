<?php

namespace DiscoveryUkraine\SagaLaraFlow\Enums;

enum CompensationFailurePolicy: string
{
    case Stop = 'stop';
    case Continue = 'continue';
}
