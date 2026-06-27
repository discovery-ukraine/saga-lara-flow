<?php

namespace DiscoveryUkraine\SagaLaraFlow\Enums;

enum ParallelFailurePolicy: string
{
    case FailFast = 'fail_fast';
    case WaitAllThenFail = 'wait_all_then_fail';
}
