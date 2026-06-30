<?php

namespace DiscoveryUkraine\SagaLaraFlow\Enums;

/**
 * How a parallel() block reacts when one of its steps fails.
 *
 * FailFast cancels the block on the first hard failure: pending siblings never
 * start, in-flight ones are awaited (they cannot be force-killed), and the flow
 * fails once the block settles. WaitAllThenFail lets every step run to a terminal
 * state first, then fails if any step failed. Either way, completed steps are
 * compensated together as one rollback level.
 */
enum ParallelFailurePolicy: string
{
    case FailFast = 'fail_fast';
    case WaitAllThenFail = 'wait_all_then_fail';
}
