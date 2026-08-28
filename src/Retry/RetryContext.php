<?php

namespace DiscoveryUkraine\SagaLaraFlow\Retry;

/**
 * Everything a retry policy is told about the failure it is being asked to judge.
 * Built by the replay seam from the step's row; the policy never constructs one.
 *
 * The counters are three different things and none of them is a "try number":
 * $cyclesSpent counts the signal-gated cycles already spent on this step (0 the
 * first time the step parks), $cap is the ceiling the row was scheduled with, and
 * $executions counts every run of the step, the queue's own retries included.
 *
 * The row is deliberately absent. A policy is a predicate over this snapshot; the
 * step it is deciding about is mid-replay, and handing it out would invite a write.
 */
final readonly class RetryContext
{
    public function __construct(
        public string $runId,
        public string $workflowClass,
        public string $actionClass,
        public int $sequence,
        public string $signal,
        public int $cyclesSpent,
        public ?int $cap,
        public int $executions,
        public RecordedFailure $failure,
    ) {}
}
