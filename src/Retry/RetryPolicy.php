<?php

namespace DiscoveryUkraine\SagaLaraFlow\Retry;

use Throwable;

/**
 * A named, reusable retryOnSignal() policy: one object owning which signal a failed
 * step parks on, how long it may keep parking, and whether THIS failure is worth
 * parking at all. Pass an instance to ActionBuilder::retryOnSignal().
 *
 * Nothing about it is persisted — handle() rebuilds it on every replay — so all five
 * members must be deterministic, and only maxRetries() has its value frozen, onto the
 * row at scheduling. shouldRetry() may not write to the run it is deciding for, nor
 * drive any run; the engine refuses that with RetryPolicyReentryException.
 */
abstract class RetryPolicy
{
    /**
     * The signal name a failed step parks on. Takes effect on every replay: the
     * column records it but is never read back.
     */
    abstract public function signal(): string;

    /**
     * Cap on signal-gated cycles; null falls back to
     * actions.retry_on_signal.max_retries, and null there means unbounded. Called on
     * every replay, but only the value read at scheduling is kept, on the row.
     */
    public function maxRetries(): ?int
    {
        return null;
    }

    /**
     * How long ONE wait may last before the monitor gives up on it; null falls back
     * to the configured default signal timeout.
     */
    public function waitSeconds(): ?int
    {
        return null;
    }

    /**
     * Exception classes this policy reacts to, subclasses included; null reacts to
     * every failure. Checked before shouldRetry().
     *
     * @return list<class-string<Throwable>>|null
     */
    public function only(): ?array
    {
        return null;
    }

    /**
     * The final say. Returning false ends the policy for this failure and the step
     * fails exactly as it would have without a retry policy at all.
     *
     * It decides whether to PARK, not whether to wake: once a step is parked, the
     * signal that arrives spends a cycle and re-runs it without asking again.
     *
     * A throw is absorbed and read as false — the step fails rather than the run —
     * and recorded in the engine's anomaly log. Reaching back into the workflow is
     * the exception: that is refused outright, because no answer makes it safe.
     */
    public function shouldRetry(RetryContext $context): bool
    {
        return true;
    }
}
