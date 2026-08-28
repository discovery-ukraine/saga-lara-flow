<?php

namespace DiscoveryUkraine\SagaLaraFlow\Retry;

use Throwable;

/**
 * A named, reusable retryOnSignal() policy: one object that owns which signal a
 * failed step parks on, how long it may keep parking, and — the part four scalar
 * arguments could never express — whether THIS failure is worth parking at all.
 *
 * Pass an instance to ActionBuilder::retryOnSignal(); the builder reads it and the
 * step behaves exactly as it would have with the equivalent arguments.
 *
 * All five run on every replay — the builder rebuilds the policy each pass, so even a
 * step already scheduled or completed calls them again. What is frozen is one value,
 * not one method:
 *
 * - maxRetries() is read when the step is SCHEDULED into
 *   action_runs.retry_signal_max_attempts, and every later replay enforces the row.
 *   Raising it in a deploy does not lift the ceiling of a step already parked.
 * - signal(), waitSeconds(), only() and shouldRetry() take effect immediately, so a
 *   deploy changes them for runs already in flight. action_runs.retry_signal records
 *   the name a step last parked on but is rewritten at every parking and never read
 *   back: renaming a signal moves what a parked step will next wait for, and abandons
 *   a delivery already made under the old name.
 *
 * Every method runs during replay, so all five must be deterministic — the same
 * question must get the same answer on every pass. Two further obligations:
 *
 * - The first four are called while the workflow is still building the step, before
 *   the seam has resolved anything, so one that throws takes the whole run down with
 *   it and the step it was attached to never registers its compensation — exactly as
 *   an argument expression that throws would. Only shouldRetry() is guarded.
 * - shouldRetry() is not called exactly once (a process that dies between the
 *   decision and the parking makes the next replay ask again), so it must be a pure
 *   predicate and leave side effects to a listener. It also may not write to the run
 *   it is deciding for — no seam, and no FlowHandle mutation on that run: it is not
 *   asked again once the step it guards succeeds, so an ordinal it consumed would be
 *   left for nobody to replay, and a run it cancelled would be handed a live wait the
 *   moment it returned. RetryPolicyReentryException refuses the attempt before
 *   anything is written. Other runs are fair game.
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
