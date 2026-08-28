<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions;

/**
 * A retryOnSignal() predicate reached back into the workflow DSL. It cannot be
 * allowed to: the predicate is asked only while a failed step is deciding whether
 * to park, and it is not asked at all once that step succeeds — so any ordinal it
 * consumed would be left unclaimed on the next replay and the step after it would
 * land in the wrong slot.
 *
 * Unlike an ordinary throw from a predicate, this one is not absorbed as "do not
 * park". There is no answer that makes it safe, and the alternative is history the
 * engine would later blame on the workflow having changed.
 */
class RetryPolicyReentryException extends FlowException
{
    public static function for(string $operation): self
    {
        return new self(
            "A retryOnSignal() decision tried to run {$operation}. A retry predicate must be a pure "
            .'function of the RetryContext it is given: it cannot start actions, await signals, run '
            .'child workflows, or record side effects, because it is not replayed once the step it '
            .'guards succeeds. Move the work into the action, or into a listener.'
        );
    }
}
