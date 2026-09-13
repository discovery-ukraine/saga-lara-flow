<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions;

use Throwable;

/**
 * A tenancy hook is configured but names nothing that can be called. It is refused
 * rather than skipped: a skipped restore would run the step in whatever tenant the
 * worker happens to be in, which looks exactly like a hook that was turned off.
 */
class InvalidTenancyHookException extends FlowException
{
    public static function for(string $name, mixed $hook, ?Throwable $previous = null): self
    {
        $given = match (true) {
            is_string($hook) => "[{$hook}]",
            is_array($hook) => '['.implode(', ', array_map(
                fn (mixed $part): string => is_string($part) ? $part : get_debug_type($part),
                $hook,
            )).']',
            default => get_debug_type($hook),
        };

        return new self(
            "The tenancy.{$name} hook {$given} cannot be called. Name an invokable class or a "
            ."[Class::class, 'method'] pair with its full namespace, or set the hook to null to turn it off.",
            previous: $previous,
        );
    }
}
