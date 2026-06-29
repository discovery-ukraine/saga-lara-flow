<?php

namespace DiscoveryUkraine\SagaLaraFlow\Data;

use Closure;
use Laravel\SerializableClosure\SerializableClosure;

/**
 * A durable description of how to compensate a completed step: either a class
 * (recommended) or a serialized closure. It is reconstructed deterministically
 * on every replay from the compensateWith() call, and is carried
 * verbatim inside RunCompensationJob so a rollback can run in a separate process.
 */
final readonly class CompensationDefinition
{
    /**
     * @param  array<int, mixed>  $arguments
     */
    private function __construct(
        public string $type,
        public ?string $class,
        public array $arguments,
        public ?SerializableClosure $closure,
    ) {}

    /**
     * @param  array<int, mixed>  $arguments
     */
    public static function forClass(string $class, array $arguments = []): self
    {
        return new self('class', $class, array_values($arguments), null);
    }

    public static function forClosure(Closure $closure): self
    {
        return new self('closure', null, [], new SerializableClosure($closure));
    }

    public function isClosure(): bool
    {
        return $this->type === 'closure';
    }
}
