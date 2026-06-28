<?php

namespace DiscoveryUkraine\SagaLaraFlow\Contracts;

/**
 * Converts workflow/action values to a JSON-safe representation for storage and
 * back. Implementations must round-trip scalars, arrays, Arrayable/
 * JsonSerializable objects and Eloquent models (stored as a reference and
 * rehydrated on the way out).
 */
interface Serializer
{
    /**
     * Convert a value into a JSON-safe representation (scalar, array or null).
     */
    public function serialize(mixed $value): mixed;

    /**
     * Rebuild a value previously produced by serialize().
     */
    public function deserialize(mixed $payload): mixed;
}
