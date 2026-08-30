<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use Closure;
use JsonSerializable;

/**
 * An action result the package's serializer accepts and PHP's `serialize()` refuses:
 * it presents clean data through JsonSerializable while holding a closure. Handing
 * one of these to a listener verbatim is what breaks a queued listener, so it stands
 * in for every result carrying a connection, a container binding, or a callback.
 */
final class ClosureBackedResult implements JsonSerializable
{
    private Closure $unserializable;

    public function __construct()
    {
        $this->unserializable = fn (): int => 1;
    }

    public function jsonSerialize(): mixed
    {
        return ['charge_id' => 'ch_9f3'];
    }
}
