<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

/**
 * A flow run whose save() reports success and writes nothing, for the interval a test
 * turns it on. Stands in for a transaction that a host listener aborted: the caller is
 * handed a model saying one thing while the row says another, which is the only way a
 * commit reporting success can still have kept nothing.
 */
class UnsavedFlowRun extends FlowRun
{
    public static bool $swallowSaves = false;

    public static function reset(): void
    {
        self::$swallowSaves = false;
    }

    /**
     * @param  array<string, mixed>  $options
     */
    public function save(array $options = []): bool
    {
        if (self::$swallowSaves) {
            return true;
        }

        return parent::save($options);
    }
}
