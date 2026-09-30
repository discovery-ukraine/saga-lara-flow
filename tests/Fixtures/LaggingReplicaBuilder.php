<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowChild;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Illuminate\Database\Eloquent\Builder;
use ReflectionProperty;

/**
 * A replica that has not caught up, for reads the connection does not route to the writer:
 * it shows the given runs as still Running and holds no link or step rows yet. The suite
 * runs one PDO, so only a read's own routing can tell the two apart.
 *
 * @template TModel of \Illuminate\Database\Eloquent\Model
 *
 * @extends Builder<TModel>
 */
final class LaggingReplicaBuilder extends Builder
{
    /** @var list<string> */
    public static array $runningRuns = [];

    public static bool $noLinks = false;

    public static bool $noSteps = false;

    public static function reset(): void
    {
        self::$runningRuns = [];
        self::$noLinks = false;
        self::$noSteps = false;
    }

    public function getModels($columns = ['*'])
    {
        $models = parent::getModels($columns);

        $connection = $this->getQuery()->getConnection();
        $pinned = (bool) new ReflectionProperty($connection, 'readOnWriteConnection')->getValue($connection);

        if ($this->getQuery()->useWritePdo || $pinned) {
            return $models;
        }

        if (self::$noLinks) {
            $models = array_values(array_filter($models, fn ($model): bool => ! $model instanceof FlowChild));
        }

        if (self::$noSteps) {
            $models = array_values(array_filter($models, fn ($model): bool => ! $model instanceof ActionRun));
        }

        // A read of one column carries no key, so the run it asked for is in the bindings.
        $asked = array_intersect(self::$runningRuns, $this->getQuery()->getBindings()) !== [];

        foreach ($models as $model) {
            $lagging = $model->getKey() === null ? $asked : in_array($model->getKey(), self::$runningRuns, true);

            if ($model instanceof FlowRun && $lagging) {
                $model->setRawAttributes(['status' => FlowStatus::Running->value] + $model->getAttributes(), true);
            }
        }

        return $models;
    }
}
