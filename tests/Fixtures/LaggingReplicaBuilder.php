<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures;

use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\CompensationRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowChild;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowEvent;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowSignal;
use Illuminate\Database\Eloquent\Builder;
use ReflectionProperty;

/**
 * A replica that has not caught up, for reads the connection does not route to the writer:
 * it shows the given runs, completed steps and compensations as still Running, holds none of the
 * missing runs, and holds no link, step, compensation, signal or event rows yet. The suite runs one PDO, so only a read's own routing can tell the
 * two apart.
 *
 * @template TModel of \Illuminate\Database\Eloquent\Model
 *
 * @extends Builder<TModel>
 */
final class LaggingReplicaBuilder extends Builder
{
    /** @var list<string> */
    public static array $runningRuns = [];

    /** @var list<string> */
    public static array $missingRuns = [];

    public static bool $noLinks = false;

    public static bool $noSteps = false;

    public static bool $unfinishedSteps = false;

    public static bool $noCompensations = false;

    public static bool $unfinishedCompensations = false;

    public static bool $noSignals = false;

    public static bool $noEvents = false;

    public static function reset(): void
    {
        self::$runningRuns = [];
        self::$missingRuns = [];
        self::$noLinks = false;
        self::$noSteps = false;
        self::$unfinishedSteps = false;
        self::$noCompensations = false;
        self::$unfinishedCompensations = false;
        self::$noSignals = false;
        self::$noEvents = false;
    }

    /**
     * The replica holds every compensation as still Running, so a filter that leaves out the
     * Completed ones leaves out none of them.
     */
    public function whereNot($column, $operator = null, $value = null, $boolean = 'and')
    {
        if (self::$unfinishedCompensations && $column === 'status' && $operator === CompensationStatus::Completed
            && $this->getModel() instanceof CompensationRun && ! $this->readsTheWriter()) {
            return $this;
        }

        return parent::whereNot($column, $operator, $value, $boolean);
    }

    public function exists(): bool
    {
        if (self::$noEvents && $this->getModel() instanceof FlowEvent && ! $this->readsTheWriter()) {
            return false;
        }

        return $this->toBase()->exists();
    }

    public function getModels($columns = ['*'])
    {
        $models = parent::getModels($columns);

        if ($this->readsTheWriter()) {
            return $models;
        }

        if (self::$noEvents) {
            $models = array_values(array_filter($models, fn ($model): bool => ! $model instanceof FlowEvent));
        }

        if (self::$noLinks) {
            $models = array_values(array_filter($models, fn ($model): bool => ! $model instanceof FlowChild));
        }

        if (self::$noSteps) {
            $models = array_values(array_filter($models, fn ($model): bool => ! $model instanceof ActionRun));
        }

        // Inside a transaction Laravel reads from the writer, which an engine's refresh() relies on.
        if (self::$missingRuns !== [] && $this->getQuery()->getConnection()->transactionLevel() === 0) {
            $models = array_values(array_filter(
                $models,
                fn ($model): bool => ! ($model instanceof FlowRun && in_array($model->getKey(), self::$missingRuns, true)),
            ));
        }

        if (self::$noCompensations) {
            $models = array_values(array_filter($models, fn ($model): bool => ! $model instanceof CompensationRun));
        }

        if (self::$noSignals) {
            $models = array_values(array_filter($models, fn ($model): bool => ! $model instanceof FlowSignal));
        }

        foreach ($models as $model) {
            if (self::$unfinishedSteps && $model instanceof ActionRun && $model->status === ActionStatus::Completed) {
                $model->setRawAttributes(
                    ['status' => ActionStatus::Running->value, 'result' => null, 'finished_at' => null] + $model->getAttributes(),
                    true,
                );
            }
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

    private function readsTheWriter(): bool
    {
        $connection = $this->getQuery()->getConnection();
        $pinned = (bool) new ReflectionProperty($connection, 'readOnWriteConnection')->getValue($connection);

        return $this->getQuery()->useWritePdo || $pinned;
    }
}
