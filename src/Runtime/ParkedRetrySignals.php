<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\SignalStatus;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowChild;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowSignal;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Model;

final readonly class ParkedRetrySignals
{
    /**
     * Narrow $parked to steps or child links parked on a retry that a signal can still end:
     * the wait at their ordinal is open, or holds a delivery the next replay takes.
     *
     * @template TModel of Model
     *
     * @param  Builder<TModel>  $parked
     */
    public static function constrain(Builder $parked, ActionStatus|ChildStatus $status, string $runColumn): void
    {
        /** @var class-string<FlowSignal> $signals */
        $signals = config('saga-lara-flow.models.flow_signal');

        $model = $parked->getModel();
        $wait = $signals::query();
        $waits = $wait->getModel();

        $parked->where($model->qualifyColumn('status'), $status)
            ->whereExists($wait
                ->selectRaw('1')
                ->whereColumn($waits->qualifyColumn('flow_run_id'), $model->qualifyColumn($runColumn))
                ->whereColumn($waits->qualifyColumn('wait_sequence'), $model->qualifyColumn('sequence'))
                ->whereIn($waits->qualifyColumn('status'), [SignalStatus::Waiting, SignalStatus::Received]));
    }

    /**
     * @param  list<string>  $runIds
     * @return array<string, list<string>> keyed by run id
     */
    public function byRun(array $runIds): array
    {
        return $this->read($runIds, fromWriter: false);
    }

    /**
     * @return array<string, bool> keyed by signal name: whether its wait is still open
     */
    public function of(FlowRun $flowRun): array
    {
        $names = $this->read([$flowRun->id], fromWriter: true)[$flowRun->id] ?? [];

        /** @var class-string<FlowSignal> $signals */
        $signals = config('saga-lara-flow.models.flow_signal');

        $open = $names === [] ? [] : $signals::query()
            ->useWritePdo()
            ->where('flow_run_id', $flowRun->id)
            ->whereIn('name', $names)
            ->where('status', SignalStatus::Waiting)
            ->whereNotNull('wait_sequence')
            ->get(['name'])
            ->pluck('name')
            ->all();

        return array_combine($names, array_map(fn (string $name): bool => in_array($name, $open, true), $names));
    }

    /**
     * @param  list<string>  $runIds
     * @return array<string, list<string>>
     */
    private function read(array $runIds, bool $fromWriter): array
    {
        /** @var class-string<ActionRun> $steps */
        $steps = config('saga-lara-flow.models.action_run');

        /** @var class-string<FlowChild> $children */
        $children = config('saga-lara-flow.models.flow_child');

        $signals = [];

        $parkedSteps = $steps::query()
            ->when($fromWriter, fn (Builder $query) => $query->useWritePdo())
            ->whereIn('flow_run_id', $runIds)
            ->tap(fn (Builder $query) => self::constrain($query, ActionStatus::AwaitingRetry, 'flow_run_id'))
            ->whereNotNull('retry_signal')
            ->get(['flow_run_id', 'retry_signal']);

        foreach ($parkedSteps as $step) {
            $signals[$step->flow_run_id][] = (string) $step->retry_signal;
        }

        $parkedChildren = $children::query()
            ->when($fromWriter, fn (Builder $query) => $query->useWritePdo())
            ->whereIn('parent_flow_run_id', $runIds)
            ->tap(fn (Builder $query) => self::constrain($query, ChildStatus::AwaitingRetry, 'parent_flow_run_id'))
            ->get(['parent_flow_run_id', 'retry_signal']);

        foreach ($parkedChildren as $link) {
            $signals[$link->parent_flow_run_id][] = (string) $link->retry_signal;
        }

        return array_map(static fn (array $names): array => array_values(array_unique($names)), $signals);
    }
}
