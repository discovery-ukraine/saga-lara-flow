<?php

namespace DiscoveryUkraine\SagaLaraFlow\Repositories;

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowNotFoundException;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Models\SideEffect;
use Illuminate\Database\Eloquent\Model;

class EloquentFlowRepository implements FlowRepository
{
    public function create(array $attributes, array $tags = []): FlowRun
    {
        $model = $this->flowRunClass();

        $run = new $model;
        $run->fill($attributes);
        $run->save();

        foreach ($tags as $tag) {
            $run->tags()->create([
                'key' => $tag['key'],
                'value' => $tag['value'] ?? null,
            ]);
        }

        return $run;
    }

    public function find(string $id): ?FlowRun
    {
        return $this->flowRunClass()::query()->find($id);
    }

    public function findOrFail(string $id): FlowRun
    {
        return $this->find($id) ?? throw FlowNotFoundException::for($id);
    }

    public function findActionStep(string $flowRunId, int $sequence): ?ActionRun
    {
        /** @var ActionRun|null $step */
        $step = $this->stepBySequence($this->actionRunClass(), $flowRunId, $sequence);

        return $step;
    }

    public function findSideEffect(string $flowRunId, int $sequence): ?SideEffect
    {
        /** @var SideEffect|null $step */
        $step = $this->stepBySequence($this->sideEffectClass(), $flowRunId, $sequence);

        return $step;
    }

    /**
     * Resolve a row identified by its (flow_run_id, sequence) ordinal.
     *
     * @param  class-string<Model>  $model
     */
    private function stepBySequence(string $model, string $flowRunId, int $sequence): ?Model
    {
        return $model::query()
            ->where('flow_run_id', $flowRunId)
            ->where('sequence', $sequence)
            ->first();
    }

    /**
     * @return class-string<FlowRun>
     */
    private function flowRunClass(): string
    {
        return config('saga-lara-flow.models.flow_run');
    }

    /**
     * @return class-string<ActionRun>
     */
    private function actionRunClass(): string
    {
        return config('saga-lara-flow.models.action_run');
    }

    /**
     * @return class-string<SideEffect>
     */
    private function sideEffectClass(): string
    {
        return config('saga-lara-flow.models.side_effect');
    }
}
