<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Events\SideEffectRecorded;
use DiscoveryUkraine\SagaLaraFlow\Events\SideEffectReused;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Models\SideEffect;

/**
 * Persists and replays side effects — nondeterministic values captured once and
 * reused verbatim on later replays.
 */
final readonly class SideEffectRecorder
{
    public function __construct(
        private EventLog $events,
        private Serializer $serializer,
    ) {}

    /**
     * Persist a side effect's computed value the first time it is encountered.
     * The value is serialized once here and replayed verbatim on every later
     * pass; the key is a human label only, identity is the sequence.
     */
    public function recordSideEffect(FlowRun $flowRun, int $sequence, string $key, mixed $value): SideEffect
    {
        /** @var class-string<SideEffect> $model */
        $model = config('saga-lara-flow.models.side_effect');

        $sideEffect = new $model;

        $sideEffect->fill([
            'flow_run_id' => $flowRun->id,
            'sequence' => $sequence,
            'key' => $key,
            'value' => $this->serializer->serialize($value),
        ]);

        $sideEffect->save();

        $this->events->record($flowRun, FlowEventType::SideEffectRecorded, $sequence, $sideEffect, [
            'key' => $key,
        ]);

        event(new SideEffectRecorded($sideEffect));

        return $sideEffect;
    }

    /**
     * Signal that a side effect resolved from stored history on replay. This is
     * a silent replay resolution (like a completed action): by default it only
     * dispatches the Laravel event for observability and writes NO flow_events
     * row, so the log stays bounded across the many replays a single run
     * performs. Set history.record_side_effect_reuse to also persist a
     * side_effect.reused flow_events row on every reuse for a full audit trail.
     */
    public function sideEffectReused(FlowRun $flowRun, SideEffect $sideEffect): void
    {
        if (config('saga-lara-flow.history.record_side_effect_reuse', false)) {
            $this->events->record($flowRun, FlowEventType::SideEffectReused, $sideEffect->sequence, $sideEffect, [
                'key' => $sideEffect->key,
            ]);
        }

        event(new SideEffectReused($sideEffect));
    }
}
