<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Carbon;

/**
 * The single writer of the flow_events log. Every recorder appends history
 * through here, so the event-sourcing append path lives in exactly one place.
 */
final readonly class EventLog
{
    /**
     * @param  array<string, mixed>  $payload
     */
    public function record(
        FlowRun $flowRun,
        FlowEventType $eventType,
        ?int $sequence,
        ?Model $subject,
        array $payload = []
    ): void {
        /** @var class-string<Model> $model */
        $model = config('saga-lara-flow.models.flow_event');

        $event = new $model;

        $event->fill([
            'flow_run_id' => $flowRun->id,
            'sequence' => $sequence,
            'type' => $eventType,
            'subject_type' => $subject !== null ? $subject::class : null,
            'subject_id' => $subject?->getKey(),
            'payload' => $payload === [] ? null : $payload,
            'recorded_at' => Carbon::now(),
        ]);

        $event->save();
    }
}
