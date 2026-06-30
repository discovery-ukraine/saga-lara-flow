<?php

namespace DiscoveryUkraine\SagaLaraFlow\Models;

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Models\Concerns\UsesSagaFlowConnection;
use Illuminate\Database\Eloquent\Concerns\HasUlids;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Support\Carbon;

/**
 * @property string $id
 * @property string $flow_run_id
 * @property ?int $sequence
 * @property FlowEventType $type
 * @property ?string $subject_type
 * @property ?string $subject_id
 * @property ?array<int|string, mixed> $payload
 * @property ?Carbon $recorded_at
 * @property ?Carbon $created_at
 * @property ?Carbon $updated_at
 * @property-read FlowRun $flowRun
 */
class FlowEvent extends Model
{
    use HasUlids;
    use UsesSagaFlowConnection;

    protected string $baseTable = 'flow_events';

    protected $guarded = [];

    protected function casts(): array
    {
        return [
            'sequence' => 'integer',
            'type' => FlowEventType::class,
            'payload' => 'array',
            'recorded_at' => 'datetime',
        ];
    }

    public function flowRun(): BelongsTo
    {
        return $this->belongsTo(config('saga-lara-flow.models.flow_run'), 'flow_run_id');
    }
}
