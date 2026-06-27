<?php

namespace DiscoveryUkraine\SagaLaraFlow\Models;

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Models\Concerns\UsesSagaFlowConnection;
use Illuminate\Database\Eloquent\Concerns\HasUlids;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;

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
