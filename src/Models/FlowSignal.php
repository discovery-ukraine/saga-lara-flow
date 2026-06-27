<?php

namespace DiscoveryUkraine\SagaLaraFlow\Models;

use DiscoveryUkraine\SagaLaraFlow\Enums\SignalStatus;
use DiscoveryUkraine\SagaLaraFlow\Models\Concerns\UsesSagaFlowConnection;
use Illuminate\Database\Eloquent\Concerns\HasUlids;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;

class FlowSignal extends Model
{
    use HasUlids;
    use UsesSagaFlowConnection;

    protected string $baseTable = 'flow_signals';

    protected $guarded = [];

    protected function casts(): array
    {
        return [
            'payload' => 'array',
            'status' => SignalStatus::class,
            'wait_sequence' => 'integer',
            'timeout_at' => 'datetime',
            'received_at' => 'datetime',
            'consumed_at' => 'datetime',
        ];
    }

    public function flowRun(): BelongsTo
    {
        return $this->belongsTo(config('saga-lara-flow.models.flow_run'), 'flow_run_id');
    }
}
