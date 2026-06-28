<?php

namespace DiscoveryUkraine\SagaLaraFlow\Models;

use DiscoveryUkraine\SagaLaraFlow\Enums\SignalStatus;
use DiscoveryUkraine\SagaLaraFlow\Models\Concerns\UsesSagaFlowConnection;
use Illuminate\Database\Eloquent\Concerns\HasUlids;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Support\Carbon;

/**
 * @property string $id
 * @property string $flow_run_id
 * @property string $name
 * @property ?array<int|string, mixed> $payload
 * @property SignalStatus $status
 * @property ?int $wait_sequence
 * @property ?Carbon $timeout_at
 * @property ?Carbon $received_at
 * @property ?Carbon $consumed_at
 * @property-read FlowRun $flowRun
 */
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
