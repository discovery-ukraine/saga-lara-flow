<?php

namespace DiscoveryUkraine\SagaLaraFlow\Models;

use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Models\Concerns\UsesSagaFlowConnection;
use Illuminate\Database\Eloquent\Concerns\HasUlids;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;

class ActionRun extends Model
{
    use HasUlids;
    use UsesSagaFlowConnection;

    protected string $baseTable = 'action_runs';

    protected $guarded = [];

    protected function casts(): array
    {
        return [
            'sequence' => 'integer',
            'status' => ActionStatus::class,
            'continue_on_failure' => 'boolean',
            'has_compensation' => 'boolean',
            'parallel_group' => 'integer',
            'arguments' => 'array',
            'result' => 'array',
            'exception' => 'array',
            'attempts' => 'integer',
            'started_at' => 'datetime',
            'finished_at' => 'datetime',
            'expires_at' => 'datetime',
        ];
    }

    public function flowRun(): BelongsTo
    {
        return $this->belongsTo(config('saga-lara-flow.models.flow_run'), 'flow_run_id');
    }
}
