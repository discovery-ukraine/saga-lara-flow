<?php

namespace DiscoveryUkraine\SagaLaraFlow\Models;

use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationStatus;
use DiscoveryUkraine\SagaLaraFlow\Models\Concerns\UsesSagaFlowConnection;
use Illuminate\Database\Eloquent\Concerns\HasUlids;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;

class CompensationRun extends Model
{
    use HasUlids;
    use UsesSagaFlowConnection;

    protected string $baseTable = 'compensation_runs';

    protected $guarded = [];

    protected function casts(): array
    {
        return [
            'sequence' => 'integer',
            'status' => CompensationStatus::class,
            'continue_on_failure' => 'boolean',
            'arguments' => 'array',
            'result' => 'array',
            'exception' => 'array',
            'started_at' => 'datetime',
            'finished_at' => 'datetime',
        ];
    }

    public function flowRun(): BelongsTo
    {
        return $this->belongsTo(config('saga-lara-flow.models.flow_run'), 'flow_run_id');
    }

    public function actionRun(): BelongsTo
    {
        return $this->belongsTo(config('saga-lara-flow.models.action_run'), 'action_run_id');
    }
}
