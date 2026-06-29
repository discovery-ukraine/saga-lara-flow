<?php

namespace DiscoveryUkraine\SagaLaraFlow\Models;

use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationStatus;
use DiscoveryUkraine\SagaLaraFlow\Models\Concerns\UsesSagaFlowConnection;
use Illuminate\Database\Eloquent\Concerns\HasUlids;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Support\Carbon;

/**
 * @property string $id
 * @property string $flow_run_id
 * @property ?string $action_run_id
 * @property int $sequence
 * @property string $compensation_type
 * @property ?string $compensation_class
 * @property CompensationStatus $status
 * @property bool $continue_on_failure
 * @property ?array<int|string, mixed> $arguments
 * @property ?array<int|string, mixed> $result
 * @property ?array<int|string, mixed> $exception
 * @property ?Carbon $started_at
 * @property ?Carbon $finished_at
 * @property-read FlowRun $flowRun
 */
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
