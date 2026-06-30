<?php

namespace DiscoveryUkraine\SagaLaraFlow\Models;

use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Models\Concerns\UsesSagaFlowConnection;
use Illuminate\Database\Eloquent\Concerns\HasUlids;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Support\Carbon;

/**
 * @property string $id
 * @property string $flow_run_id
 * @property int $sequence
 * @property string $action_class
 * @property ?string $action_name
 * @property ActionStatus $status
 * @property bool $continue_on_failure
 * @property bool $has_compensation
 * @property ?int $parallel_group
 * @property ?array<int|string, mixed> $arguments
 * @property ?array<int|string, mixed> $result
 * @property ?array<int|string, mixed> $exception
 * @property int $attempts
 * @property ?Carbon $started_at
 * @property ?Carbon $finished_at
 * @property ?Carbon $expires_at
 * @property int $repair_attempts
 * @property ?Carbon $repair_available_at
 * @property-read FlowRun $flowRun
 */
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
            'repair_attempts' => 'integer',
            'repair_available_at' => 'datetime',
        ];
    }

    public function flowRun(): BelongsTo
    {
        return $this->belongsTo(config('saga-lara-flow.models.flow_run'), 'flow_run_id');
    }
}
