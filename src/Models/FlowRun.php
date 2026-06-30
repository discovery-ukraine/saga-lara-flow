<?php

namespace DiscoveryUkraine\SagaLaraFlow\Models;

use DiscoveryUkraine\SagaLaraFlow\Contracts\StateMachine;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Models\Concerns\UsesSagaFlowConnection;
use Illuminate\Database\Eloquent\Concerns\HasUlids;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Database\Eloquent\Relations\HasMany;
use Illuminate\Support\Carbon;

/**
 * @property string $id
 * @property string $workflow_class
 * @property ?string $workflow_name
 * @property ?string $workflow_version
 * @property FlowStatus $status
 * @property ?array<int|string, mixed> $arguments
 * @property ?array<int|string, mixed> $result
 * @property ?array<int|string, mixed> $exception
 * @property ?array<int|string, mixed> $tenancy_context
 * @property ?string $connection
 * @property ?string $queue
 * @property ?string $parent_id
 * @property ?string $parent_close_policy
 * @property int $current_sequence
 * @property ?Carbon $started_at
 * @property ?Carbon $finished_at
 * @property ?Carbon $expires_at
 * @property ?Carbon $cancelled_at
 * @property int $repair_attempts
 * @property ?Carbon $repair_available_at
 */
class FlowRun extends Model
{
    use HasUlids;
    use UsesSagaFlowConnection;

    protected string $baseTable = 'flow_runs';

    protected $guarded = [];

    protected function casts(): array
    {
        return [
            'status' => FlowStatus::class,
            'arguments' => 'array',
            'result' => 'array',
            'exception' => 'array',
            'tenancy_context' => 'array',
            'current_sequence' => 'integer',
            'started_at' => 'datetime',
            'finished_at' => 'datetime',
            'expires_at' => 'datetime',
            'cancelled_at' => 'datetime',
            'repair_attempts' => 'integer',
            'repair_available_at' => 'datetime',
        ];
    }

    public function actions(): HasMany
    {
        return $this->hasMany(config('saga-lara-flow.models.action_run'), 'flow_run_id');
    }

    public function events(): HasMany
    {
        return $this->hasMany(config('saga-lara-flow.models.flow_event'), 'flow_run_id');
    }

    public function signals(): HasMany
    {
        return $this->hasMany(config('saga-lara-flow.models.flow_signal'), 'flow_run_id');
    }

    public function tags(): HasMany
    {
        return $this->hasMany(config('saga-lara-flow.models.flow_tag'), 'flow_run_id');
    }

    public function compensations(): HasMany
    {
        return $this->hasMany(config('saga-lara-flow.models.compensation_run'), 'flow_run_id');
    }

    public function sideEffects(): HasMany
    {
        return $this->hasMany(config('saga-lara-flow.models.side_effect'), 'flow_run_id');
    }

    public function children(): HasMany
    {
        return $this->hasMany(config('saga-lara-flow.models.flow_child'), 'parent_flow_run_id');
    }

    public function parent(): BelongsTo
    {
        return $this->belongsTo(config('saga-lara-flow.models.flow_run'), 'parent_id');
    }

    public function isTerminal(): bool
    {
        return $this->status->isTerminal();
    }

    public function markRunning(): static
    {
        return $this->transitionTo(FlowStatus::Running);
    }

    public function markWaiting(): static
    {
        return $this->transitionTo(FlowStatus::Waiting);
    }

    public function markCancelling(): static
    {
        return $this->transitionTo(FlowStatus::Cancelling);
    }

    public function markCompleted(?array $result = null): static
    {
        if ($result !== null) {
            $this->result = $result;
        }

        return $this->transitionTo(FlowStatus::Completed);
    }

    public function markFailed(?array $exception = null): static
    {
        if ($exception !== null) {
            $this->exception = $exception;
        }

        return $this->transitionTo(FlowStatus::Failed);
    }

    public function markCancelled(): static
    {
        return $this->transitionTo(FlowStatus::Cancelled);
    }

    public function markExpired(): static
    {
        return $this->transitionTo(FlowStatus::Expired);
    }

    protected function transitionTo(FlowStatus $to): static
    {
        /** @var static */
        return app(StateMachine::class)->transition($this, $to);
    }
}
