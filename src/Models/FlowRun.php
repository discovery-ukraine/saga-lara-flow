<?php

namespace DiscoveryUkraine\SagaLaraFlow\Models;

use Carbon\CarbonInterface;
use DiscoveryUkraine\SagaLaraFlow\Contracts\StateMachine;
use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Models\Concerns\UsesSagaFlowConnection;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Concerns\HasUlids;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Database\Eloquent\Relations\HasMany;
use Illuminate\Support\Carbon;
use Illuminate\Support\Str;

/**
 * @property string $id
 * @property string $workflow_class
 * @property ?string $workflow_name
 * @property ?string $workflow_version
 * @property FlowStatus $status
 * @property ?array<int|string, mixed> $arguments
 * @property mixed $result
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
 * @property int $expiry_attempts
 * @property ?Carbon $expiry_available_at
 * @property ?string $rollback_token
 * @property ?FlowStatus $rollback_target
 * @property ?array<string, mixed> $rollback_cause
 * @property bool $rollback_compensates
 * @property ?Carbon $created_at
 * @property ?Carbon $updated_at
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
            'expiry_attempts' => 'integer',
            'expiry_available_at' => 'datetime',
            'rollback_target' => FlowStatus::class,
            'rollback_cause' => 'array',
            'rollback_compensates' => 'boolean',
        ];
    }

    /**
     * Cancelling is live: settling its rows is what a rollback needs. Typed over any model because
     * whereHas() hands its constraint the related query as Builder<Model>.
     *
     * @param  Builder<Model>  $query
     */
    public static function live(Builder $query): void
    {
        $query->whereNotIn('status', FlowStatus::terminal());
    }

    /**
     * live() minus Cancelling: asked wherever work begins, never where started work is written
     * down.
     *
     * @param  Builder<Model>  $query
     */
    public static function mayStartWork(Builder $query): void
    {
        $query->whereIn('status', FlowStatus::mayStartWork());
    }

    /**
     * Cancelling, untouched since $since, and no compensation touched since or held Running by a
     * worker that may still be alive.
     *
     * @param  Builder<FlowRun>  $query
     */
    public static function rollbackStalled(Builder $query, CarbonInterface $since): void
    {
        $query->where('status', FlowStatus::Cancelling)
            ->where('updated_at', '<=', $since)
            ->whereDoesntHave('compensations', function (Builder $compensation) use ($since): void {
                $compensation->where(function (Builder $moving) use ($since): void {
                    $moving->where('updated_at', '>', $since)
                        ->orWhere(function (Builder $held): void {
                            $held->where('status', CompensationStatus::Running)
                                ->where(function (Builder $reclaim): void {
                                    $reclaim->whereNull('reclaim_stale_at')
                                        ->orWhere('reclaim_stale_at', '>', Carbon::now());
                                });
                        });
                });
            });
    }

    /**
     * Every relation below is ordered (PostgreSQL otherwise returns physical order); a reverse
     * order or chunkById() needs reorder() first.
     */
    public function actions(): HasMany
    {
        return $this->hasMany(config('saga-lara-flow.models.action_run'), 'flow_run_id')
            ->orderBy('sequence');
    }

    public function events(): HasMany
    {
        return $this->hasMany(config('saga-lara-flow.models.flow_event'), 'flow_run_id')
            ->orderBy('recorded_at')
            ->orderBy('id');
    }

    public function signals(): HasMany
    {
        return $this->hasMany(config('saga-lara-flow.models.flow_signal'), 'flow_run_id')
            ->orderBy('id');
    }

    public function tags(): HasMany
    {
        return $this->hasMany(config('saga-lara-flow.models.flow_tag'), 'flow_run_id')
            ->orderBy('id');
    }

    /**
     * By id as well: (flow_run_id, sequence) is not unique on compensation_runs.
     */
    public function compensations(): HasMany
    {
        return $this->hasMany(config('saga-lara-flow.models.compensation_run'), 'flow_run_id')
            ->orderBy('sequence')
            ->orderBy('id');
    }

    public function sideEffects(): HasMany
    {
        return $this->hasMany(config('saga-lara-flow.models.side_effect'), 'flow_run_id')
            ->orderBy('sequence');
    }

    public function children(): HasMany
    {
        return $this->hasMany(config('saga-lara-flow.models.flow_child'), 'parent_flow_run_id')
            ->orderBy('sequence');
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

    /**
     * Written by the same conditional write as the move, so whoever moves the run owns its
     * rollback.
     *
     * @param  array<string, mixed>|null  $cause
     * @param  ?string  $token  the owner's token, when the owner has to recognise it again later
     */
    public function intendRollback(
        FlowStatus $target,
        ?array $cause,
        bool $compensates = true,
        ?string $token = null
    ): static {
        $this->rollback_token = $token ?? (string) Str::ulid();
        $this->rollback_target = $target;
        $this->rollback_cause = $cause;
        $this->rollback_compensates = $compensates;

        return $this;
    }

    public function parentFenceToken(): string
    {
        return (string) $this->getKey();
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
