<?php

namespace DiscoveryUkraine\SagaLaraFlow\Models;

use DiscoveryUkraine\SagaLaraFlow\Enums\ChildClosePolicy;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildStatus;
use DiscoveryUkraine\SagaLaraFlow\Models\Concerns\UsesSagaFlowConnection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Support\Carbon;

/**
 * @property int $id
 * @property string $parent_flow_run_id
 * @property string $child_flow_run_id
 * @property int $sequence
 * @property string $child_workflow_class
 * @property ChildClosePolicy $close_policy
 * @property bool $continue_parent_on_failure
 * @property ChildStatus $status
 * @property ?Carbon $created_at
 * @property ?Carbon $updated_at
 * @property-read FlowRun $child
 * @property-read FlowRun $parent
 */
class FlowChild extends Model
{
    use UsesSagaFlowConnection;

    protected string $baseTable = 'flow_children';

    protected $guarded = [];

    protected function casts(): array
    {
        return [
            'sequence' => 'integer',
            'close_policy' => ChildClosePolicy::class,
            'continue_parent_on_failure' => 'boolean',
            'status' => ChildStatus::class,
        ];
    }

    public function parent(): BelongsTo
    {
        return $this->belongsTo(config('saga-lara-flow.models.flow_run'), 'parent_flow_run_id');
    }

    public function child(): BelongsTo
    {
        return $this->belongsTo(config('saga-lara-flow.models.flow_run'), 'child_flow_run_id');
    }
}
