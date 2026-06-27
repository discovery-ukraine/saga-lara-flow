<?php

namespace DiscoveryUkraine\SagaLaraFlow\Models;

use DiscoveryUkraine\SagaLaraFlow\Enums\ChildClosePolicy;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildStatus;
use DiscoveryUkraine\SagaLaraFlow\Models\Concerns\UsesSagaFlowConnection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;

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
