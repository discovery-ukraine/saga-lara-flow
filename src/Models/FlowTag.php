<?php

namespace DiscoveryUkraine\SagaLaraFlow\Models;

use DiscoveryUkraine\SagaLaraFlow\Models\Concerns\UsesSagaFlowConnection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;

class FlowTag extends Model
{
    use UsesSagaFlowConnection;

    protected string $baseTable = 'flow_tags';

    protected $guarded = [];

    public function flowRun(): BelongsTo
    {
        return $this->belongsTo(config('saga-lara-flow.models.flow_run'), 'flow_run_id');
    }
}
