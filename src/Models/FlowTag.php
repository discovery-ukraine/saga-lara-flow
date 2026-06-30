<?php

namespace DiscoveryUkraine\SagaLaraFlow\Models;

use DiscoveryUkraine\SagaLaraFlow\Models\Concerns\UsesSagaFlowConnection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\Relations\BelongsTo;
use Illuminate\Support\Carbon;

/**
 * @property int $id
 * @property string $flow_run_id
 * @property string $key
 * @property ?string $value
 * @property ?Carbon $created_at
 * @property ?Carbon $updated_at
 * @property-read FlowRun $flowRun
 */
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
