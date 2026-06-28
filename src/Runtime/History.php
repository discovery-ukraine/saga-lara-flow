<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Eloquent\Model;

/**
 * Read model over a run's recorded progress: the ordered event log and the
 * action runs keyed by their replay sequence. Read-only — it never replays the
 * workflow.
 */
readonly class History
{
    public function __construct(
        private FlowRun $flowRun,
    ) {}

    /**
     * The full event log in recorded order.
     *
     * @return Collection<int, Model>
     */
    public function events(): Collection
    {
        return $this->flowRun->events()
            ->orderBy('recorded_at')
            ->orderBy('id')
            ->get();
    }

    /**
     * Action runs ordered by their (flow_run_id, sequence) identity.
     *
     * @return Collection<int, Model>
     */
    public function actions(): Collection
    {
        return $this->flowRun->actions()
            ->orderBy('sequence')
            ->get();
    }
}
