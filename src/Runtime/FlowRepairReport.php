<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

final readonly class FlowRepairReport
{
    public function __construct(
        public int $redispatchedActions = 0,
        public int $rewokenFlows = 0,
        public int $skipped = 0,
        public int $resumedRollbacks = 0,
    ) {}

    /**
     * @return array{redispatched_actions: int, rewoken_flows: int, skipped: int, resumed_rollbacks: int}
     */
    public function toArray(): array
    {
        return [
            'redispatched_actions' => $this->redispatchedActions,
            'rewoken_flows' => $this->rewokenFlows,
            'skipped' => $this->skipped,
            'resumed_rollbacks' => $this->resumedRollbacks,
        ];
    }
}
