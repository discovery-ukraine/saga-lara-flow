<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

/**
 * The outcome of one FlowDoctor::repair() pass: how many stuck actions were
 * re-dispatched, how many stuck flows were re-woken, how many stalled rollbacks were
 * resumed, and how many candidates were skipped because they no longer qualified once
 * re-checked, or could not be acted on.
 * A small structured report so callers (the command, future observability)
 * can read counts without parsing text.
 */
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
