<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Data\CompensationDefinition;
use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationFailurePolicy;

/**
 * One entry on the saga compensation stack: the compensation registered for an
 * action step (a completed one, or — opt-in via compensateStepOnSelfFailure — one that
 * failed). The effective failure policy resolves as
 * actionCompensationFailurePolicy ?? groupCompensationFailurePolicy ?? config (precedence action > group > config).
 * parallelGroupId is set only for steps that belong to a saga() group asked to
 * compensate in parallel, so SagaRunner can roll those entries back as one level.
 */
final readonly class CompensationEntry
{
    public function __construct(
        public string $actionRunId,
        public int $sequence,
        public CompensationDefinition $definition,
        public ?CompensationFailurePolicy $actionCompensationFailurePolicy = null,
        public ?CompensationFailurePolicy $groupCompensationFailurePolicy = null,
        public ?int $parallelGroupId = null,
    ) {}

    public function effectivePolicy(): CompensationFailurePolicy
    {
        return $this->actionCompensationFailurePolicy
            ?? $this->groupCompensationFailurePolicy
            ?? config('saga-lara-flow.sagas.default_compensation_failure_policy');
    }
}
