<?php

namespace DiscoveryUkraine\SagaLaraFlow\Data;

use DateTimeInterface;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildClosePolicy;
use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationFailurePolicy;

final readonly class ChildSchedule
{
    /**
     * @param  array<int, mixed>  $arguments
     */
    public function __construct(
        public string $workflowClass,
        public array $arguments,
        public ChildClosePolicy $closePolicy,
        public bool $continueParentOnFailure = false,
        public ?DateTimeInterface $expiresAt = null,
        public ?CompensationDefinition $compensation = null,
        public ?CompensationFailurePolicy $compensationFailurePolicy = null,
        public ?SignalRetry $retry = null,
    ) {}
}
