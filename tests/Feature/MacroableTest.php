<?php

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\FlowManager;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\TestWorkflow;

it('extends FlowManager via Macroable', function () {
    FlowManager::macro('totalRuns', fn (): int => $this->query()->count());

    app(FlowRepository::class)->create([
        'workflow_class' => TestWorkflow::class,
        'status' => FlowStatus::Pending,
        'arguments' => [],
    ]);

    expect(SagaFlow::totalRuns())->toBe(1);
});
