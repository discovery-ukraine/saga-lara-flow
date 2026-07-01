<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\AutoActionWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\OneActionWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ParentAwaitWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\TenantSpy;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\TenantWorkflow;
use Illuminate\Support\Facades\Queue;

beforeEach(function () {
    TenantSpy::reset();
    config()->set('saga-lara-flow.tenancy.capture', [TenantSpy::class, 'capture']);
    config()->set('saga-lara-flow.tenancy.restore', [TenantSpy::class, 'restore']);
});

/**
 * @return array{context: array<int|string, mixed>|null, ambient: ?string}
 */
function firstActionResult(string $flowRunId): array
{
    /** @var array{context: array<int|string, mixed>|null, ambient: ?string} $result */
    $result = SagaFlow::findRun($flowRunId)->actions()->orderBy('sequence')->first()->result;

    return $result;
}

it('captures the tenant context onto the run at creation', function () {
    Queue::fake();

    TenantSpy::$current = 'acme';

    $run = SagaFlow::create(TenantWorkflow::class)->run();

    expect($run->tenancy_context)->toBe(['tenant' => 'acme']);
});

it('inherits the parent tenant context onto a child run', function () {
    TenantSpy::$current = 'acme';

    $parent = SagaFlow::create(ParentAwaitWorkflow::class)->runSync();

    $child = FlowRun::query()->where('parent_id', $parent->id)->first();

    expect($child)->not->toBeNull()
        ->and($child->tenancy_context)->toBe(['tenant' => 'acme']);
});

it('auto-restores the tenant around a queued action when enabled', function () {
    useDatabaseQueue();
    config()->set('saga-lara-flow.tenancy.auto', true);

    TenantSpy::$current = 'acme';
    $run = SagaFlow::create(TenantWorkflow::class)->run();

    TenantSpy::reset(); // worker starts in the central context

    drainQueue();

    $result = firstActionResult($run->id);

    expect($result['context'])->toBe(['tenant' => 'acme'])
        ->and($result['ambient'])->toBe('acme')
        ->and(TenantSpy::$current)->toBeNull(); // reverted after the job
});

it('does not leak tenant context between runs sharing a worker', function () {
    useDatabaseQueue();
    config()->set('saga-lara-flow.tenancy.auto', true);

    TenantSpy::$current = 'tenant-a';
    $a = SagaFlow::create(TenantWorkflow::class)->run();

    TenantSpy::$current = 'tenant-b';
    $b = SagaFlow::create(TenantWorkflow::class)->run();

    TenantSpy::reset(); // both runs now drain on a central-context worker

    drainQueue();

    $resultA = firstActionResult($a->id);
    $resultB = firstActionResult($b->id);

    expect($resultA['ambient'])->toBe('tenant-a')
        ->and($resultB['ambient'])->toBe('tenant-b')
        ->and($resultA['context'])->toBe(['tenant' => 'tenant-a'])
        ->and($resultB['context'])->toBe(['tenant' => 'tenant-b'])
        ->and(TenantSpy::$current)->toBeNull(); // no residue after the batch
});

it('exposes the tenant for discovery but does not auto-restore in manual mode', function () {
    useDatabaseQueue(); // config auto stays false (default)

    TenantSpy::$current = 'acme';
    $run = SagaFlow::create(TenantWorkflow::class)->run();

    TenantSpy::reset();

    drainQueue();

    $result = firstActionResult($run->id);

    expect($result['context'])->toBe(['tenant' => 'acme']) // discovery works
        ->and($result['ambient'])->toBeNull(); // not restored — manual control
});

it('lets a #[Tenancy(auto: true)] action override the config default of off', function () {
    useDatabaseQueue(); // config auto stays false

    TenantSpy::$current = 'acme';
    $run = SagaFlow::create(AutoActionWorkflow::class)->run();

    TenantSpy::reset();

    drainQueue();

    $result = firstActionResult($run->id);

    expect($result['ambient'])->toBe('acme'); // restored via the attribute
});

it('is a no-op with no tenancy hooks configured', function () {
    config()->set('saga-lara-flow.tenancy.capture', null);
    config()->set('saga-lara-flow.tenancy.restore', null);

    $run = SagaFlow::create(OneActionWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Completed)
        ->and($run->tenancy_context)->toBeNull();
});
