<?php

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\FlowHandle;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\TestWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\TwoStepWorkflow;
use Illuminate\Contracts\Pagination\LengthAwarePaginator;

/**
 * @param  array<string, ?string>  $tags
 */
function makeQueryableRun(FlowStatus $status, string $workflowClass, array $tags = []): FlowRun
{
    $normalized = [];

    foreach ($tags as $key => $value) {
        $normalized[] = ['key' => $key, 'value' => $value];
    }

    return app(FlowRepository::class)->create([
        'workflow_class' => $workflowClass,
        'status' => $status,
        'arguments' => [],
    ], $normalized);
}

beforeEach(function () {
    $this->running = makeQueryableRun(FlowStatus::Running, TestWorkflow::class, ['order' => '1']);
    $this->failed = makeQueryableRun(FlowStatus::Failed, TestWorkflow::class, ['order' => '2']);
    $this->completed = makeQueryableRun(FlowStatus::Completed, TwoStepWorkflow::class, ['order' => '1']);
});

it('filters by status', function () {
    expect(SagaFlow::query()->whereStatus(FlowStatus::Running)->get()->pluck('id')->all())
        ->toBe([$this->running->id]);

    expect(SagaFlow::query()->failed()->get()->pluck('id')->all())
        ->toBe([$this->failed->id]);
});

it('filters to signalable (non-terminal, non-cancelling) runs', function () {
    $pending = makeQueryableRun(FlowStatus::Pending, TestWorkflow::class);
    $waiting = makeQueryableRun(FlowStatus::Waiting, TestWorkflow::class);
    makeQueryableRun(FlowStatus::Cancelling, TestWorkflow::class);
    makeQueryableRun(FlowStatus::Cancelled, TestWorkflow::class);
    makeQueryableRun(FlowStatus::Expired, TestWorkflow::class);

    // Running is matched, Waiting is matched (a flow parked on awaitSignal()),
    // Pending is matched; Cancelling and every terminal status are excluded.
    $expected = [$this->running->id, $pending->id, $waiting->id];

    expect(SagaFlow::query()->active()->get()->pluck('id')->all())
        ->toEqualCanonicalizing($expected);

    // signalable() is an alias of active().
    expect(SagaFlow::query()->signalable()->get()->pluck('id')->all())
        ->toEqualCanonicalizing($expected);
});

it('filters by workflow class', function () {
    expect(SagaFlow::query()->whereWorkflow(TwoStepWorkflow::class)->get()->pluck('id')->all())
        ->toBe([$this->completed->id]);
});

it('filters by tag key and value', function () {
    expect(SagaFlow::query()->whereTag('order', '1')->get()->pluck('id')->all())
        ->toEqualCanonicalizing([$this->running->id, $this->completed->id]);

    // Key-only matches any value.
    expect(SagaFlow::query()->whereTag('order')->count())->toBe(3);
});

it('combines filters', function () {
    $first = SagaFlow::query()
        ->whereWorkflow(TestWorkflow::class)
        ->whereTag('order', '1')
        ->first();

    expect($first)->not->toBeNull()
        ->and($first->id)->toBe($this->running->id);
});

it('hydrates handles and paginates', function () {
    $handles = SagaFlow::query()->whereTag('order')->handles();

    expect($handles)->toHaveCount(3)
        ->and($handles->first())->toBeInstanceOf(FlowHandle::class);

    $page = SagaFlow::query()->whereTag('order')->paginate(2);

    expect($page)->toBeInstanceOf(LengthAwarePaginator::class)
        ->and($page->total())->toBe(3)
        ->and($page->count())->toBe(2);
});

it('filters by created_at window', function () {
    FlowRun::query()->whereKey($this->failed->id)->update(['created_at' => now()->subDays(5)]);

    expect(SagaFlow::query()->before(now()->subDay())->get()->pluck('id')->all())
        ->toBe([$this->failed->id]);

    expect(SagaFlow::query()->after(now()->subDay())->get()->pluck('id')->all())
        ->toEqualCanonicalizing([$this->running->id, $this->completed->id]);
});
