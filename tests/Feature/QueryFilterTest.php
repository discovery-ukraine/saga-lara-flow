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

it('filters by run id', function () {
    expect(SagaFlow::query()->whereId($this->running->id, $this->completed->id)->count())->toBe(2)
        ->and(SagaFlow::query()->whereId($this->failed->id)->first()?->id)->toBe($this->failed->id)
        ->and(SagaFlow::query()->whereId($this->running->id, $this->completed->id)->signalable()->count())->toBe(1);
});

it('matches no run when no id is given', function () {
    expect(SagaFlow::query()->whereId()->count())->toBe(0)
        ->and(SagaFlow::query()->whereId(...[])->get()->all())->toBe([]);
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

it('filters by any of several values of one tag', function () {
    makeQueryableRun(FlowStatus::Running, TestWorkflow::class, ['order' => '3', 'shop' => '1']);

    expect(SagaFlow::query()->whereTagIn('order', ['1', '2'])->get()->pluck('id')->all())
        ->toEqualCanonicalizing([$this->running->id, $this->failed->id, $this->completed->id])
        ->and(SagaFlow::query()->whereTagIn('order', ['first' => '2', 'second' => '9'])->get()->pluck('id')->all())
        ->toBe([$this->failed->id])
        ->and(SagaFlow::query()->whereTagIn('shop', ['2'])->count())->toBe(0)
        // The key bounds the values: '1' under another key does not count.
        ->and(SagaFlow::query()->whereTagIn('shop', ['1'])->count())->toBe(1)
        // Narrowed by the filters around it, before and after alike.
        ->and(SagaFlow::query()->whereTagIn('order', ['1'])->failed()->count())->toBe(0)
        ->and(SagaFlow::query()->failed()->whereTagIn('order', ['1'])->count())->toBe(0)
        ->and(SagaFlow::query()->whereWorkflow(TwoStepWorkflow::class)->whereTagIn('order', ['1', '2'])->get()->pluck('id')->all())
        ->toBe([$this->completed->id]);
});

it('matches no run when no tag value is given', function () {
    expect(SagaFlow::query()->whereTagIn('order', [])->count())->toBe(0)
        ->and(SagaFlow::query()->whereTagIn('order', [])->get()->all())->toBe([]);
});

it('compares a numeric tag value as the string it was written as', function () {
    $seven = makeQueryableRun(FlowStatus::Running, TestWorkflow::class);
    SagaFlow::query()->whereId($seven->id)->handles()->first()->tag('customer', 7);
    makeQueryableRun(FlowStatus::Running, TestWorkflow::class, ['customer' => '007']);

    // Handed an integer, MySQL compares the column as a number and takes '007' for 7; SQLite and
    // PostgreSQL compare it as text either way, so only MySQL tells the cast apart.
    expect(SagaFlow::query()->whereTagIn('customer', [7, 12])->get()->pluck('id')->all())
        ->toBe([$seven->id])
        ->and(SagaFlow::query()->whereTag('customer', 7)->get()->pluck('id')->all())
        ->toBe([$seven->id]);
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
