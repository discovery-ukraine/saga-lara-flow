<?php

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\SignalStatus;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowEvent;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowSignal;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\OneActionWorkflow;
use Illuminate\Database\Eloquent\Relations\HasMany;

/**
 * Issue #44. A relation read with no order lets the driver choose the answer. SQLite
 * and MySQL happen to answer in insertion order, which is why nobody noticed;
 * PostgreSQL answers in physical order, and an updated row moves to the end of the
 * heap, so a run whose first step was rewritten hands back its second step first.
 *
 * That is not hypothetical: ReclaimStaleRunningTest went red in CI on PostgreSQL with
 * "Failed asserting that 900 is null", having been handed the second step of a
 * two-step workflow.
 *
 * Physical order is the wrong thing to reproduce, though: it depends on whether the
 * update was HOT, on page fill, on autovacuum and on the plan the driver picked, none
 * of which a test controls. Insertion order it does control — so every case below
 * writes its rows backwards. Without an order on the relation each driver hands them
 * back backwards, which is the same defect stated in a way that is deterministic on
 * all three.
 */
function orderProbeRun(): FlowRun
{
    return app(FlowRepository::class)->create([
        'workflow_class' => OneActionWorkflow::class,
        'status' => FlowStatus::Waiting,
        'arguments' => [],
    ]);
}

it('reads a run\'s steps by ordinal, not by the order they were written', function () {
    $run = orderProbeRun();

    foreach ([2, 0, 1] as $sequence) {
        ActionRun::create([
            'flow_run_id' => $run->id,
            'sequence' => $sequence,
            'action_class' => MakeValueAction::class,
            'status' => ActionStatus::Pending,
        ]);
    }

    $fresh = SagaFlow::findRun($run->id);

    expect($fresh->actions()->pluck('sequence')->all())->toBe([0, 1, 2])
        ->and($fresh->actions()->first()->sequence)->toBe(0);
});

it('reads a run\'s events in recorded order, not in the order they were written', function () {
    $run = orderProbeRun();

    // Recorded out of order on purpose: `recorded_at` is what History reads by, and it
    // is set by the recorder rather than by the insert, so the two can disagree.
    foreach ([120, 0, 60] as $offset) {
        FlowEvent::create([
            'flow_run_id' => $run->id,
            'type' => FlowEventType::FlowStarted,
            'recorded_at' => now()->addSeconds($offset),
        ]);
    }

    $recorded = SagaFlow::findRun($run->id)->events()->pluck('recorded_at')->all();
    $sorted = $recorded;
    sort($sorted);

    expect($recorded)->toHaveCount(3)->and($recorded)->toBe($sorted);
});

it('reads a run\'s signals in arrival order, not in the order they were written', function () {
    $run = orderProbeRun();

    // Written newest-first, with the ulid given rather than generated: left to itself a
    // ulid is monotonic, so insertion order and id order would agree and this would
    // prove nothing.
    foreach (['01K0000000000000000000000C', '01K0000000000000000000000A', '01K0000000000000000000000B'] as $id) {
        FlowSignal::create([
            'id' => $id,
            'flow_run_id' => $run->id,
            'name' => 'approval',
            'status' => SignalStatus::Waiting,
            'wait_sequence' => 0,
        ]);
    }

    expect(SagaFlow::findRun($run->id)->signals()->pluck('id')->all())->toBe([
        '01K0000000000000000000000A',
        '01K0000000000000000000000B',
        '01K0000000000000000000000C',
    ]);
});

/**
 * The cost of the default order, recorded rather than discovered later.
 *
 * chunkById(), lazyById() and eachById() page by a cursor on the key, and clear an
 * existing order only on the cursor column itself
 * (Query\Builder::removeExistingOrdersFor). An order on any other column therefore
 * survives and outranks `id`, and the traversal — which pages with `where id > last` —
 * can revisit a row and skip another.
 *
 * It does not bite the rows the engine writes: those are inserted in ordinal order and
 * their ulids ascend with them, so `order by sequence, id` and `order by id` agree. It
 * bites rows written in some other order, which is what this test builds.
 */
it('lets an id-cursor traversal outrank the default order, and reorder() restores it [pinning]', function () {
    $run = orderProbeRun();

    // Ids descending while sequences ascend — the disagreement the engine's own writes
    // never produce, and a backfill or an import can.
    foreach ([[0, 'C'], [1, 'B'], [2, 'A']] as [$sequence, $letter]) {
        ActionRun::create([
            'id' => '01K'.str_repeat('0', 22).$letter,
            'flow_run_id' => $run->id,
            'sequence' => $sequence,
            'action_class' => MakeValueAction::class,
            'status' => ActionStatus::Pending,
        ]);
    }

    $walk = function (bool $reorder) use ($run): array {
        $relation = SagaFlow::findRun($run->id)->actions();
        $seen = [];

        ($reorder ? $relation->reorder() : $relation)->chunkById(2, function ($chunk) use (&$seen): void {
            foreach ($chunk as $row) {
                $seen[] = $row->sequence;
            }
        });

        return $seen;
    };

    // Sequence 0 twice, sequence 2 never: `order by sequence, id` fights `where id > ?`.
    expect($walk(reorder: false))->toBe([0, 1, 0])
        ->and($walk(reorder: true))->toBe([2, 1, 0]);
});

/**
 * A pinning test over the wiring rather than over one read: every relation FlowRun
 * exposes states an order, so a relation added later cannot quietly reintroduce the
 * defect for whichever call site reads it first.
 */
it('declares an order on every relation it exposes [pinning]', function () {
    $run = new FlowRun;

    $relations = collect((new ReflectionClass(FlowRun::class))->getMethods(ReflectionMethod::IS_PUBLIC))
        ->filter(fn (ReflectionMethod $method) => (string) $method->getReturnType() === HasMany::class)
        ->map(fn (ReflectionMethod $method) => $method->getName());

    expect($relations)->toHaveCount(7);

    foreach ($relations as $name) {
        /** @var HasMany $relation */
        $relation = $run->{$name}();

        expect($relation->getQuery()->getQuery()->orders)
            ->not->toBeEmpty("FlowRun::{$name}() reads in whatever order the driver chooses.");
    }
});
