<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ChildThenSignalWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ChildValueRelayWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ModelChildParentWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ScalarChildCompensationWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\TypedScalarParentWorkflow;
use Illuminate\Support\Facades\DB;

beforeEach(function () {
    CompensationLog::reset();
    // Run dispatched jobs inline on the default sync queue driver.
    config()->set('saga-lara-flow.queue.after_commit', false);
});

it('hands a parent the scalar its child returned (sync)', function () {
    $run = SagaFlow::create(ChildValueRelayWorkflow::class)->withArguments(42)->runSync();

    expect($run->status)->toBe(FlowStatus::Completed)
        ->and($run->result['type'])->toBe('int')
        ->and($run->result['received'])->toBe(42);
});

it('hands a parent the scalar its child returned over the real queue', function () {
    useDatabaseQueue();

    $run = SagaFlow::create(ChildValueRelayWorkflow::class)->withArguments(42)->run();
    drainQueue();

    $final = SagaFlow::findRun($run->id);

    expect($final->status)->toBe(FlowStatus::Completed)
        ->and($final->result['type'])->toBe('int')
        ->and($final->result['received'])->toBe(42);
});

it('lets a parent pass a child result straight into a typed parameter', function () {
    $run = SagaFlow::create(TypedScalarParentWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Completed)
        ->and($run->result)->toBe('id-42');
});

it('keeps false, zero, an empty string and null distinguishable across the seam', function (mixed $value, string $type) {
    $run = SagaFlow::create(ChildValueRelayWorkflow::class)->withArguments($value)->runSync();

    expect($run->status)->toBe(FlowStatus::Completed)
        ->and($run->result['type'])->toBe($type)
        ->and($run->result['received'])->toBe($value);
})->with([
    'false' => [false, 'bool'],
    'true' => [true, 'bool'],
    'zero' => [0, 'int'],
    'empty string' => ['', 'string'],
    'float' => [0.5, 'float'],
    'null' => [null, 'null'],
]);

it('stores a scalar workflow result unwrapped on the run itself', function () {
    $run = SagaFlow::create(TypedScalarParentWorkflow::class)->runSync();

    // Read past the model cast: the column itself must hold the bare JSON value.
    $stored = DB::table((new FlowRun)->getTable())->where('id', $run->id)->value('result');

    expect($stored)->toBe('"id-42"');
});

it('resolves the child to the same scalar on the compensation-only replay', function () {
    $run = SagaFlow::create(ScalarChildCompensationWorkflow::class)->runSync();

    // Parked on awaitSignal('go').
    expect($run->status)->toBe(FlowStatus::Waiting);

    $compensated = SagaFlow::loadFlow($run->id)->compensate();

    expect($compensated->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toBe(['child:int']);
});

// Pinning: an array result is stored as it stands, so a child that legitimately
// returns ['value' => ...] is indistinguishable from a wrapped scalar. Nothing
// unwraps it, which is why the wrapper was dropped at the write instead.
it('leaves an array result exactly as the child returned it', function () {
    $run = SagaFlow::create(ChildValueRelayWorkflow::class)
        ->withArguments(['value' => 42])
        ->runSync();

    expect($run->status)->toBe(FlowStatus::Completed)
        ->and($run->result['type'])->toBe('array')
        ->and($run->result['received'])->toBe(['value' => 42]);
});

// Pinning: models travel as a reference array, a shape the write path never
// touched and must keep not touching.
it('rehydrates a model a child returned', function () {
    $run = SagaFlow::create(ModelChildParentWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Completed)
        ->and($run->result['type'])->toBe(FlowRun::class);
});

// Pinning: a row written before the envelope was dropped keeps it, and the seam
// resolves it as stored on every replay — the behaviour UPGRADING describes
// under the scalar result. Unwrapping it here would corrupt a child that legitimately
// returned ['value' => ...].
it('resolves a child stored with the old envelope exactly as it stands', function () {
    $run = SagaFlow::create(ChildThenSignalWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Waiting);

    $childId = $run->children()->first()->child_flow_run_id;

    // What the write path produced before this release for a scalar result.
    DB::table((new FlowRun)->getTable())
        ->where('id', $childId)
        ->update(['result' => json_encode(['value' => 42])]);

    SagaFlow::loadFlow($run->id)->signal('go');

    $final = SagaFlow::findRun($run->id);

    expect($final->status)->toBe(FlowStatus::Completed)
        ->and($final->result['type'])->toBe('array')
        ->and($final->result['received'])->toBe(['value' => 42]);
});
