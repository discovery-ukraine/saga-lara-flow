<?php

use DiscoveryUkraine\SagaLaraFlow\Contracts\ActionRunRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Data\ActionSchedule;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Middleware\LockMiddlewareFactory;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ActionRecorder;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowDoctor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\OneActionWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ReclaimOverrideWorkflow;
use Illuminate\Queue\Middleware\WithoutOverlapping;

/**
 * Issue #13's `actions.reclaim.stale_running` / `sagas.reclaim.stale_running`: an
 * opt-in, off-by-default mechanism that lets a Running row be claimed again once it
 * has sat at least its own reclaim_stale_after_seconds since started_at. Every test
 * here that touches ActionRecorder::startAction() directly mirrors the style already
 * used for SignalRecorder's compare-and-swap in RetryOnSignalQueuedTest.
 */
function stagedRun(): FlowRun
{
    return app(FlowRepository::class)->create([
        'workflow_class' => OneActionWorkflow::class,
        'status' => FlowStatus::Waiting,
        'arguments' => [],
    ]);
}

it('does not let a Running action be reclaimed by default (mechanism off)', function () {
    $run = stagedRun();

    $action = ActionRun::create([
        'flow_run_id' => $run->id,
        'sequence' => 0,
        'action_class' => MakeValueAction::class,
        'status' => ActionStatus::Running,
        'started_at' => now()->subDay(),
        'attempts' => 1,
        // reclaim_stale_after_seconds left null — today's behaviour, unchanged.
    ]);

    expect(app(ActionRecorder::class)->startAction($action))->toBeFalse();
});

it('does not reclaim a Running action before its own threshold has elapsed', function () {
    $run = stagedRun();

    $action = ActionRun::create([
        'flow_run_id' => $run->id,
        'sequence' => 0,
        'action_class' => MakeValueAction::class,
        'status' => ActionStatus::Running,
        'started_at' => now()->subSeconds(10),
        'reclaim_stale_after_seconds' => 900,
        // Claimed 10s ago: the deadline is still 890s away.
        'reclaim_stale_at' => now()->subSeconds(10)->addSeconds(900),
        'attempts' => 1,
    ]);

    expect(app(ActionRecorder::class)->startAction($action))->toBeFalse();
});

it('reclaims a Running action once its own threshold has elapsed', function () {
    $run = stagedRun();

    $action = ActionRun::create([
        'flow_run_id' => $run->id,
        'sequence' => 0,
        'action_class' => MakeValueAction::class,
        'status' => ActionStatus::Running,
        'started_at' => now()->subSeconds(1000),
        'reclaim_stale_after_seconds' => 900,
        // What the claim would have written 1000s ago: started_at + threshold, so
        // the deadline is 100s in the past and the row is due for reclaim.
        'reclaim_stale_at' => now()->subSeconds(1000)->addSeconds(900),
        'attempts' => 1,
    ]);

    expect(app(ActionRecorder::class)->startAction($action))->toBeTrue()
        ->and($action->fresh()->status)->toBe(ActionStatus::Running)
        ->and($action->fresh()->attempts)->toBe(2);
});

it('leaves reclaim_stale_after_seconds null on a freshly scheduled action when config is off (default)', function () {
    $run = stagedRun();

    $action = app(ActionRecorder::class)->scheduleAction($run, 0, new ActionSchedule(MakeValueAction::class, ['x']));

    expect($action->reclaim_stale_after_seconds)->toBeNull();
});

it('resolves reclaim_stale_after_seconds from config once the mechanism is globally enabled', function () {
    config()->set('saga-lara-flow.actions.reclaim.stale_running.enabled', true);
    config()->set('saga-lara-flow.actions.reclaim.stale_running.after_seconds', 123);

    $run = stagedRun();

    $action = app(ActionRecorder::class)->scheduleAction($run, 0, new ActionSchedule(MakeValueAction::class, ['x']));

    expect($action->reclaim_stale_after_seconds)->toBe(123);
});

it('lets a per-action explicit threshold win regardless of the enabled flags', function () {
    $run = SagaFlow::create(ReclaimOverrideWorkflow::class)
        ->withArguments(42)
        ->runSync();

    // Ordered, not just first: ReclaimOverrideWorkflow schedules a second step that
    // carries no override, and an unordered read is free to hand it back instead.
    expect($run->actions()->orderBy('sequence')->first()->reclaim_stale_after_seconds)->toBe(42);
});

it('lets a per-action override force-enable reclaim (config default seconds) even when config is off', function () {
    $run = SagaFlow::create(ReclaimOverrideWorkflow::class)
        ->withArguments(null, true)
        ->runSync();

    expect($run->actions()->orderBy('sequence')->first()->reclaim_stale_after_seconds)
        ->toBe((int) config('saga-lara-flow.actions.reclaim.stale_running.after_seconds'));
});

it('lets a per-action override force-disable reclaim even when config is globally on', function () {
    config()->set('saga-lara-flow.actions.reclaim.stale_running.enabled', true);

    $run = SagaFlow::create(ReclaimOverrideWorkflow::class)
        ->withArguments(null, false)
        ->runSync();

    expect($run->actions()->orderBy('sequence')->first()->reclaim_stale_after_seconds)->toBeNull();
});

it('resolves the compensation-side reclaim threshold independently of the action-side one', function () {
    // The second step in ReclaimOverrideWorkflow always fails, forcing a real
    // rollback so the registered compensation gets its own CompensationRun row.
    $run = SagaFlow::create(ReclaimOverrideWorkflow::class)
        ->withArguments(42, null, 77)
        ->runSync();

    expect($run->status)->toBe(FlowStatus::Failed)
        ->and($run->actions()->orderBy('sequence')->first()->reclaim_stale_after_seconds)->toBe(42)
        ->and($run->compensations()->orderBy('sequence')->first()->reclaim_stale_after_seconds)->toBe(77);
});

it('gives RunCompensationJob its own WithoutOverlapping lock, independent of the action lock', function () {
    $middleware = app(LockMiddlewareFactory::class)->compensationMiddleware('comp-run-id');

    expect($middleware)->toHaveCount(1)
        ->and($middleware[0])->toBeInstanceOf(WithoutOverlapping::class);

    $actionMiddleware = app(LockMiddlewareFactory::class)->actionMiddleware('comp-run-id');

    // Same raw id, different lock key prefix ("action:" vs "compensation:"), so the
    // two locks never collide even if called for the same underlying id by mistake.
    expect($middleware[0])->not->toEqual($actionMiddleware[0]);
});

it('returns no lock middleware for compensations when locking is disabled', function () {
    config()->set('saga-lara-flow.locks.enabled', false);

    expect(app(LockMiddlewareFactory::class)->compensationMiddleware('comp-run-id'))->toBe([]);
});

it('inherits the action TTL when an application published its config before compensation_ttl_seconds existed', function () {
    // Exactly what shallow config merging leaves behind: the host's own 'locks'
    // array, tuned for long steps, with no key for the one added in 1.2.0.
    config()->set('saga-lara-flow.locks.compensation_ttl_seconds', null);
    config()->set('saga-lara-flow.locks.action_ttl_seconds', 3600);

    $middleware = app(LockMiddlewareFactory::class)->compensationMiddleware('comp-run-id');

    expect($middleware[0]->expiresAfter)->toBe(3600);
});

it('never turns a missing or zero TTL into a lock that outlives the worker', function () {
    // Zero reaches Redis as SETNX with no expiry, so a worker killed before
    // WithoutOverlapping's finally runs would wedge the row for good.
    config()->set('saga-lara-flow.locks.compensation_ttl_seconds', 0);
    config()->set('saga-lara-flow.locks.action_ttl_seconds', 0);

    $middleware = app(LockMiddlewareFactory::class)->compensationMiddleware('comp-run-id');

    expect($middleware[0]->expiresAfter)->toBeGreaterThan(0);
});

it('redispatches a stale Running action enabled per step while reclaim is globally off', function () {
    useDatabaseQueue();
    config()->set('saga-lara-flow.repair.enabled', true);
    config()->set('saga-lara-flow.actions.reclaim.stale_running.enabled', false);

    $run = stagedRun();

    // A step that opted itself in via ->reclaimStaleAfter()/->enableStaleReclaim():
    // the per-step override wins over the global switch, here as everywhere else.
    $action = ActionRun::create([
        'flow_run_id' => $run->id,
        'sequence' => 0,
        'action_class' => MakeValueAction::class,
        'arguments' => ['x'],
        'status' => ActionStatus::Running,
        'started_at' => now()->subSeconds(1000),
        'reclaim_stale_after_seconds' => 900,
        'reclaim_stale_at' => now()->subSeconds(1000)->addSeconds(900),
        'attempts' => 1,
    ]);

    expect(app(FlowDoctor::class)->repair()->redispatchedActions)->toBe(1);

    drainQueue();

    expect($action->fresh()->status)->toBe(ActionStatus::Completed);
});

it('reaches a due row that a backlog of not-yet-due rows would otherwise hide', function () {
    config()->set('saga-lara-flow.repair.enabled', true);
    config()->set('saga-lara-flow.repair.batch_size', 2);

    $run = stagedRun();

    // Older rows with long windows that have NOT elapsed. Ordered by started_at they
    // sit ahead of the due row below, so a candidate window filtered afterwards in
    // PHP would be filled entirely with them, pass after pass.
    foreach (range(0, 9) as $i) {
        ActionRun::create([
            'flow_run_id' => $run->id,
            'sequence' => $i + 1,
            'action_class' => MakeValueAction::class,
            'arguments' => ['x'],
            'status' => ActionStatus::Running,
            'started_at' => now()->subSeconds(5000 + $i),
            'reclaim_stale_after_seconds' => 86400,
            'reclaim_stale_at' => now()->addSeconds(80000),
            'attempts' => 1,
        ]);
    }

    // Newer, short window, already elapsed — the row that actually needs recovering.
    $due = ActionRun::create([
        'flow_run_id' => $run->id,
        'sequence' => 0,
        'action_class' => MakeValueAction::class,
        'arguments' => ['x'],
        'status' => ActionStatus::Running,
        'started_at' => now()->subSeconds(60),
        'reclaim_stale_after_seconds' => 30,
        'reclaim_stale_at' => now()->subSeconds(30),
        'attempts' => 1,
    ]);

    $candidates = collect(app(ActionRunRepository::class)->dueForStaleRunningRepair(2, 10));

    expect($candidates->pluck('id'))->toContain($due->id);
});

// --- FlowDoctor R3: active recovery for a stuck sequential Running action ---

it('redispatches a stuck sequential Running action once past its own reclaim window', function () {
    useDatabaseQueue();
    config()->set('saga-lara-flow.repair.enabled', true);
    config()->set('saga-lara-flow.actions.reclaim.stale_running.enabled', true);

    $run = stagedRun();

    $action = ActionRun::create([
        'flow_run_id' => $run->id,
        'sequence' => 0,
        'action_class' => MakeValueAction::class,
        'arguments' => ['x'],
        'status' => ActionStatus::Running,
        'started_at' => now()->subSeconds(1000),
        'reclaim_stale_after_seconds' => 900,
        // What the claim would have written 1000s ago: started_at + threshold, so
        // the deadline is 100s in the past and the row is due for reclaim.
        'reclaim_stale_at' => now()->subSeconds(1000)->addSeconds(900),
        'attempts' => 1,
    ]);

    $report = app(FlowDoctor::class)->repair();

    expect($report->redispatchedActions)->toBe(1);

    drainQueue();

    expect($action->fresh()->status)->toBe(ActionStatus::Completed);
});

it('does not touch a Running action still inside its reclaim window', function () {
    config()->set('saga-lara-flow.repair.enabled', true);
    config()->set('saga-lara-flow.actions.reclaim.stale_running.enabled', true);

    $run = stagedRun();

    ActionRun::create([
        'flow_run_id' => $run->id,
        'sequence' => 0,
        'action_class' => MakeValueAction::class,
        'arguments' => ['x'],
        'status' => ActionStatus::Running,
        'started_at' => now()->subSeconds(10),
        'reclaim_stale_after_seconds' => 900,
        // Claimed 10s ago: the deadline is still 890s away.
        'reclaim_stale_at' => now()->subSeconds(10)->addSeconds(900),
        'attempts' => 1,
    ]);

    expect(app(FlowDoctor::class)->repair()->redispatchedActions)->toBe(0);
});

it('never touches a stale Running action when redispatch_stale_running_actions is off', function () {
    config()->set('saga-lara-flow.repair.enabled', true);
    config()->set('saga-lara-flow.actions.reclaim.stale_running.enabled', true);
    config()->set('saga-lara-flow.repair.redispatch_stale_running_actions', false);

    $run = stagedRun();

    ActionRun::create([
        'flow_run_id' => $run->id,
        'sequence' => 0,
        'action_class' => MakeValueAction::class,
        'arguments' => ['x'],
        'status' => ActionStatus::Running,
        'started_at' => now()->subSeconds(1000),
        'reclaim_stale_after_seconds' => 900,
        // What the claim would have written 1000s ago: started_at + threshold, so
        // the deadline is 100s in the past and the row is due for reclaim.
        'reclaim_stale_at' => now()->subSeconds(1000)->addSeconds(900),
        'attempts' => 1,
    ]);

    expect(app(FlowDoctor::class)->repair()->redispatchedActions)->toBe(0);
});

it('never touches a stale Running action belonging to a parallel block', function () {
    config()->set('saga-lara-flow.repair.enabled', true);
    config()->set('saga-lara-flow.actions.reclaim.stale_running.enabled', true);

    $run = stagedRun();

    ActionRun::create([
        'flow_run_id' => $run->id,
        'sequence' => 0,
        'action_class' => MakeValueAction::class,
        'arguments' => ['x'],
        'status' => ActionStatus::Running,
        'started_at' => now()->subSeconds(1000),
        'reclaim_stale_after_seconds' => 900,
        'parallel_group' => 1,
        'attempts' => 1,
    ]);

    expect(app(FlowDoctor::class)->repair()->redispatchedActions)->toBe(0);
});
