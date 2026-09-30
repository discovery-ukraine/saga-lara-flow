<?php

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowSignal;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\FlakyPaymentAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ManualCompensateWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\NamedRetryWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\SignalOnlyWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\TwoStepWorkflow;
use Illuminate\Support\Facades\Artisan;

function cliPendingRun(): FlowRun
{
    return app(FlowRepository::class)->create([
        'workflow_class' => TwoStepWorkflow::class,
        'status' => FlowStatus::Pending,
        'arguments' => [],
    ]);
}

it('lists runs filtered by status', function () {
    $running = app(FlowRepository::class)->create([
        'workflow_class' => TwoStepWorkflow::class,
        'status' => FlowStatus::Running,
        'arguments' => [],
    ]);

    $this->artisan('saga-flow:list', ['--status' => 'running'])
        ->expectsOutputToContain($running->id)
        ->assertSuccessful();
});

it('rejects an unknown status', function () {
    $this->artisan('saga-flow:list', ['--status' => 'bogus'])
        ->assertFailed();
});

it('shows a run with its actions', function () {
    $run = SagaFlow::create(TwoStepWorkflow::class)->withArguments('order-1')->runSync();

    $this->artisan('saga-flow:show', ['run' => $run->id])
        ->expectsOutputToContain($run->id)
        ->assertSuccessful();

    $this->artisan('saga-flow:show', ['run' => $run->id, '--compact' => true])
        ->assertSuccessful();
});

it('errors when showing a missing run', function () {
    $this->artisan('saga-flow:show', ['run' => '01JMISSINGMISSINGMISSING'])
        ->assertFailed();
});

it('cancels a non-terminal run', function () {
    $run = cliPendingRun();

    $this->artisan('saga-flow:cancel', ['run' => $run->id])->assertSuccessful();

    expect($run->fresh()->status)->toBe(FlowStatus::Cancelled);
});

it('shows the steps of a cancelled run as settled', function () {
    $run = SagaFlow::create(SignalOnlyWorkflow::class)->runSync();

    ActionRun::create([
        'flow_run_id' => $run->id,
        'sequence' => 1,
        'action_class' => 'App\\Actions\\Ship',
        'status' => ActionStatus::Pending,
        'attempts' => 0,
    ]);

    $this->artisan('saga-flow:cancel', ['run' => $run->id])->assertSuccessful();

    Artisan::call('saga-flow:show', ['run' => $run->id]);

    // Matched on the table rows, not the whole output: the event log below them still
    // says flow.waiting, which is history and stays true.
    expect(Artisan::output())
        ->toMatch('/\|\s+1\s+\|\s+cancelled\s+\|/')
        ->toMatch('/\|\s+go\s+\|\s+cancelled\s+\|/');
});

it('warns when cancelling a terminal run', function () {
    $run = cliPendingRun();
    $run->markCancelled();

    $this->artisan('saga-flow:cancel', ['run' => $run->id])->assertSuccessful();
});

it('compensates and cancels a run with --compensate', function () {
    CompensationLog::reset();

    $run = SagaFlow::create(ManualCompensateWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Waiting);

    $this->artisan('saga-flow:cancel', ['run' => $run->id, '--compensate' => true])
        ->assertSuccessful();

    expect($run->fresh()->status)->toBe(FlowStatus::Cancelled)
        ->and(CompensationLog::all())->toContain('undo:a');
});

it('delivers a signal to a waiting run', function () {
    $run = SagaFlow::create(SignalOnlyWorkflow::class)->runSync();

    expect($run->status)->toBe(FlowStatus::Waiting);

    $this->artisan('saga-flow:signal', ['run' => $run->id, 'name' => 'go'])
        ->assertSuccessful();

    expect($run->fresh()->status)->toBe(FlowStatus::Completed);
});

it('delivers a retry signal to a parked run without naming it', function () {
    FlakyPaymentAction::reset(failures: 1);

    $run = SagaFlow::create(NamedRetryWorkflow::class)->withArguments('balance-refilled')->runSync();

    expect($run->status)->toBe(FlowStatus::Waiting);

    $this->artisan('saga-flow:signal-retry', ['run' => $run->id, '--payload' => '{"by":"operator"}'])
        ->expectsOutputToContain('Retry signal delivered')
        ->assertSuccessful();

    expect($run->fresh()->status)->toBe(FlowStatus::Completed)
        ->and(FlowSignal::query()->where('flow_run_id', $run->id)->firstOrFail()->payload)->toBe(['by' => 'operator']);
});

it('delivers the retry signal given with --signal', function () {
    $run = SagaFlow::create(SignalOnlyWorkflow::class)->runSync();

    $this->artisan('saga-flow:signal-retry', ['run' => $run->id, '--signal' => 'go'])
        ->expectsOutputToContain('Signal [go] delivered')
        ->assertSuccessful();

    expect($run->fresh()->status)->toBe(FlowStatus::Completed);
});

it('warns when the run has nothing parked on a retry', function () {
    $run = SagaFlow::create(SignalOnlyWorkflow::class)->runSync();

    $this->artisan('saga-flow:signal-retry', ['run' => $run->id])
        ->expectsOutputToContain('no step or child waiting on a retry signal')
        ->assertSuccessful();

    expect($run->fresh()->status)->toBe(FlowStatus::Waiting)
        ->and(FlowSignal::query()->where('flow_run_id', $run->id)->count())->toBe(1);
});

it('rejects a retry payload that is not a JSON object or array', function () {
    $run = SagaFlow::create(SignalOnlyWorkflow::class)->runSync();

    $this->artisan('saga-flow:signal-retry', ['run' => $run->id, '--signal' => 'go', '--payload' => '"text"'])
        ->expectsOutputToContain('Payload must be a JSON object or array.')
        ->assertFailed();

    expect($run->fresh()->status)->toBe(FlowStatus::Waiting);
});

it('errors when retrying a missing run', function () {
    $this->artisan('saga-flow:signal-retry', ['run' => 'missing'])->assertFailed();
});

it('prunes old terminal runs and their related rows', function () {
    $run = SagaFlow::create(TwoStepWorkflow::class)->withArguments('order-1')->runSync();

    expect($run->status)->toBe(FlowStatus::Completed)
        ->and(ActionRun::query()->where('flow_run_id', $run->id)->count())->toBe(2);

    FlowRun::query()->whereKey($run->id)->update(['created_at' => now()->subDays(10)]);

    // Dry run deletes nothing.
    $this->artisan('saga-flow:prune', ['--days' => '1', '--dry-run' => true])->assertSuccessful();
    expect(FlowRun::query()->whereKey($run->id)->exists())->toBeTrue();

    // Real prune removes the run and its actions.
    $this->artisan('saga-flow:prune', ['--days' => '1'])->assertSuccessful();

    expect(FlowRun::query()->whereKey($run->id)->exists())->toBeFalse()
        ->and(ActionRun::query()->where('flow_run_id', $run->id)->count())->toBe(0);
});
