<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Jobs\ResumeWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Jobs\RunWorkflowJob;
use DiscoveryUkraine\SagaLaraFlow\Models\CompensationRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowDoctor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowMonitor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompensationLog;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\UndoAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\WriterRoutedFlowRun;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Illuminate\Support\Facades\DB;

/**
 * A rollback plans the stack it will undo once, so a run must be driven once. A job
 * queued while the run could still start work — a resume owed to a wait the sweep then
 * expired — arrives to find it rolling back, and a pass entered there plans a second
 * rollback and runs every compensation on it twice.
 *
 * The three statuses mayStartWork() names are therefore what a pass begins on, weighed
 * before the deadline and read from the writer.
 */
beforeEach(function (): void {
    CompensationLog::reset();
    WriterRoutedFlowRun::reset();
});

final class StaleResumeWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();
        $this->awaitSignal('go');
    }
}

it('runs a compensation once when a resume queued before the sweep arrives after it', function (): void {
    useDatabaseQueue();

    $run = SagaFlow::create(StaleResumeWorkflow::class)->expiresAt(now()->addSeconds(30))->run();

    drainQueue();

    expect(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Waiting);

    // The run is still Waiting, so the delivery is legitimate and queues a resume. The
    // deadline passes while that job waits its turn behind the sweep.
    $this->travel(60)->seconds();

    SagaFlow::loadFlow($run->id)->signal('go');

    expect(app(FlowMonitor::class)->sweep()['runs'])->toBe(1);

    expect(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Cancelling);

    drainQueue();

    expect(CompensationLog::all())->toBe(['undo:a'])
        ->and(CompensationRun::query()->count())->toBe(1)
        ->and(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Expired);
});

it('decides on the status the writer holds, not the one the caller carries', function (): void {
    config()->set('saga-lara-flow.models.flow_run', WriterRoutedFlowRun::class);

    useDatabaseQueue();

    $run = SagaFlow::create(StaleResumeWorkflow::class)->expiresAt(now()->addSeconds(30))->run();

    drainQueue();

    // Read while the run may still start work; the sweep moves it underneath.
    $stale = WriterRoutedFlowRun::query()->findOrFail($run->id);

    $this->travel(60)->seconds();
    app(FlowMonitor::class)->sweep();

    expect($stale->status)->toBe(FlowStatus::Waiting);

    WriterRoutedFlowRun::reset();

    $driven = app(FlowExecutor::class)->drive($stale, RunMode::Queued);

    // The caller is handed the run the writer holds rather than having the instance it
    // still owns refreshed underneath it, and nothing was planned a second time.
    expect($driven->status)->toBe(FlowStatus::Cancelling)
        ->and($stale->status)->toBe(FlowStatus::Waiting)
        ->and(WriterRoutedFlowRun::$writerReads)->toBe(1)
        ->and(CompensationRun::query()->count())->toBe(1);
});

it('leaves a rolling-back run where a kick found it', function (): void {
    config()->set('saga-lara-flow.models.flow_run', WriterRoutedFlowRun::class);

    useDatabaseQueue();

    $run = SagaFlow::create(StaleResumeWorkflow::class)->expiresAt(now()->addSeconds(30))->run();

    drainQueue();

    $this->travel(60)->seconds();
    app(FlowMonitor::class)->sweep();

    $rollingBack = WriterRoutedFlowRun::query()->findOrFail($run->id);

    expect($rollingBack->status)->toBe(FlowStatus::Cancelling);

    $events = DB::connection('testing')->table('saga_flow_events')->count();
    $jobs = DB::connection('testing')->table('jobs')->count();

    // A kick decides on the writer, so a caller holding a snapshot from before the sweep
    // is not what tells it the run is still worth re-driving.
    $rollingBack->status = FlowStatus::Waiting;

    WriterRoutedFlowRun::reset();

    expect(app(FlowDoctor::class)->kick($rollingBack)->status)->toBe(FlowStatus::Cancelling)
        ->and(WriterRoutedFlowRun::$writerReads)->toBe(1)
        ->and(DB::connection('testing')->table('saga_flow_events')->count())->toBe($events)
        ->and(DB::connection('testing')->table('jobs')->count())->toBe($jobs);

    $this->artisan('saga-flow:kick', ['run' => $run->id])
        ->expectsOutputToContain("Flow run [{$run->id}] is cancelling; nothing to re-drive.")
        ->assertSuccessful();
});

it('turns a stale job away before the pass is set up', function (string $job): void {
    config()->set('saga-lara-flow.models.flow_run', WriterRoutedFlowRun::class);

    useDatabaseQueue();

    $run = SagaFlow::create(StaleResumeWorkflow::class)->expiresAt(now()->addSeconds(30))->run();

    drainQueue();

    $this->travel(60)->seconds();
    app(FlowMonitor::class)->sweep();

    $jobs = DB::connection('testing')->table('jobs')->count();

    WriterRoutedFlowRun::reset();

    app()->call([new $job($run->id), 'handle']);

    // The job answers from its own read: no pass is entered, so the executor never
    // reaches the writer, and nothing new is queued.
    expect(WriterRoutedFlowRun::$writerReads)->toBe(0)
        ->and(DB::connection('testing')->table('jobs')->count())->toBe($jobs);
})->with([ResumeWorkflowJob::class, RunWorkflowJob::class]);
