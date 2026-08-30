<?php

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Events\ActionOutcomeRejected;
use DiscoveryUkraine\SagaLaraFlow\Events\CompensationOutcomeRejected;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\CompensationRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ActionRecorder;
use DiscoveryUkraine\SagaLaraFlow\Runtime\CompensationRecorder;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ClosureBackedResult;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\OneActionWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\QueuedRejectionListener;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\ThrowingRejectionListener;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\UndoAction;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;

/**
 * A refused outcome is the one case where the engine holds work that happened and
 * has nowhere to put it: the fence is right about the row, and the value the step
 * produced exists only in the worker's memory. The anomaly line names the loss but
 * cannot carry the payload — it goes to the application's default channel, chosen
 * for neither business data nor its retention. These events hand it to the host
 * instead, which is the only party that can decide where it belongs.
 */
function rejectedOutcomeRun(): FlowRun
{
    return app(FlowRepository::class)->create([
        'workflow_class' => OneActionWorkflow::class,
        'status' => FlowStatus::Waiting,
        'arguments' => [],
    ]);
}

/**
 * The row as reclaim leaves it: claimed by a first worker, then claimed again by a
 * second once the deadline passed. The first worker is still alive and about to
 * finish — the state the outcome fence exists for.
 */
function supersededAction(FlowRun $run): ActionRun
{
    $recorder = app(ActionRecorder::class);

    $workerA = ActionRun::create([
        'flow_run_id' => $run->id,
        'sequence' => 0,
        'action_class' => MakeValueAction::class,
        'status' => ActionStatus::Pending,
        'reclaim_stale_after_seconds' => 900,
        'attempts' => 0,
    ]);

    expect($recorder->startAction($workerA))->toBeTrue();

    ActionRun::query()->whereKey($workerA->id)->update(['reclaim_stale_at' => now()->subMinute()]);

    $workerB = ActionRun::query()->findOrFail($workerA->id);

    expect($recorder->startAction($workerB))->toBeTrue()
        ->and($workerB->attempts)->toBe(2);

    return $workerA;
}

/**
 * The compensation mirror of supersededAction(): reclaim has handed the row to a
 * second worker while the first is still running.
 */
function supersededCompensation(FlowRun $run): CompensationRun
{
    $recorder = app(CompensationRecorder::class);

    $workerA = CompensationRun::create([
        'flow_run_id' => $run->id,
        'sequence' => 0,
        'compensation_type' => 'class',
        'compensation_class' => UndoAction::class,
        'status' => CompensationStatus::Pending,
        'reclaim_stale_after_seconds' => 900,
        'attempts' => 0,
    ]);

    expect($recorder->startCompensation($workerA))->toBeTrue();

    CompensationRun::query()->whereKey($workerA->id)->update(['reclaim_stale_at' => now()->subMinute()]);

    $workerB = CompensationRun::query()->findOrFail($workerA->id);

    expect($recorder->startCompensation($workerB))->toBeTrue()
        ->and($workerB->attempts)->toBe(2);

    return $workerA;
}

/**
 * Both rejection events into one collector. It is an ArrayObject rather than an
 * array so the caller reads what the listeners appended: an array returned from
 * here would be a copy taken before either of them ever ran, and every assertion
 * against it would hold no matter what the engine dispatched.
 *
 * @return ArrayObject<int, ActionOutcomeRejected|CompensationOutcomeRejected>
 */
function captureRejections(): ArrayObject
{
    /** @var ArrayObject<int, ActionOutcomeRejected|CompensationOutcomeRejected> $captured */
    $captured = new ArrayObject;

    Event::listen(ActionOutcomeRejected::class, function ($event) use ($captured): void {
        $captured[] = $event;
    });

    Event::listen(CompensationOutcomeRejected::class, function ($event) use ($captured): void {
        $captured[] = $event;
    });

    return $captured;
}

it('hands the host the result an action produced and could not record', function () {
    $captured = [];
    Event::listen(ActionOutcomeRejected::class, function (ActionOutcomeRejected $event) use (&$captured): void {
        $captured[] = $event;
    });

    $workerA = supersededAction(rejectedOutcomeRun());

    expect(app(ActionRecorder::class)->completeAction($workerA, ['charge_id' => 'ch_9f3']))->toBeFalse()
        // The loss is real: the row keeps nothing of what this worker produced.
        ->and($workerA->fresh()->result)->toBeNull();

    expect($captured)->toHaveCount(1);

    expect($captured[0]->outcome)->toBe(FlowEventType::ActionCompleted)
        ->and($captured[0]->result)->toBe(['charge_id' => 'ch_9f3'])
        ->and($captured[0]->exception)->toBeNull()
        ->and($captured[0]->actionRun->id)->toBe($workerA->id)
        ->and($captured[0]->actionRun->sequence)->toBe(0)
        ->and($captured[0]->actionRun->action_class)->toBe(MakeValueAction::class);
});

it('hands the host the throw an action produced, which is not rethrown either', function () {
    $captured = [];
    Event::listen(ActionOutcomeRejected::class, function (ActionOutcomeRejected $event) use (&$captured): void {
        $captured[] = $event;
    });

    $workerA = supersededAction(rejectedOutcomeRun());
    $thrown = new RuntimeException('gateway said no');

    expect(app(ActionRecorder::class)->failAction($workerA, $thrown))->toBeFalse()
        ->and($workerA->fresh()->exception)->toBeNull();

    expect($captured)->toHaveCount(1);

    // ActionDispatcher deliberately swallows this throw rather than failing a job whose
    // work was already discarded, so the event is the only place it is ever named.
    expect($captured[0]->outcome)->toBe(FlowEventType::ActionFailed)
        ->and($captured[0]->exception)->toBe($thrown)
        ->and($captured[0]->result)->toBeNull();
});

it('hands the host the result a compensation produced and could not record', function () {
    $captured = [];
    Event::listen(CompensationOutcomeRejected::class, function (CompensationOutcomeRejected $event) use (&$captured): void {
        $captured[] = $event;
    });

    $workerA = supersededCompensation(rejectedOutcomeRun());

    expect(app(CompensationRecorder::class)->completeCompensation($workerA, ['refund_id' => 're_77']))->toBeFalse()
        ->and($workerA->fresh()->result)->toBeNull();

    expect($captured)->toHaveCount(1);

    expect($captured[0]->outcome)->toBe(FlowEventType::CompensationCompleted)
        ->and($captured[0]->result)->toBe(['refund_id' => 're_77'])
        ->and($captured[0]->exception)->toBeNull()
        ->and($captured[0]->compensationRun->id)->toBe($workerA->id);
});

it('hands the host the throw a compensation produced and could not record', function () {
    $captured = [];
    Event::listen(CompensationOutcomeRejected::class, function (CompensationOutcomeRejected $event) use (&$captured): void {
        $captured[] = $event;
    });

    $workerA = supersededCompensation(rejectedOutcomeRun());
    $thrown = new RuntimeException('refund gateway said no');

    expect(app(CompensationRecorder::class)->failCompensation($workerA, $thrown))->toBeFalse()
        ->and($workerA->fresh()->exception)->toBeNull();

    expect($captured)->toHaveCount(1);

    expect($captured[0]->outcome)->toBe(FlowEventType::CompensationFailed)
        ->and($captured[0]->exception)->toBe($thrown)
        ->and($captured[0]->result)->toBeNull();
});

it('hands the listener a model reading as the row, not as the refused outcome', function () {
    $captured = [];
    Event::listen(ActionOutcomeRejected::class, function (ActionOutcomeRejected $event) use (&$captured): void {
        $captured[] = [
            'status' => $event->actionRun->status,
            'result' => $event->actionRun->result,
            'dirty' => array_keys($event->actionRun->getDirty()),
        ];
    });

    $workerA = supersededAction(rejectedOutcomeRun());

    app(ActionRecorder::class)->completeAction($workerA, ['charge_id' => 'ch_9f3']);

    // A listener that saved this model would write the refused outcome back with no
    // fence under it. The payload travels in the event, never on the model.
    expect($captured[0]['status'])->toBe(ActionStatus::Running)
        ->and($captured[0]['result'])->toBeNull()
        ->and($captured[0]['dirty'])->toBe([]);
});

it('says nothing when the outcome is recorded', function () {
    $captured = captureRejections();

    $run = rejectedOutcomeRun();
    $recorder = app(ActionRecorder::class);

    $action = ActionRun::create([
        'flow_run_id' => $run->id,
        'sequence' => 0,
        'action_class' => MakeValueAction::class,
        'status' => ActionStatus::Pending,
        'attempts' => 0,
    ]);

    expect($recorder->startAction($action))->toBeTrue()
        ->and($recorder->completeAction($action, ['charge_id' => 'ch_9f3']))->toBeTrue()
        ->and($action->fresh()->result)->toBe(['charge_id' => 'ch_9f3'])
        ->and($captured->getArrayCopy())->toBe([]);
});

it('says nothing for a refused expiry, which discards nothing a worker produced', function () {
    $captured = captureRejections();

    $run = rejectedOutcomeRun();

    $action = ActionRun::create([
        'flow_run_id' => $run->id,
        'sequence' => 0,
        'action_class' => MakeValueAction::class,
        'status' => ActionStatus::Completed,
        'attempts' => 1,
    ]);

    // expireAction shares the anomaly reason but not the loss: the exception it writes
    // is the monitor's own account of the deadline, not a value only this worker holds.
    expect(app(ActionRecorder::class)->expireAction($action, ['class' => 'X', 'message' => 'overdue']))
        ->toBeFalse()
        ->and($captured->getArrayCopy())->toBe([]);
});

it('carries a payload the recommended queued listener can actually be given', function () {
    useDatabaseQueue();

    Event::listen(ActionOutcomeRejected::class, QueuedRejectionListener::class);

    $captured = [];
    Event::listen(ActionOutcomeRejected::class, function (ActionOutcomeRejected $event) use (&$captured): void {
        $captured[] = $event->result;
    });

    $workerA = supersededAction(rejectedOutcomeRun());

    // Laravel serialises the listener job with the event inside it. Handing over the
    // value the action returned would put a closure under PHP's serialize() and throw
    // out of a branch whose whole point is not to; the recorded form cannot.
    expect(app(ActionRecorder::class)->completeAction($workerA, new ClosureBackedResult))->toBeFalse()
        ->and(DB::table('jobs')->count())->toBe(1)
        ->and($captured)->toBe([['charge_id' => 'ch_9f3']]);
});

it('journals a listener that throws instead of failing the job over it', function () {
    $path = sys_get_temp_dir().'/saga-rejection-'.uniqid().'.log';
    logToFile($path);

    Event::listen(ActionOutcomeRejected::class, ThrowingRejectionListener::class);

    $workerA = supersededAction(rejectedOutcomeRun());

    // Letting this throw would fail the job, and RunActionJob::failed() then writes
    // queue bookkeeping into the row the second worker now owns — measured: the
    // exhausted-attempts fence passes, because both claims read Running.
    expect(app(ActionRecorder::class)->completeAction($workerA, ['charge_id' => 'ch_9f3']))->toBeFalse()
        ->and(file_get_contents($path))->toContain('rejection_undelivered')
        ->and(file_get_contents($path))->toContain('listener blew up');
});
