<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Events\ChildWorkflowStarted;
use DiscoveryUkraine\SagaLaraFlow\Facades\SagaFlow;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowChild;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowEvent;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Models\SideEffect;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowExecutor;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\CompetingRollback;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\EchoValueChildWorkflow;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\MakeValueAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\SideEffectCounter;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\UndoAction;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\UnwritableFlowChild;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\WriterRoutedFlowRun;
use DiscoveryUkraine\SagaLaraFlow\Workflow;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Event;

/**
 * A pass already replaying when a rollback commits keeps going, and every ordinal it
 * reaches for the first time starts work of its own: a child workflow that runs to
 * completion outside the plan, and a side-effect factory — host code — called for
 * real. Neither is a step, so the claim that holds a step to mayStartWork() never sees
 * them.
 *
 * The rollback is committed from inside a side-effect factory, host code the engine
 * calls at a legitimate seam, so one process reaches the state two would.
 */
beforeEach(function (): void {
    SideEffectCounter::reset();
    WriterRoutedFlowRun::reset();
});

final class RollbackDuringSideEffectWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();

        $this->sideEffect('one', function (): string {
            CompetingRollback::commit($this->runtime->run()->id);

            return 'first';
        });

        $this->sideEffect('two', function (): string {
            SideEffectCounter::$count++;

            return 'second';
        });
    }
}

final class RollbackDuringChildWorkflow extends Workflow
{
    public function handle(): void
    {
        $this->action(MakeValueAction::class, 'a')->compensateWith(UndoAction::class, 'a')->run();

        $this->sideEffect('one', function (): string {
            CompetingRollback::commit($this->runtime->run()->id);

            return 'first';
        });

        $this->child(EchoValueChildWorkflow::class, ['done'])->run();
    }
}

final class OneSideEffectWorkflow extends Workflow
{
    public function handle(): string
    {
        return $this->sideEffect('one', fn (): string => 'value');
    }
}

final class OneChildWorkflow extends Workflow
{
    public function handle(): mixed
    {
        return $this->child(EchoValueChildWorkflow::class, ['done'])->run();
    }
}

it('leaves a side-effect factory uncalled once the run is rolling back', function (): void {
    useDatabaseQueue();

    $run = SagaFlow::create(RollbackDuringSideEffectWorkflow::class)->run();

    drainQueue();

    // The first factory ran while the run could still start work, so its value is
    // history. The second is reached afterwards, and a factory is host code: an HTTP
    // call, a charge, an id handed out.
    expect(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Cancelling)
        ->and(SideEffectCounter::$count)->toBe(0)
        ->and(SideEffect::query()->count())->toBe(1);
});

it('starts no child once the run is rolling back', function (): void {
    useDatabaseQueue();

    $run = SagaFlow::create(RollbackDuringChildWorkflow::class)->run();

    drainQueue();

    // Neither the link nor a run of its own: a child started here would finish under a
    // parent reporting a complete unwind, and the default Abandon policy closes nothing.
    expect(FlowRun::query()->findOrFail($run->id)->status)->toBe(FlowStatus::Cancelling)
        ->and(FlowChild::query()->count())->toBe(0)
        ->and(FlowRun::query()->count())->toBe(1);
});

it('leaves no child run behind when the link cannot be written', function (): void {
    config()->set('saga-lara-flow.models.flow_child', UnwritableFlowChild::class);

    $run = SagaFlow::create(OneChildWorkflow::class)->runSync();

    // The child and its link are one write or neither, so a run nobody owns is not
    // what a half-written ordinal leaves behind.
    expect($run->status)->toBe(FlowStatus::Failed)
        ->and(FlowRun::query()->count())->toBe(1);
});

it('asks the connection that wrote the run whether work may begin', function (string $workflow): void {
    config()->set('saga-lara-flow.models.flow_run', WriterRoutedFlowRun::class);

    useDatabaseQueue();

    $run = SagaFlow::create($workflow)->run();

    WriterRoutedFlowRun::reset();

    app(FlowExecutor::class)->drive(WriterRoutedFlowRun::query()->findOrFail($run->id), RunMode::Queued);

    // Two reads for the pass: the boundary it begins on, and the seam's own before it
    // starts the one thing this run has to start.
    expect(WriterRoutedFlowRun::$writerReads)->toBe(2);
})->with([[OneSideEffectWorkflow::class], [OneChildWorkflow::class]]);

it('keeps a child whose announcement a listener threw over', function (): void {
    Event::listen(ChildWorkflowStarted::class, function (): void {
        throw new RuntimeException('a listener with an opinion');
    });

    $run = SagaFlow::create(OneChildWorkflow::class)->runSync();

    // The host is told after the transaction, not inside it, so a listener cannot undo
    // rows that are already on record — nor start work of its own against a child that
    // is about to stop existing.
    expect($run->status)->toBe(FlowStatus::Failed)
        ->and(FlowChild::query()->count())->toBe(1)
        ->and(FlowRun::query()->count())->toBe(2);
});

it('never announces a child without its link, poisoned through a model observer', function (): void {
    // No listener anywhere: the engine's own child.started insert carries the failure in,
    // and PostgreSQL turns the COMMIT that reports success into a rollback.
    FlowEvent::created(function (): void {
        try {
            DB::connection('testing')->statement('insert into no_such_table (id) values (1)');
        } catch (Throwable) {
            // An observer that eats its own failure — broken, but nothing stops it.
        }
    });

    $announced = 0;

    Event::listen(ChildWorkflowStarted::class, function () use (&$announced): void {
        $announced++;
    });

    SagaFlow::create(OneChildWorkflow::class)->runSync();

    // Whether the write held is the driver's business; that the announcement agrees with
    // it is not.
    expect($announced)->toBe(FlowChild::query()->count());
});
