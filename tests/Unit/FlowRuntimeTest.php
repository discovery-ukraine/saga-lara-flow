<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ActionFailedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\MissingFlowContextException;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowRuntime;

it('throws when no run is bound', function () {
    (new FlowRuntime)->run();
})->throws(MissingFlowContextException::class);

it('does not leak sequence state between runs in the same process', function () {
    $runtime = new FlowRuntime;

    $runtime->bind(new FlowRun(['id' => 'a']), RunMode::Sync);
    $runtime->nextSequence();
    $runtime->nextSequence();
    $runtime->clear();

    $runtime->bind(new FlowRun(['id' => 'b']), RunMode::Queued);

    expect($runtime->nextSequence())->toBe(0)
        ->and($runtime->mode())->toBe(RunMode::Queued)
        ->and($runtime->run()->id)->toBe('b');
});

it('rewinds the sequence on reset for replay', function () {
    $runtime = new FlowRuntime;
    $runtime->bind(new FlowRun(['id' => 'a']), RunMode::Sync);

    $runtime->nextSequence();
    $runtime->reset();

    expect($runtime->nextSequence())->toBe(0);
});

it('answers for the throws it raised and no other', function () {
    $runtime = new FlowRuntime;

    $raised = $runtime->raising(ActionFailedException::forAction('App\\Actions\\Charge', 1, 'declined'));
    $twin = ActionFailedException::forAction('App\\Actions\\Charge', 1, 'declined');

    expect($runtime->raised($raised))->toBeTrue()
        ->and($runtime->raised($twin))->toBeFalse();
});

it('forgets what an earlier pass raised on reset', function () {
    $runtime = new FlowRuntime;
    $runtime->bind(new FlowRun(['id' => 'a']), RunMode::Sync);

    $raised = $runtime->raising(ActionFailedException::forAction('App\\Actions\\Charge', 1, 'declined'));

    $runtime->reset();

    expect($runtime->raised($raised))->toBeFalse();
});

it('does not hold a throw a workflow caught and dropped', function () {
    $runtime = new FlowRuntime;

    $ending = $runtime->raising(ActionFailedException::forAction('App\\Actions\\Charge', 1, 'declined'));
    $weak = WeakReference::create($ending);

    unset($ending);
    gc_collect_cycles();

    expect($weak->get())->toBeNull();
});
