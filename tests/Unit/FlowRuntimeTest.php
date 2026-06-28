<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
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
