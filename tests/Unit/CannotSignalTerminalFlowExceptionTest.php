<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CannotSignalTerminalFlowException;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

it('builds a clear message naming the run and its terminal status', function () {
    $run = new FlowRun;
    $run->id = '01J000000000000000000RUNID';
    $run->status = FlowStatus::Completed;

    $exception = CannotSignalTerminalFlowException::for($run);

    expect($exception->getMessage())
        ->toContain('01J000000000000000000RUNID')
        ->toContain('completed')
        ->toContain('cannot be signalled');
});
