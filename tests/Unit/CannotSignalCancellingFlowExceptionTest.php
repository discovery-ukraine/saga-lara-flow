<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CannotSignalCancellingFlowException;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

it('builds a clear message naming the run and the rollback it is in', function () {
    $run = new FlowRun;
    $run->id = '01J000000000000000000RUNID';
    $run->status = FlowStatus::Cancelling;

    $exception = CannotSignalCancellingFlowException::for($run);

    expect($exception->getMessage())
        ->toContain('01J000000000000000000RUNID')
        ->toContain('cancelling')
        ->toContain('cannot be signalled');
});
