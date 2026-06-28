<?php

use DiscoveryUkraine\SagaLaraFlow\Exceptions\HistoryContractMismatchException;

it('builds a clear message for an action class mismatch', function () {
    $exception = HistoryContractMismatchException::forActionClass(
        2,
        'App\\FlowActions\\ChargeCustomer',
        'App\\FlowActions\\SendEmail',
        '01J000000000000000000RUNID',
    );

    expect($exception->getMessage())
        ->toContain('Sequence 2')
        ->toContain('App\\FlowActions\\ChargeCustomer')
        ->toContain('App\\FlowActions\\SendEmail')
        ->toContain('01J000000000000000000RUNID')
        ->toContain('Version long-running workflows');
});

it('builds a clear message for a signal name mismatch', function () {
    $exception = HistoryContractMismatchException::forSignalName(
        1,
        'manager.approved',
        'finance.approved',
        '01J000000000000000000RUNID',
    );

    expect($exception->getMessage())
        ->toContain('Sequence 1')
        ->toContain("signal 'manager.approved'")
        ->toContain("signal 'finance.approved'")
        ->toContain('01J000000000000000000RUNID')
        ->toContain('Version long-running workflows');
});

it('builds a clear message for an operation type mismatch', function () {
    $exception = HistoryContractMismatchException::forOperationType(
        3,
        'side effect',
        'action App\\FlowActions\\ChargeCustomer',
        '01J000000000000000000RUNID',
    );

    expect($exception->getMessage())
        ->toContain('Sequence 3')
        ->toContain('side effect')
        ->toContain('action App\\FlowActions\\ChargeCustomer')
        ->toContain('01J000000000000000000RUNID');
});
