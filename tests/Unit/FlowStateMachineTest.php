<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\States\FlowStateMachine;

it('allows valid transitions', function (FlowStatus $from, FlowStatus $to) {
    expect((new FlowStateMachine)->canTransition($from, $to))->toBeTrue();
})->with([
    'pending -> running' => [FlowStatus::Pending, FlowStatus::Running],
    'pending -> cancelled' => [FlowStatus::Pending, FlowStatus::Cancelled],
    'pending -> expired' => [FlowStatus::Pending, FlowStatus::Expired],
    'running -> waiting' => [FlowStatus::Running, FlowStatus::Waiting],
    'running -> completed' => [FlowStatus::Running, FlowStatus::Completed],
    'running -> failed' => [FlowStatus::Running, FlowStatus::Failed],
    'running -> cancelling' => [FlowStatus::Running, FlowStatus::Cancelling],
    'waiting -> running' => [FlowStatus::Waiting, FlowStatus::Running],
    'waiting -> completed' => [FlowStatus::Waiting, FlowStatus::Completed],
    'cancelling -> cancelled' => [FlowStatus::Cancelling, FlowStatus::Cancelled],
]);

it('rejects invalid transitions', function (FlowStatus $from, FlowStatus $to) {
    expect((new FlowStateMachine)->canTransition($from, $to))->toBeFalse();
})->with([
    'pending -> completed' => [FlowStatus::Pending, FlowStatus::Completed],
    'pending -> waiting' => [FlowStatus::Pending, FlowStatus::Waiting],
    'completed -> running' => [FlowStatus::Completed, FlowStatus::Running],
    'failed -> running' => [FlowStatus::Failed, FlowStatus::Running],
    'cancelled -> running' => [FlowStatus::Cancelled, FlowStatus::Running],
    'expired -> running' => [FlowStatus::Expired, FlowStatus::Running],
    'cancelling -> running' => [FlowStatus::Cancelling, FlowStatus::Running],
]);

it('treats every terminal status as terminal', function () {
    expect(FlowStatus::Completed->isTerminal())->toBeTrue()
        ->and(FlowStatus::Failed->isTerminal())->toBeTrue()
        ->and(FlowStatus::Cancelled->isTerminal())->toBeTrue()
        ->and(FlowStatus::Expired->isTerminal())->toBeTrue()
        ->and(FlowStatus::Pending->isTerminal())->toBeFalse()
        ->and(FlowStatus::Running->isTerminal())->toBeFalse()
        ->and(FlowStatus::Waiting->isTerminal())->toBeFalse();
});
