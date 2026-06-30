<?php

use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowDoctor;

// Reuses stageStuckPendingAction() from DoctorRedispatchActionTest.php (Pest loads
// every test file, so the top-level helper is available across the suite).

it('grows the backoff window across repair attempts', function () {
    $action = stageStuckPendingAction();
    $doctor = app(FlowDoctor::class);

    expect($doctor->repair()->redispatchedActions)->toBe(1)
        ->and($action->fresh()->repair_attempts)->toBe(1);

    // Within the window: throttled.
    expect($doctor->repair()->redispatchedActions)->toBe(0)
        ->and($action->fresh()->repair_attempts)->toBe(1);

    // Force the window open: the next attempt fires and pushes the window out further.
    ActionRun::query()->whereKey($action->id)->update(['repair_available_at' => now()->subSecond()]);

    expect($doctor->repair()->redispatchedActions)->toBe(1);

    $action->refresh();

    // attempts=2 → backoff base(10) * 2^1 = 20s.
    $seconds = now()->diffInSeconds($action->repair_available_at);

    expect($action->repair_attempts)->toBe(2)
        ->and($action->repair_available_at->isFuture())->toBeTrue()
        ->and($seconds)->toBeGreaterThan(14)
        ->and($seconds)->toBeLessThan(26);
});

it('stops repairing an entity once max_attempts is reached', function () {
    $action = stageStuckPendingAction();

    ActionRun::query()->whereKey($action->id)->update([
        'repair_attempts' => (int) config('saga-lara-flow.repair.max_attempts'),
        'repair_available_at' => null,
    ]);

    expect(app(FlowDoctor::class)->repair()->redispatchedActions)->toBe(0)
        ->and($action->fresh()->repair_attempts)
        ->toBe((int) config('saga-lara-flow.repair.max_attempts'));
});
