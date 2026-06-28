<?php

use DiscoveryUkraine\SagaLaraFlow\Runtime\StepSequence;

it('hands out monotonic ordinals starting at zero', function () {
    $sequence = new StepSequence;

    expect($sequence->next())->toBe(0)
        ->and($sequence->next())->toBe(1)
        ->and($sequence->next())->toBe(2)
        ->and($sequence->current())->toBe(3);
});

it('rewinds to zero on reset', function () {
    $sequence = new StepSequence;

    $sequence->next();
    $sequence->next();
    $sequence->reset();

    expect($sequence->next())->toBe(0);
});
