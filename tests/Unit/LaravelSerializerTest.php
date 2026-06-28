<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Serialization\LaravelSerializer;

it('round-trips scalars and nested arrays', function () {
    $serializer = new LaravelSerializer;

    expect($serializer->serialize('x'))->toBe('x')
        ->and($serializer->serialize(42))->toBe(42)
        ->and($serializer->serialize(null))->toBeNull();

    $nested = ['a' => 1, 'b' => ['c' => 'd']];

    expect($serializer->deserialize($serializer->serialize($nested)))->toBe($nested);
});

it('stores an Eloquent model as a reference and rehydrates it', function () {
    $serializer = new LaravelSerializer;

    $run = new FlowRun;
    $run->fill(['workflow_class' => 'X', 'status' => FlowStatus::Pending]);
    $run->save();

    $reference = $serializer->serialize($run);

    expect($reference)->toMatchArray([
        '_model' => FlowRun::class,
        'id' => $run->id,
    ]);

    $restored = $serializer->deserialize($reference);

    expect($restored)->toBeInstanceOf(FlowRun::class)
        ->and($restored->id)->toBe($run->id);
});
