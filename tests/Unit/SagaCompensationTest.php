<?php

use DiscoveryUkraine\SagaLaraFlow\Data\CompensationDefinition;
use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationFailurePolicy;
use DiscoveryUkraine\SagaLaraFlow\Runtime\CompensationEntry;
use DiscoveryUkraine\SagaLaraFlow\Runtime\SagaStack;
use DiscoveryUkraine\SagaLaraFlow\Tests\Fixtures\UndoAction;

function makeEntry(
    ?CompensationFailurePolicy $action = null,
    ?CompensationFailurePolicy $group = null
): CompensationEntry {
    return new CompensationEntry(
        actionRunId: 'ar',
        sequence: 0,
        definition: CompensationDefinition::forClass(UndoAction::class, ['x']),
        actionCompensationFailurePolicy: $action,
        groupCompensationFailurePolicy: $group,
    );
}

it('keeps entries in push order and rewinds on reset', function () {
    $stack = new SagaStack;

    expect($stack->isEmpty())->toBeTrue();

    $stack->push(makeEntry());
    $stack->push(makeEntry());

    expect($stack->entries())->toHaveCount(2)
        ->and($stack->isEmpty())->toBeFalse();

    $stack->reset();

    expect($stack->isEmpty())->toBeTrue()
        ->and($stack->entries())->toBe([]);
});

it('builds a class compensation definition', function () {
    $definition = CompensationDefinition::forClass(UndoAction::class, ['x']);

    expect($definition->type)->toBe('class')
        ->and($definition->class)->toBe(UndoAction::class)
        ->and($definition->arguments)->toBe(['x'])
        ->and($definition->closure)->toBeNull()
        ->and($definition->isClosure())->toBeFalse();
});

it('builds a closure compensation definition', function () {
    $definition = CompensationDefinition::forClosure(fn () => 'undone');

    expect($definition->type)->toBe('closure')
        ->and($definition->class)->toBeNull()
        ->and($definition->isClosure())->toBeTrue()
        ->and($definition->closure)->not->toBeNull();
});

it('resolves compensation policy with precedence action > group > config', function () {
    config()->set('saga-lara-flow.sagas.default_compensation_failure_policy', CompensationFailurePolicy::Stop);

    expect(makeEntry(action: CompensationFailurePolicy::Continue,
        group: CompensationFailurePolicy::Stop)->effectivePolicy())
        ->toBe(CompensationFailurePolicy::Continue)
        ->and(makeEntry(group: CompensationFailurePolicy::Continue)->effectivePolicy())
        ->toBe(CompensationFailurePolicy::Continue)
        ->and(makeEntry()->effectivePolicy())
        ->toBe(CompensationFailurePolicy::Stop);
});
