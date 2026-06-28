<?php

namespace DiscoveryUkraine\SagaLaraFlow\Concerns;

use Illuminate\Contracts\Container\BindingResolutionException;
use ReflectionException;
use ReflectionMethod;
use ReflectionNamedType;

/**
 * Invokes a handle() method with native Laravel dependency injection: class-typed
 * parameters are resolved from the container, while remaining parameters are
 * filled positionally from the stored workflow/action arguments.
 *
 * Per the determinism contract, prefer passing IDs over models as arguments;
 * class-typed parameters are always container-resolved.
 */
trait ResolvesMethodDependencies
{
    /**
     * @param  array<int, mixed>  $arguments
     *
     * @throws ReflectionException
     * @throws BindingResolutionException
     */
    protected function callWithDependencies(object $instance, string $method, array $arguments): mixed
    {
        $resolved = $this->resolveMethodDependencies($instance, $method, $arguments);

        return $instance->{$method}(...$resolved);
    }

    /**
     * @param  array<int, mixed>  $arguments
     * @return array<int, mixed>
     *
     * @throws ReflectionException
     * @throws BindingResolutionException
     */
    private function resolveMethodDependencies(object $instance, string $method, array $arguments): array
    {
        $reflection = new ReflectionMethod($instance, $method);

        $positional = array_values($arguments);

        $resolved = [];

        foreach ($reflection->getParameters() as $parameter) {
            $type = $parameter->getType();

            if ($type instanceof ReflectionNamedType && ! $type->isBuiltin()) {
                $resolved[] = app()->make($type->getName());

                continue;
            }

            if ($positional !== []) {
                $resolved[] = array_shift($positional);

                continue;
            }

            if ($parameter->isDefaultValueAvailable()) {
                $resolved[] = $parameter->getDefaultValue();

                continue;
            }

            if ($parameter->getType()->allowsNull()) {
                $resolved[] = null;
            }
        }

        return $resolved;
    }
}
