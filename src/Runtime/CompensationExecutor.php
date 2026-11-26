<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Concerns\ResolvesMethodDependencies;
use DiscoveryUkraine\SagaLaraFlow\Data\CompensationDefinition;
use DiscoveryUkraine\SagaLaraFlow\Enums\StepExecution;
use DiscoveryUkraine\SagaLaraFlow\Models\CompensationRun;
use Illuminate\Contracts\Container\BindingResolutionException;
use ReflectionException;
use RuntimeException;
use Throwable;

/**
 * A compensation that throws is recorded as Failed and swallowed: the rollback policy reads the
 * row.
 */
class CompensationExecutor
{
    use ResolvesMethodDependencies;

    public function __construct(
        private readonly CompensationRecorder $recorder,
    ) {}

    /**
     * @see ActionDispatcher::execute()
     *
     * @throws Throwable
     */
    public function execute(
        CompensationRun $compensation,
        CompensationDefinition $definition,
        ?string $rollbackToken = null,
    ): StepExecution {
        if (! $this->recorder->startCompensation($compensation, $rollbackToken)) {
            return StepExecution::ClaimLost;
        }

        try {
            $result = $this->run($definition);
        } catch (Throwable $exception) {
            return $this->recorder->failCompensation($compensation, $exception)
                ? StepExecution::Executed
                : StepExecution::Superseded;
        }

        return $this->recorder->completeCompensation($compensation, $result)
            ? StepExecution::Executed
            : StepExecution::Superseded;
    }

    /**
     * @throws ReflectionException
     * @throws BindingResolutionException
     */
    private function run(CompensationDefinition $definition): mixed
    {
        if ($definition->isClosure()) {
            $closure = $definition->closure?->getClosure()
                ?? throw new RuntimeException('Compensation closure is missing.');

            return $closure();
        }

        $class = $definition->class
            ?? throw new RuntimeException('Compensation class is missing.');

        return $this->callWithDependencies(app()->make($class), 'handle', $definition->arguments);
    }
}
