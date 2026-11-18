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
 * Runs a single compensation to its terminal state. Shared by sync inline
 * rollback (SagaRunner) and the queued RunCompensationJob. A compensation that
 * throws is recorded as Failed and the throwable is swallowed — rollback policy
 * (Stop vs Continue) is decided by SagaRunner from the recorded status, never by
 * letting the job itself fail.
 */
class CompensationExecutor
{
    use ResolvesMethodDependencies;

    public function __construct(
        private readonly CompensationRecorder $recorder,
    ) {}

    /**
     * Mirrors ActionDispatcher::execute(): anything but Executed means someone else is
     * handling this row, never that the compensation failed. ClaimLost is the row
     * being unavailable before anything ran; Superseded is the undo having run with
     * its outcome dropped.
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
