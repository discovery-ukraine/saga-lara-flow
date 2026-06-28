<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Concerns\ResolvesMethodDependencies;
use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Contracts\StateMachine;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\FlowSuspended;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\InternalFlowControl;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Support\TenancyManager;
use Throwable;

/**
 * The drive loop — the heart of the engine. It runs the workflow's handle()
 * method and interprets the control-flow exceptions it throws: replay on inline
 * resolution (sync), suspend on a genuine wait (queued), fail on a business
 * error. Completed steps are skipped on replay because they resolve from stored
 * history by their (flow_run_id, sequence) identity.
 *
 * Invariant: catch (Throwable) sits AFTER catch (InternalFlowControl), so an
 * internal control signal is never mistaken for a business failure.
 */
class FlowExecutor
{
    use ResolvesMethodDependencies;

    public function __construct(
        private readonly StateMachine $stateMachine,
        private readonly StepRecorder $recorder,
        private readonly FlowRuntime $runtime,
        private readonly TenancyManager $tenancy,
        private readonly Serializer $serializer,
    ) {}

    public function drive(FlowRun $flowRun, RunMode $mode): FlowRun
    {
        $this->tenancy->restore($flowRun);

        $resuming = $flowRun->status !== FlowStatus::Pending;

        $this->stateMachine->transition($flowRun, FlowStatus::Running);

        $resuming ? $this->recorder->flowResumed($flowRun) : $this->recorder->flowStarted($flowRun);

        while (true) {
            $this->runtime->bind($flowRun, $mode);
            $this->runtime->reset();

            try {
                try {
                    $workflow = app()->make($flowRun->workflow_class, ['runtime' => $this->runtime]);

                    /** @var array<int, mixed> $arguments */
                    $arguments = (array) $this->serializer->deserialize($flowRun->arguments ?? []);

                    $result = $this->callWithDependencies($workflow, 'handle', $arguments);
                } finally {
                    $this->runtime->clear();
                }
            } catch (FlowSuspended $suspended) {
                if ($suspended->inlineResolved) {
                    continue; // Sync: the step ran inline; replay from the top.
                }

                return $this->suspend($flowRun);
            } catch (InternalFlowControl) {
                return $this->suspend($flowRun);
            } catch (Throwable $exception) {
                return $this->failFlow($flowRun, $exception);
            }

            return $this->completeFlow($flowRun, $result);
        }
    }

    private function suspend(FlowRun $flowRun): FlowRun
    {
        $flowRun->markWaiting();

        $this->recorder->flowWaiting($flowRun);

        return $flowRun;
    }

    private function completeFlow(FlowRun $flowRun, mixed $result): FlowRun
    {
        $flowRun->result = $this->normalizeResult($result);

        $flowRun->markCompleted();

        $this->recorder->flowCompleted($flowRun);

        return $flowRun;
    }

    private function failFlow(FlowRun $flowRun, Throwable $exception): FlowRun
    {
        $flowRun->markFailed([
            'class' => $exception::class,
            'message' => $exception->getMessage(),
            'code' => $exception->getCode(),
        ]);

        $this->recorder->flowFailed($flowRun, $exception);

        return $flowRun;
    }

    /**
     * @return array<int|string, mixed>|null
     */
    private function normalizeResult(mixed $result): ?array
    {
        $serialized = $this->serializer->serialize($result);

        if ($serialized === null) {
            return null;
        }

        return is_array($serialized) ? $serialized : ['value' => $serialized];
    }
}
