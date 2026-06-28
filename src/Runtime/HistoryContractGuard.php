<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Contracts\ActionRunRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\SideEffectRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\SignalRepository;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\HistoryContractMismatchException;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowSignal;
use DiscoveryUkraine\SagaLaraFlow\Models\SideEffect;

/**
 * Enforces the determinism / history contract (§5.5, §6): the operation a replay
 * requests at a given (flow_run_id, sequence) ordinal must match the one recorded
 * there. It centralises the cross-type checks the action, side-effect, and signal
 * seams used to duplicate. Each expectX returns the recorded entity (for the seam
 * to replay) or null when the slot is free, throwing HistoryContractMismatchException
 * when a different class/operation-type occupies it.
 */
final readonly class HistoryContractGuard
{
    public function __construct(
        private ActionRunRepository $actionRepository,
        private SideEffectRepository $sideEffectRepository,
        private SignalRepository $signalRepository,
    ) {}

    /**
     * @throws HistoryContractMismatchException
     */
    public function expectAction(string $flowRunId, int $sequence, string $actionClass): ?ActionRun
    {
        $action = $this->actionRepository->find($flowRunId, $sequence);

        if ($action !== null) {
            if ($action->action_class !== $actionClass) {
                throw HistoryContractMismatchException::forActionClass(
                    $sequence,
                    $action->action_class,
                    $actionClass,
                    $flowRunId,
                );
            }

            return $action;
        }

        $requested = "action {$actionClass}";

        $this->rejectSideEffect($flowRunId, $sequence, $requested);
        $this->rejectSignal($flowRunId, $sequence, $requested);

        return null;
    }

    /**
     * @throws HistoryContractMismatchException
     */
    public function expectSideEffect(string $flowRunId, int $sequence, string $key): ?SideEffect
    {
        $requested = "side effect '{$key}'";

        $this->rejectAction($flowRunId, $sequence, $requested);
        $this->rejectSignal($flowRunId, $sequence, $requested);

        return $this->sideEffectRepository->find($flowRunId, $sequence);
    }

    /**
     * @throws HistoryContractMismatchException
     */
    public function expectSignal(string $flowRunId, int $sequence, string $name): ?FlowSignal
    {
        $signal = $this->signalRepository->find($flowRunId, $sequence);

        if ($signal !== null) {
            if ($signal->name !== $name) {
                throw HistoryContractMismatchException::forSignalName(
                    $sequence,
                    $signal->name,
                    $name,
                    $flowRunId,
                );
            }

            return $signal;
        }

        $requested = "signal '{$name}'";

        $this->rejectAction($flowRunId, $sequence, $requested);
        $this->rejectSideEffect($flowRunId, $sequence, $requested);

        return null;
    }

    /**
     * @throws HistoryContractMismatchException
     */
    private function rejectAction(string $flowRunId, int $sequence, string $requested): void
    {
        $action = $this->actionRepository->find($flowRunId, $sequence);

        if ($action !== null) {
            throw HistoryContractMismatchException::forOperationType(
                $sequence,
                "action {$action->action_class}",
                $requested,
                $flowRunId,
            );
        }
    }

    /**
     * @throws HistoryContractMismatchException
     */
    private function rejectSideEffect(string $flowRunId, int $sequence, string $requested): void
    {
        $sideEffect = $this->sideEffectRepository->find($flowRunId, $sequence);

        if ($sideEffect !== null) {
            throw HistoryContractMismatchException::forOperationType(
                $sequence,
                "side effect '{$sideEffect->key}'",
                $requested,
                $flowRunId,
            );
        }
    }

    /**
     * @throws HistoryContractMismatchException
     */
    private function rejectSignal(string $flowRunId, int $sequence, string $requested): void
    {
        $signal = $this->signalRepository->find($flowRunId, $sequence);

        if ($signal !== null) {
            throw HistoryContractMismatchException::forOperationType(
                $sequence,
                "signal '{$signal->name}'",
                $requested,
                $flowRunId,
            );
        }
    }
}
