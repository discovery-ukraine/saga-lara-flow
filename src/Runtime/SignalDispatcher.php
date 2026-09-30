<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Contracts\SignalRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CannotSignalCancellingFlowException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CannotSignalTerminalFlowException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowNotFoundException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\NoAwaitingRetrySignalException;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowSignal;

/**
 * Delivers an external signal into a run and wakes it. If the run is parked on a
 * matching awaitSignal, the open wait-signal is fulfilled in place; otherwise the
 * signal is stored as a floating Received row for a future awaitSignal to consume
 * (FIFO). Delivery runs outside the queue and holds no lock, so filling a signal is
 * a conditional write that falls back to the floating row when it loses.
 * Only a run that can still consume one accepts a signal: FlowStatus::signalable().
 */
readonly class SignalDispatcher
{
    public function __construct(
        private SignalRepository $repository,
        private SignalRecorder $recorder,
        private FlowResumer $resumer,
        private ParkedRetrySignals $parked,
    ) {}

    /**
     * @param  array<int|string, mixed>  $payload
     *
     * @throws CannotSignalTerminalFlowException
     * @throws CannotSignalCancellingFlowException
     * @throws FlowNotFoundException
     */
    public function deliver(FlowRun $flowRun, string $name, array $payload): FlowSignal
    {
        $this->refuseUnsignalable($this->reread($flowRun));

        $waitingSignal = $this->repository->earliestWaiting($flowRun->id, $name);

        $signal = $waitingSignal === null
            ? null
            : $this->recorder->fulfilWaitingSignal($waitingSignal, $payload);

        // No open wait-signal, or the one we found was claimed by a retry seam while
        // we were writing: keep the delivery as a floating Received row rather than
        // attaching it to a spent signal, where nothing would look for it again.
        $signal ??= $this->recorder->storeReceivedSignal($flowRun, $name, $payload);

        $this->wake($flowRun);

        return $signal;
    }

    /**
     * Fill the open waits of the run's parked steps and children, read from the writer, and
     * wake the run once. A park whose wait already holds its delivery, or has it closed while
     * this runs, gets the wake alone: a floating signal would be left for a later awaitSignal().
     *
     * @param  array<int|string, mixed>  $payload
     * @return list<FlowSignal>
     *
     * @throws CannotSignalTerminalFlowException
     * @throws CannotSignalCancellingFlowException
     * @throws NoAwaitingRetrySignalException
     * @throws FlowNotFoundException
     */
    public function deliverRetry(FlowRun $flowRun, array $payload): array
    {
        $names = $this->parked->of($flowRun);

        // Read after the parked rows: a run that ended in between settled them, and says so.
        $this->refuseUnsignalable($this->reread($flowRun));

        if ($names === []) {
            throw NoAwaitingRetrySignalException::for($flowRun);
        }

        $signals = [];

        foreach ($this->parked->openWaits($flowRun, $names) as $wait) {
            $signal = $this->recorder->fulfilWaitingSignal($wait, $payload);

            if ($signal !== null) {
                $signals[] = $signal;
            }
        }

        $this->wake($flowRun);

        return $signals;
    }

    /**
     * @throws CannotSignalTerminalFlowException
     * @throws CannotSignalCancellingFlowException
     */
    private function refuseUnsignalable(FlowRun $current): void
    {
        if ($current->isTerminal()) {
            throw CannotSignalTerminalFlowException::for($current);
        }

        if (! in_array($current->status, FlowStatus::signalable(), true)) {
            throw CannotSignalCancellingFlowException::for($current);
        }
    }

    /**
     * The run this delivery is decided on. Falling back to the caller's snapshot when
     * the writer has no row would trust the state this read exists to replace, so a
     * run that is gone raises instead.
     */
    private function reread(FlowRun $flowRun): FlowRun
    {
        return $flowRun->newQuery()->useWritePdo()->find($flowRun->getKey())
            ?? throw FlowNotFoundException::for((string) $flowRun->getKey());
    }

    private function wake(FlowRun $flowRun): void
    {
        if (config('saga-lara-flow.signals.wake_workflow_on_signal')) {
            $this->resumer->resume($flowRun);
        }
    }
}
