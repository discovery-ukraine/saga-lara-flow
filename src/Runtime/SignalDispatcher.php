<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Contracts\SignalRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CannotSignalCancellingFlowException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CannotSignalTerminalFlowException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowNotFoundException;
use DiscoveryUkraine\SagaLaraFlow\Jobs\ResumeWorkflowJob;
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
        $current = $this->reread($flowRun);

        if ($current->isTerminal()) {
            throw CannotSignalTerminalFlowException::for($current);
        }

        if (! in_array($current->status, FlowStatus::signalable(), true)) {
            throw CannotSignalCancellingFlowException::for($current);
        }

        $waitingSignal = $this->repository->earliestWaiting($flowRun->id, $name);

        $signal = $waitingSignal === null
            ? null
            : $this->recorder->fulfilWaitingSignal($waitingSignal, $payload);

        // No open wait-signal, or the one we found was claimed by a retry seam while
        // we were writing: keep the delivery as a floating Received row rather than
        // attaching it to a spent signal, where nothing would look for it again.
        $signal ??= $this->recorder->storeReceivedSignal($flowRun, $name, $payload);

        if (config('saga-lara-flow.signals.wake_workflow_on_signal')) {
            $this->wake($flowRun);
        }

        return $signal;
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
        $job = ResumeWorkflowJob::dispatch($flowRun->id);

        if ($flowRun->connection !== null) {
            $job->onConnection($flowRun->connection);
        }

        if ($flowRun->queue !== null) {
            $job->onQueue($flowRun->queue);
        }

        if (config('saga-lara-flow.queue.after_commit')) {
            $job->afterCommit();
        }
    }
}
