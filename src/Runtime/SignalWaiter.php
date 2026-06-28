<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Contracts\SignalRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\SignalStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\HistoryContractMismatchException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\FlowSuspended;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowSignal;

/**
 * The awaitSignal seam: resolves a signal by the workflow's deterministic
 * (flow_run_id, sequence) ordinal. It either replays a previously consumed
 * signal, consumes a delivered one inline, or parks the flow with a wait-signal
 * and suspends — exactly like ActionBuilder does for actions.
 *
 * Signals are the special identity case (§6): a delivered signal carries no
 * sequence; an awaitSignal at ordinal S creates a wait-signal with wait_sequence
 * = S. Timeout is a no-op in this phase and arrives with the monitor (Phase 8).
 */
readonly class SignalWaiter
{
    public function __construct(
        private HistoryContractGuard $guard,
        private SignalRepository $repository,
        private SignalRecorder $recorder,
        private Serializer $serializer,
        private FlowSuspender $suspender,
    ) {}

    /**
     * Resolve a signal for the current run: replay it, consume a delivered one, or
     * park and suspend.
     *
     * @throws HistoryContractMismatchException
     * @throws FlowSuspended
     */
    public function await(FlowRuntime $runtime, string $name): mixed
    {
        $flowRun = $runtime->run();
        $sequence = $runtime->nextSequence();

        $signal = $this->guard->expectSignal($flowRun->id, $sequence, $name);

        if ($signal !== null) {
            return match ($signal->status) {
                // Already resolved on an earlier pass: replay its payload verbatim.
                SignalStatus::Consumed => $this->serializer->deserialize($signal->payload),
                // Delivered into the signal since we parked: consume and move on.
                SignalStatus::Received => $this->consume($flowRun, $signal, $sequence),
                // Still parked (Waiting/TimedOut — timeout handling lands in Phase 8).
                default => $this->suspender->suspend('signal', $sequence),
            };
        }

        // Consume the earliest delivered-but-unmatched signal, if one is waiting.
        $pending = $this->repository->earliestPending($flowRun->id, $name);

        if ($pending !== null) {
            return $this->consume($flowRun, $pending, $sequence);
        }

        // Nothing to resolve: park a wait-signal and suspend until a signal arrives.
        $this->recorder->recordSignalWaiting($flowRun, $name, $sequence);

        $this->suspender->suspend('signal', $sequence);
    }

    private function consume(FlowRun $flowRun, FlowSignal $signal, int $sequence): mixed
    {
        $this->recorder->consumeSignal($flowRun, $signal, $sequence);

        return $this->serializer->deserialize($signal->payload);
    }
}
