<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DateTimeInterface;
use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Contracts\SignalRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\SignalStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\AwaitSignalTimeoutException;
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
 * Signals are the special identity case: a delivered signal carries no
 * sequence; an awaitSignal at ordinal S creates a wait-signal with wait_sequence
 * = S. A timeout deadline (awaitSignal(timeout:) / timeoutAfter()) is persisted on
 * the wait-marker; the monitor flips it to TimedOut after the deadline, and the
 * parked awaitSignal then resolves it by throwing AwaitSignalTimeoutException.
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
     * @throws AwaitSignalTimeoutException
     * @throws FlowSuspended
     */
    public function await(FlowRuntime $runtime, string $name, ?DateTimeInterface $timeout = null): mixed
    {
        $flowRun = $runtime->run();
        $sequence = $runtime->nextSequence();

        $signal = $this->guard->expectSignal($flowRun->id, $sequence, $name);

        // Compensation-only planning: replay a consumed signal, surface a timed-out
        // one (so a catch-and-continue workflow still collects later steps), otherwise
        // stop here (a not-yet-resolved signal is the live frontier).
        if ($runtime->isCollecting()) {
            if ($signal !== null && $signal->status === SignalStatus::Consumed) {
                return $this->serializer->deserialize($signal->payload);
            }

            if ($signal !== null && $signal->status === SignalStatus::TimedOut) {
                throw AwaitSignalTimeoutException::for($signal, $sequence);
            }

            $this->suspender->suspend('signal', $sequence);
        }

        if ($signal !== null) {
            return match ($signal->status) {
                // Already resolved on an earlier pass: replay its payload verbatim.
                SignalStatus::Consumed => $this->serializer->deserialize($signal->payload),
                // Delivered into the signal since we parked: consume and move on.
                SignalStatus::Received => $this->consume($flowRun, $signal, $sequence),
                // The monitor timed the wait-marker out: surface a business error the
                // workflow may catch, otherwise it fails the flow and rolls back.
                SignalStatus::TimedOut => throw AwaitSignalTimeoutException::for($signal, $sequence),
                // Still parked (Waiting): keep waiting.
                default => $this->suspender->suspend('signal', $sequence),
            };
        }

        // Consume the earliest delivered-but-unmatched signal, if one is waiting.
        $pending = $this->repository->earliestPending($flowRun->id, $name);

        if ($pending !== null) {
            return $this->consume($flowRun, $pending, $sequence);
        }

        // Nothing to resolve: park a wait-signal (with its timeout deadline, if any)
        // and suspend until a signal arrives or the monitor times it out.
        $this->recorder->recordSignalWaiting($flowRun, $name, $sequence, $timeout);

        $this->suspender->suspend('signal', $sequence);
    }

    private function consume(FlowRun $flowRun, FlowSignal $signal, int $sequence): mixed
    {
        $this->recorder->consumeSignal($flowRun, $signal, $sequence);

        return $this->serializer->deserialize($signal->payload);
    }
}
