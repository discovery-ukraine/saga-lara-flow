<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DateTimeInterface;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowEventType;
use DiscoveryUkraine\SagaLaraFlow\Enums\SignalStatus;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowSignalConsumed;
use DiscoveryUkraine\SagaLaraFlow\Events\FlowSignalReceived;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowSignal;
use Illuminate\Support\Carbon;

/**
 * Persists the signal lifecycle (waiting → received → consumed) and dispatches
 * the matching events. Signal payloads are stored via the model's JSON cast.
 */
final readonly class SignalRecorder
{
    public function __construct(
        private EventLog $events,
    ) {}

    /**
     * Park the flow on an unmatched awaitSignal: persist a Waiting wait-signal at
     * its (flow_run_id, wait_sequence) ordinal. No flow_events row is written here
     * (there is no signal.waiting type); the flow-level FlowWaiting event records
     * the suspension and the signal row itself is visible via FlowRun::signals().
     *
     * A non-null $timeoutAt persists the awaitSignal(timeout:) / timeoutAfter()
     * deadline so the monitor can later time the wait-marker out.
     */
    public function recordSignalWaiting(
        FlowRun $flowRun,
        string $name,
        int $sequence,
        ?DateTimeInterface $timeoutAt = null,
    ): FlowSignal {
        $signal = $this->newSignal();

        $signal->fill([
            'flow_run_id' => $flowRun->id,
            'name' => $name,
            'status' => SignalStatus::Waiting,
            'wait_sequence' => $sequence,
            'timeout_at' => $timeoutAt ?? $this->defaultTimeout(),
        ]);

        $signal->save();

        return $signal;
    }

    /**
     * Fall back to the configured default signal timeout (seconds from now) when none
     * was set explicitly via awaitSignal(timeout:)/timeoutAfter(). null config off.
     */
    private function defaultTimeout(): ?DateTimeInterface
    {
        $seconds = config('saga-lara-flow.monitor.expiration.defaults.signal');

        return $seconds === null ? null : Carbon::now()->addSeconds((int) $seconds);
    }

    /**
     * Store an externally delivered signal that no open wait-signal matched yet —
     * a "floating" Received signal kept until some awaitSignal consumes it (FIFO).
     *
     * @param  array<int|string, mixed>  $payload
     */
    public function storeReceivedSignal(FlowRun $flowRun, string $name, array $payload): FlowSignal
    {
        $signal = $this->newSignal();

        $signal->fill([
            'flow_run_id' => $flowRun->id,
            'name' => $name,
            'payload' => $payload,
            'status' => SignalStatus::Received,
            'received_at' => Carbon::now(),
        ]);

        $signal->save();

        $this->emitSignalReceived($flowRun, $signal);

        return $signal;
    }

    /**
     * Deliver a signal into an existing Waiting wait-signal: attach the payload and
     * flip it to Received, keeping its wait_sequence so the parked awaitSignal can
     * resolve it on replay.
     *
     * @param  array<int|string, mixed>  $payload
     */
    public function fulfilWaitingSignal(FlowSignal $signal, array $payload): FlowSignal
    {
        $signal->payload = $payload;
        $signal->status = SignalStatus::Received;
        $signal->received_at = Carbon::now();
        $signal->save();

        $this->emitSignalReceived($signal->flowRun, $signal);

        return $signal;
    }

    /**
     * Consume a Received signal for an awaitSignal at this sequence: bind it to the
     * (flow_run_id, wait_sequence) ordinal and flip it to Consumed. On replay the
     * signal resolves from here, so the workflow sees the same payload every pass.
     */
    public function consumeSignal(FlowRun $flowRun, FlowSignal $signal, int $sequence): FlowSignal
    {
        $signal->status = SignalStatus::Consumed;
        $signal->wait_sequence = $sequence;
        $signal->consumed_at = Carbon::now();
        $signal->save();

        $this->events->record($flowRun, FlowEventType::SignalConsumed, $sequence, $signal, [
            'name' => $signal->name,
        ]);

        event(new FlowSignalConsumed($signal));

        return $signal;
    }

    /**
     * Time out a still-Waiting wait-marker (monitor): flip it to TimedOut and
     * append a signal.timed_out event. On replay the parked awaitSignal resolves it
     * by throwing AwaitSignalTimeoutException. No Laravel event is dispatched.
     */
    public function timeoutSignal(FlowSignal $signal): void
    {
        $signal->status = SignalStatus::TimedOut;
        $signal->save();

        $this->events->record(
            $signal->flowRun,
            FlowEventType::SignalTimedOut,
            $signal->wait_sequence,
            $signal,
            ['name' => $signal->name],
        );
    }

    private function emitSignalReceived(FlowRun $flowRun, FlowSignal $signal): void
    {
        $this->events->record($flowRun, FlowEventType::SignalReceived, $signal->wait_sequence, $signal, [
            'name' => $signal->name,
        ]);

        event(new FlowSignalReceived($signal));
    }

    private function newSignal(): FlowSignal
    {
        /** @var class-string<FlowSignal> $model */
        $model = config('saga-lara-flow.models.flow_signal');

        return new $model;
    }
}
