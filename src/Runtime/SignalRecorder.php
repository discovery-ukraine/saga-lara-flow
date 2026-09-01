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
 * It also settles the waits a finished run leaves open (settleOpenWaits).
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
     * deadline so the monitor can later time the wait-signal out.
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
     * The flip is conditional: delivery holds no lock, so a retry seam may have
     * claimed the signal since it was read. Returns null then — the signal is spent,
     * and the caller stores this delivery as a floating one instead.
     *
     * @param  array<int|string, mixed>  $payload
     */
    public function fulfilWaitingSignal(FlowSignal $signal, array $payload): ?FlowSignal
    {
        $claimed = $this->claimWaiting($signal, [
            'payload' => $payload,
            'status' => SignalStatus::Received,
            'received_at' => Carbon::now(),
        ]);

        if (! $claimed) {
            return null;
        }

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
     * Consume a wait-signal for this sequence, but only while it is still Waiting.
     * Returns false when a delivery landed in it since the caller read it: that
     * delivery is real, and taking it here too would turn two signals into one.
     *
     * This closes a signal that received nothing itself — the delivery that ended its
     * wait arrived as a separate floating row — so the history row is appended but no
     * FlowSignalConsumed is dispatched. The row carrying the payload dispatches it,
     * once, so a listener counting consumptions sees one per delivered signal.
     */
    public function consumeWhileWaiting(FlowRun $flowRun, FlowSignal $signal, int $sequence): bool
    {
        $claimed = $this->claimWaiting($signal, [
            'status' => SignalStatus::Consumed,
            'wait_sequence' => $sequence,
            'consumed_at' => Carbon::now(),
        ]);

        if (! $claimed) {
            return false;
        }

        $this->events->record($flowRun, FlowEventType::SignalConsumed, $sequence, $signal, [
            'name' => $signal->name,
            'superseded' => true,
        ]);

        return true;
    }

    /**
     * Time out a still-Waiting wait-signal (monitor): flip it to TimedOut and
     * append a signal.timed_out event. On replay the parked awaitSignal resolves it
     * by throwing AwaitSignalTimeoutException. No Laravel event is dispatched.
     *
     * Conditional for the same reason delivery is: the monitor writes a batch one row
     * at a time, so a delivery can land in one between the read and this write, and
     * calling that acknowledged signal a timeout would roll a saga back. Returns false
     * when the row has moved on, so the monitor neither counts it nor wakes the run.
     */
    public function timeoutSignal(FlowSignal $signal): bool
    {
        if (! $this->claimWaiting($signal, ['status' => SignalStatus::TimedOut])) {
            return false;
        }

        $this->events->record(
            $signal->flowRun,
            FlowEventType::SignalTimedOut,
            $signal->wait_sequence,
            $signal,
            ['name' => $signal->name],
        );

        return true;
    }

    /**
     * Move a wait-signal out of Waiting in a single conditional write, and sync the in-memory
     * model to match when it lands. Returns false when the row is no longer Waiting: someone
     * else moved it first and this caller must not write over them.
     *
     * The condition lives in the UPDATE — the only form every supported driver enforces
     * atomically — so these transitions raise no Eloquent model events. flow_events records
     * every one that lands; a Laravel event accompanies only the paths documented as raising
     * one, and a timeout deliberately raises none. The model is re-read rather than filled
     * from the values just written, which are already database-ready.
     *
     * @param  array<string, mixed>  $values
     */
    private function claimWaiting(FlowSignal $signal, array $values): bool
    {
        $attributes = $signal->newInstance()->forceFill($values)->getAttributes();

        $claimed = $signal->newQuery()
            ->whereKey($signal->getKey())
            ->where('status', SignalStatus::Waiting)
            ->update($attributes) === 1;

        if (! $claimed) {
            return false;
        }

        $signal->refresh();

        return true;
    }

    /**
     * Close the wait-signals a run that reached a terminal state left open. Cancelled
     * says the wait ended with the run, not on its own terms — unlike TimedOut, which
     * means a deadline passed and which replay surfaces as a business error.
     *
     * Only open wait-markers are settled. A delivered signal (no wait_sequence) and a
     * wait already flipped to Received are history — "arrived, nobody consumed it" — and
     * rewriting them would erase that. No event is appended: the run's own terminal
     * event records both the moment and the cause.
     */
    public function settleOpenWaits(FlowRun $flowRun): int
    {
        /** @var class-string<FlowSignal> $model */
        $model = config('saga-lara-flow.models.flow_signal');

        return $model::query()
            ->where('flow_run_id', $flowRun->id)
            ->whereNotNull('wait_sequence')
            ->where('status', SignalStatus::Waiting)
            ->update(['status' => SignalStatus::Cancelled]);
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
