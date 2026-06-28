<?php

namespace DiscoveryUkraine\SagaLaraFlow\Contracts;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowSignal;

interface SignalRepository
{
    /**
     * The signal wait-signal recorded at this (flow_run_id, wait_sequence) ordinal,
     * if any. Identifies a signal occupying a sequence for replay and history-contract.
     */
    public function find(string $flowRunId, int $sequence): ?FlowSignal;

    /**
     * The earliest delivered-but-unconsumed signal with this name (FIFO by ULID).
     * Floating signals carry no wait_sequence until an awaitSignal consumes them.
     */
    public function earliestPending(string $flowRunId, string $name): ?FlowSignal;

    /**
     * The earliest open wait-signal with this name (FIFO by ULID), used by signal
     * delivery to fulfil a parked awaitSignal.
     */
    public function earliestWaiting(string $flowRunId, string $name): ?FlowSignal;
}
