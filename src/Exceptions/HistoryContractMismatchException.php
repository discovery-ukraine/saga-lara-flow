<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions;

/**
 * Raised when a replay requests a different operation than the one recorded at a
 * given (flow_run_id, sequence). This almost always means the workflow code
 * changed while an older run was still active: the deterministic control flow no
 * longer matches stored history. Version long-running workflows via separate
 * classes/directories rather than editing a class that has live runs.
 *
 * It is a business-level FlowException (not an internal control signal), so the
 * drive loop treats it as a genuine failure and marks the run Failed.
 */
class HistoryContractMismatchException extends FlowException
{
    /**
     * The recorded action class at this sequence differs from the requested one.
     */
    public static function forActionClass(
        int $sequence,
        string $recordedClass,
        string $requestedClass,
        string $runId,
    ): self {
        return new self(sprintf(
            'Sequence %d expected action %s, but workflow requested action %s. '.
            'The workflow code likely changed while run %s is still active. '.
            'Version long-running workflows via separate classes/directories.',
            $sequence,
            $recordedClass,
            $requestedClass,
            $runId,
        ));
    }

    /**
     * A different kind of operation (action vs side effect) is recorded at this
     * sequence than the one the workflow requested.
     */
    public static function forOperationType(
        int $sequence,
        string $recorded,
        string $requested,
        string $runId,
    ): self {
        return new self(sprintf(
            'Sequence %d expected %s, but workflow requested %s. '.
            'The workflow code likely changed while run %s is still active. '.
            'Version long-running workflows via separate classes/directories.',
            $sequence,
            $recorded,
            $requested,
            $runId,
        ));
    }
}
