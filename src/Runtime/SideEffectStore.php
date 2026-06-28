<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use Closure;
use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\HistoryContractMismatchException;

/**
 * Records and replays side effects — nondeterministic values (now(), uuids,
 * random) captured once and reused verbatim on every later replay. Identity is
 * the (flow_run_id, sequence) ordinal, exactly like actions; the key is a human
 * label only.
 *
 * On the first pass the factory runs once and its value is persisted. On every
 * replay the stored value is returned and the factory is never called again, so
 * the workflow stays deterministic across resumes.
 */
readonly class SideEffectStore
{
    public function __construct(
        private HistoryContractGuard $guard,
        private SideEffectRecorder $recorder,
        private Serializer $serializer,
    ) {}

    /**
     * Resolve a side effect for the current run: replay its stored value, or run
     * the factory once and record it.
     *
     * @throws HistoryContractMismatchException
     */
    public function resolve(FlowRuntime $runtime, string $key, Closure $sideEffectCallback): mixed
    {
        $flowRun = $runtime->run();
        $sequence = $runtime->nextSequence();

        $existing = $this->guard->expectSideEffect($flowRun->id, $sequence, $key);

        if ($existing !== null) {
            $this->recorder->sideEffectReused($flowRun, $existing);

            return $this->serializer->deserialize($existing->value);
        }

        $sideEffectResult = $sideEffectCallback();

        $this->recorder->recordSideEffect($flowRun, $sequence, $key, $sideEffectResult);

        return $sideEffectResult;
    }
}
