<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use Illuminate\Support\Facades\Log;
use Throwable;

/**
 * The engine's second journal: what it absorbed while running.
 *
 * EventLog records what happened to a run in business terms. This records the moments
 * where the world turned out not to be what the engine assumed — a claim lost to
 * whoever already owned the row, an outcome write refused because the row had changed
 * hands, a batch already closed by a duplicate before its own job reported, a run
 * transition refused because the row had moved on, a retry policy that threw. Most are
 * ordinary consequences of at-least-once delivery and of nothing serialising an operator
 * against a worker; the last is a defect in the caller's own code, journalled here for
 * the same reason as the rest — the engine carried on, so nothing else records it.
 *
 * None of them fails a job. A refused transition also reaches its caller: the engine
 * absorbs it and stops, but FlowHandle lets it surface, because an operator whose
 * cancellation did not happen has to be told.
 *
 * Which is why they need a journal of their own: the job succeeds, flow_events stays
 * silent, and an operator investigating a step that ran twice would have nothing to go
 * on. They stay out of flow_events because an abandoned attempt changed nothing in the
 * run's history.
 */
final readonly class AnomalyLog
{
    public const string REASON_CLAIM_LOST = 'claim_lost';

    public const string REASON_OUTCOME_REJECTED = 'outcome_rejected';

    public const string REASON_BATCH_FINISHED_EARLY = 'batch_finished_early';

    public const string REASON_TRANSITION_LOST = 'transition_lost';

    public const string REASON_RETRY_POLICY_THREW = 'retry_policy_threw';

    /**
     * PSR-3's eight, the only strings a PSR logger accepts. A value outside this set
     * would make Monolog throw on a path whose entire purpose is not to throw.
     *
     * @var array<int, string>
     */
    private const array LEVELS = ['emergency', 'alert', 'critical', 'error', 'warning', 'notice', 'info', 'debug'];

    /**
     * Best-effort by construction: every caller absorbs an anomaly without failing its
     * job, so a misconfigured channel or level must not be what turns an expected race
     * into a failure. That failure would not stay local either — an exception escaping
     * a rejected outcome write reaches RunActionJob::failed(), which writes queue
     * bookkeeping into a row this worker no longer owns.
     *
     * @param  array<string, mixed>  $context
     */
    public function log(string $reason, array $context): void
    {
        try {
            $level = config('saga-lara-flow.logging.anomaly_level', 'info');

            if (! is_string($level) || ! in_array($level, self::LEVELS, true)) {
                return;
            }

            $channel = config('saga-lara-flow.logging.channel');

            $logger = $channel === null ? Log::driver() : Log::channel((string) $channel);

            $logger->log($level, "saga-lara-flow: {$reason}", $context + ['reason' => $reason]);
        } catch (Throwable) {
            // Nothing to escalate to: the caller has already decided this attempt is
            // over, and there is no second logger to report a broken logger through.
        }
    }
}
