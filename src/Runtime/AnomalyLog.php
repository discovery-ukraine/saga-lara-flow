<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use Illuminate\Support\Facades\Log;
use Throwable;

/**
 * The engine's second journal: what it absorbed while running. EventLog records what
 * happened to a run in business terms; this records the moments where the world turned
 * out not to be what the engine assumed — a lost claim, a refused write, a batch already
 * closed, a policy that threw. Each reason is a REASON_* constant below.
 *
 * They need a journal of their own because the job usually succeeds and flow_events stays
 * silent: an abandoned attempt changed nothing in the run's history, so nothing else
 * records it. A refused transition is the exception that can still surface — FlowHandle
 * raises it, and a child-close job rethrows while the child it was sent to close is live.
 */
final readonly class AnomalyLog
{
    public const string REASON_CLAIM_LOST = 'claim_lost';

    public const string REASON_CLAIM_NOT_COMMITTED = 'claim_not_committed';

    public const string REASON_OUTCOME_REJECTED = 'outcome_rejected';

    public const string REASON_BATCH_FINISHED_EARLY = 'batch_finished_early';

    public const string REASON_TRANSITION_LOST = 'transition_lost';

    public const string REASON_RETRY_POLICY_THREW = 'retry_policy_threw';

    public const string REASON_EXPIRY_FAILED = 'expiry_failed';

    public const string REASON_WRITE_REFUSED = 'write_refused';

    public const string REASON_REJECTION_UNDELIVERED = 'rejection_undelivered';

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
