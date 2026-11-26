<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use Illuminate\Support\Facades\Log;
use Throwable;

final readonly class AnomalyLog
{
    public const string REASON_CLAIM_LOST = 'claim_lost';

    public const string REASON_CLAIM_NOT_COMMITTED = 'claim_not_committed';

    public const string REASON_OUTCOME_REJECTED = 'outcome_rejected';

    public const string REASON_BATCH_FINISHED_EARLY = 'batch_finished_early';

    public const string REASON_TRANSITION_LOST = 'transition_lost';

    public const string REASON_RETRY_POLICY_THREW = 'retry_policy_threw';

    public const string REASON_EXPIRY_FAILED = 'expiry_failed';

    public const string REASON_REPLAN_FAILED = 'replan_failed';

    public const string REASON_REPLAN_INCOMPLETE = 'replan_incomplete';

    public const string REASON_ROLLBACK_RESUME_FAILED = 'rollback_resume_failed';

    public const string REASON_ROLLBACK_ABANDONED = 'rollback_abandoned';

    public const string REASON_CLOSE_FAILED = 'close_failed';

    public const string REASON_WRITE_REFUSED = 'write_refused';

    public const string REASON_REJECTION_UNDELIVERED = 'rejection_undelivered';

    /**
     * PSR-3's levels: any other string makes Monolog throw, on a path that must not.
     *
     * @var array<int, string>
     */
    private const array LEVELS = ['emergency', 'alert', 'critical', 'error', 'warning', 'notice', 'info', 'debug'];

    /**
     * Never throws: a misconfigured channel must not turn an absorbed race into a failed job.
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
            // The caller already absorbed the race; a broken logger has nothing to report to.
        }
    }
}
