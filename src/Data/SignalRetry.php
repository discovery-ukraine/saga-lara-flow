<?php

namespace DiscoveryUkraine\SagaLaraFlow\Data;

use Closure;
use DateTimeInterface;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\HistoryContractMismatchException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\InternalFlowControl;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\RetryPolicyReentryException;
use DiscoveryUkraine\SagaLaraFlow\Retry\RecordedFailure;
use DiscoveryUkraine\SagaLaraFlow\Retry\RetryContext;
use DiscoveryUkraine\SagaLaraFlow\Retry\RetryPolicy;
use DiscoveryUkraine\SagaLaraFlow\Runtime\AnomalyLog;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowRuntime;
use InvalidArgumentException;
use Throwable;

/**
 * Both forms of a retryOnSignal() policy in one shape: a RetryPolicy contributes
 * shouldRetry(...) as the decision, when: contributes itself.
 */
final readonly class SignalRetry
{
    /**
     * @param  list<class-string<Throwable>>|null  $only
     * @param  ?Closure(RetryContext): bool  $decision
     */
    private function __construct(
        public string $signal,
        public ?int $maxRetries,
        public ?int $waitSeconds,
        public ?array $only,
        public ?Closure $decision,
    ) {}

    /**
     * @param  list<class-string<Throwable>>|null  $only
     * @param  ?Closure(RetryContext): bool  $when
     */
    public static function for(
        RetryPolicy|string $signal,
        ?int $maxRetries = null,
        ?int $waitSeconds = null,
        ?array $only = null,
        ?Closure $when = null,
    ): self {
        if ($signal instanceof RetryPolicy) {
            self::rejectPolicyWithArguments($maxRetries, $waitSeconds, $only, $when);

            // Unwrapped once, so the policy is asked as often as an argument list is
            // evaluated and every seam goes on reading a plain string.
            $when = $signal->shouldRetry(...);
            $maxRetries = $signal->maxRetries();
            $waitSeconds = $signal->waitSeconds();
            $only = $signal->only();
            $signal = $signal->signal();
        }

        self::rejectNegative('maxRetries', $maxRetries);
        self::rejectNegative('waitSeconds', $waitSeconds);

        return new self($signal, $maxRetries, $waitSeconds, $only, $when);
    }

    /**
     * Resolve the retry budget: an explicit maxRetries: wins, then the configured
     * global cap, then null — unbounded, with the wait timeout and the run's own
     * expires_at as the remaining brakes.
     */
    public function resolvedMaxRetries(): ?int
    {
        if ($this->maxRetries !== null) {
            return $this->maxRetries;
        }

        $configured = config('saga-lara-flow.actions.retry_on_signal.max_retries');

        if ($configured === null) {
            return null;
        }

        self::rejectNegative('actions.retry_on_signal.max_retries', (int) $configured);

        return (int) $configured;
    }

    public function waitDeadline(): ?DateTimeInterface
    {
        return $this->waitSeconds === null ? null : now()->addSeconds($this->waitSeconds);
    }

    /**
     * Whether the recorded failure falls inside the only: filter. A null filter accepts
     * everything, and a failure with no recorded class is never retried under one.
     */
    public function matches(RecordedFailure $failure): bool
    {
        return $this->only === null || $failure->is(...$this->only);
    }

    /**
     * The last gate, and the only one that runs the caller's own code.
     *
     * A throw is absorbed as "do not park", the outcome the caller was already
     * prepared for; letting it out would fail the whole run on one path and truncate
     * the compensation stack in silence on the other. It is logged rather than
     * swallowed: a policy that never parks looks identical to one that always throws.
     *
     * @param  array<string, mixed>  $subject
     *
     * @throws InternalFlowControl
     */
    public function allows(FlowRuntime $runtime, RetryContext $context, array $subject): bool
    {
        $decide = $this->decision;

        if ($decide === null) {
            return true;
        }

        // Compensation-only planning stops at the parked ordinal either way, so the
        // answer would change nothing — and it runs caller code, which that pass must not.
        if ($runtime->isCollecting()) {
            return true;
        }

        $runtime->beginDeciding();

        try {
            return $decide($context);
        } catch (InternalFlowControl|HistoryContractMismatchException|RetryPolicyReentryException $control) {
            // Not answers, and none has a safe reading: the engine suspends with the
            // first, reports the second on its own terms, and the third is a workflow
            // the caller has to fix, not a failure that quietly never parks.
            throw $control;
        } catch (Throwable $exception) {
            app(AnomalyLog::class)->log(AnomalyLog::REASON_RETRY_POLICY_THREW, [
                'flow_run_id' => $context->runId,
                ...$subject,
                'sequence' => $context->sequence,
                'signal' => $this->signal,
                'exception' => $exception::class,
                'message' => $exception->getMessage(),
            ]);

            return false;
        } finally {
            $runtime->endDeciding();
        }
    }

    /**
     * Reject a negative budget or wait before it can be persisted. The columns are
     * unsigned, so a negative value means an error on MySQL and a policy that silently
     * never parks on the drivers that store it; failing here says which value is
     * wrong, the same way on every driver.
     */
    private static function rejectNegative(string $name, ?int $value): void
    {
        if ($value !== null && $value < 0) {
            throw new InvalidArgumentException(
                "retryOnSignal() {$name} must be zero or greater, got {$value}.",
            );
        }
    }

    /**
     * A policy object and the arguments it replaces are two sources of truth for one
     * decision, and there is no reading of "both" that is not a guess about which one
     * the caller meant. Refuse it, naming what to drop.
     *
     * @param  list<class-string<Throwable>>|null  $only
     */
    private static function rejectPolicyWithArguments(
        ?int $maxRetries,
        ?int $waitSeconds,
        ?array $only,
        ?Closure $when,
    ): void {
        $given = array_keys(array_filter([
            'maxRetries' => $maxRetries !== null,
            'waitSeconds' => $waitSeconds !== null,
            'only' => $only !== null,
            'when' => $when !== null,
        ]));

        if ($given === []) {
            return;
        }

        throw new InvalidArgumentException(
            'retryOnSignal() takes a RetryPolicy or the arguments it replaces, not both; '
            .'drop '.implode(', ', $given).' or move it into the policy.',
        );
    }
}
