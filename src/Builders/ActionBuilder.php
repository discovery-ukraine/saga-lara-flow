<?php

namespace DiscoveryUkraine\SagaLaraFlow\Builders;

use Closure;
use DateTimeInterface;
use DiscoveryUkraine\SagaLaraFlow\Contracts\ActionRunRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Contracts\SignalRepository;
use DiscoveryUkraine\SagaLaraFlow\Data\ActionSchedule;
use DiscoveryUkraine\SagaLaraFlow\Data\CompensationDefinition;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationFailurePolicy;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Enums\SignalStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\StepExecution;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ActionClaimFailedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ActionFailedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowExpiredException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\HistoryContractMismatchException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\FlowSuspended;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\InternalFlowControl;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\RetryPolicyReentryException;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowSignal;
use DiscoveryUkraine\SagaLaraFlow\Retry\RecordedFailure;
use DiscoveryUkraine\SagaLaraFlow\Retry\RetryContext;
use DiscoveryUkraine\SagaLaraFlow\Retry\RetryPolicy;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ActionDispatcher;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ActionRecorder;
use DiscoveryUkraine\SagaLaraFlow\Runtime\AnomalyLog;
use DiscoveryUkraine\SagaLaraFlow\Runtime\CompensationEntry;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowRuntime;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowSuspender;
use DiscoveryUkraine\SagaLaraFlow\Runtime\HistoryContractGuard;
use DiscoveryUkraine\SagaLaraFlow\Runtime\SignalRecorder;
use DiscoveryUkraine\SagaLaraFlow\Support\AttributeReader;
use Illuminate\Database\ConnectionInterface;
use Illuminate\Support\Facades\DB;
use InvalidArgumentException;
use Throwable;

/**
 * Fluent builder for a single action step. run() is the replay seam: it
 * identifies the step by its (flow_run_id, sequence) ordinal and either returns
 * the stored result, rethrows the stored failure, or schedules/executes the
 * step and suspends the flow.
 *
 * compensateWith() registers a compensation (a class or a closure) that is pushed
 * onto the saga stack when the step resolves Completed (deterministically on every
 * replay), so a later business failure rolls it back. By default the step's OWN
 * failure does not trigger its compensation (classic saga: only completed steps are
 * undone); compensateStepOnSelfFailure() opts a non-atomic step into being compensated when
 * it fails too — its compensation must then be idempotent and safe when the step did
 * nothing. onCompensationFailure() overrides the default Stop policy. All three
 * resolve with precedence action > group (saga()) > config.
 *
 * Constructed by the engine, and reachable for subclassing only by overriding
 * Workflow::action(): its method signatures are treated as internal.
 *
 * retryOnSignal() turns a failure into a wait: the step parks on a named signal, and
 * each delivery re-runs THIS step alone at the very same ordinal. The seam is the only
 * decision-maker, so nothing else resolves the row it reads back. Its policy may be
 * four arguments or a RetryPolicy object; either way nothing about it is persisted
 * beyond the signal name and the ceiling, and the decision is retaken on every replay.
 */
class ActionBuilder
{
    private ?CompensationDefinition $compensation = null;

    private ?CompensationFailurePolicy $actionCompensationFailurePolicy = null;

    private ?CompensationFailurePolicy $groupCompensationFailurePolicy = null;

    private ?int $parallelGroupId = null;

    private ?bool $compensateOnSelfFailure = null;

    private ?bool $groupCompensateOnSelfFailure = null;

    private ?bool $continueOnFailure = null;

    private mixed $fallbackValueOnFail = null;

    private ?DateTimeInterface $expiresAt = null;

    private ?string $retrySignal = null;

    private ?int $retryMaxRetries = null;

    private ?int $retryWaitSeconds = null;

    /**
     * @var list<class-string<Throwable>>|null
     */
    private ?array $retryOnly = null;

    /**
     * Both forms of the fourth gate reduce to this, so nothing below knows which the
     * caller used: a RetryPolicy contributes shouldRetry(...), when: contributes itself.
     *
     * @var ?Closure(RetryContext): bool
     */
    private ?Closure $retryDecision = null;

    private ?int $reclaimStaleAfterSeconds = null;

    private ?bool $reclaimStaleEnabled = null;

    private ?int $compensationReclaimStaleAfterSeconds = null;

    private ?bool $compensationReclaimStaleEnabled = null;

    /**
     * @param  array<int, mixed>  $arguments
     */
    public function __construct(
        private readonly FlowRuntime $runtime,
        private readonly string $actionClass,
        private readonly array $arguments,
    ) {}

    /**
     * Register the compensation for this step: either a class (recommended for
     * durability) or a closure (serialized via SerializableClosure). For a class the
     * variadic arguments are passed to its handle(); a closure captures its own.
     */
    public function compensateWith(string|Closure $compensation, mixed ...$arguments): static
    {
        $this->compensation = $compensation instanceof Closure
            ? CompensationDefinition::forClosure($compensation)
            : CompensationDefinition::forClass($compensation, array_values($arguments));

        return $this;
    }

    public function onCompensationFailure(CompensationFailurePolicy $policy): static
    {
        $this->actionCompensationFailurePolicy = $policy;

        return $this;
    }

    /**
     * Same idea as reclaimStaleAfter(), but for this step's registered compensation
     * (sagas.reclaim.stale_running): allow a compensation row still Running to be
     * reclaimed once it has sat this many seconds since started_at.
     */
    public function reclaimCompensationStaleAfter(int $seconds): static
    {
        if ($seconds < 0) {
            throw new InvalidArgumentException(
                "reclaimCompensationStaleAfter() seconds must be zero or greater, got {$seconds}.",
            );
        }

        $this->compensationReclaimStaleAfterSeconds = $seconds;

        return $this;
    }

    /**
     * Force this step's compensation stale-Running reclaim on or off, independently
     * of sagas.reclaim.stale_running.enabled. See enableStaleReclaim() for the same
     * idea applied to the step's own execution.
     */
    public function enableCompensationStaleReclaim(bool $enabled = true): static
    {
        $this->compensationReclaimStaleEnabled = $enabled;

        return $this;
    }

    /**
     * Make this an optional step: its failure does not fail the flow. The action
     * still respects its $tries; once retries are exhausted, it lands OptionalFailed
     * (an action.optional_failed event is recorded) and run() returns the fallback.
     */
    public function continueOnFailure(bool $continue = true): static
    {
        $this->continueOnFailure = $continue;

        return $this;
    }

    /**
     * Value returned by run() when an optional step gives up (defaults to null).
     */
    public function fallbackValueOnFail(mixed $value): static
    {
        $this->fallbackValueOnFail = $value;

        return $this;
    }

    /**
     * Set a wall-clock deadline for this step. If the monitor finds it still
     * pending/running past this instant it marks it Expired; on replay an
     * expired non-optional step fails the flow, an optional one returns its fallback.
     */
    public function expiresAt(?DateTimeInterface $expiresAt): static
    {
        $this->expiresAt = $expiresAt;

        return $this;
    }

    /**
     * Allow this step's own claim to reclaim a row still Running once it has sat
     * this many seconds since started_at — recognizing a worker that died
     * mid-execution. Also enables the mechanism for this step regardless of
     * actions.reclaim.stale_running.enabled. See enableStaleReclaim() to toggle the
     * mechanism without changing (or without knowing) the threshold.
     */
    public function reclaimStaleAfter(int $seconds): static
    {
        if ($seconds < 0) {
            throw new InvalidArgumentException(
                "reclaimStaleAfter() seconds must be zero or greater, got {$seconds}.",
            );
        }

        $this->reclaimStaleAfterSeconds = $seconds;

        return $this;
    }

    /**
     * Force this step's stale-Running reclaim on or off, independently of
     * actions.reclaim.stale_running.enabled — in either direction: turn it on for one
     * step while it stays off globally, or off for one step while it is on globally.
     * With $enabled = true and no threshold set via reclaimStaleAfter(), the step
     * uses config's after_seconds.
     */
    public function enableStaleReclaim(bool $enabled = true): static
    {
        $this->reclaimStaleEnabled = $enabled;

        return $this;
    }

    /**
     * Wait for a named signal instead of failing this step. When the step gives up
     * (its queue $tries are spent) the flow parks: the step lands AwaitingRetry, a
     * wait-signal is recorded, and the run goes Waiting. Delivering $signal re-runs
     * THIS step alone — the same (flow_run_id, sequence) ordinal, the same arguments,
     * no new sequence — so replay and every downstream step stay deterministic. A
     * failure of the retry parks again.
     *
     * $maxRetries caps the signal-gated cycles (null falls back to
     * actions.retry_on_signal.max_retries, and null there means unbounded);
     * $waitSeconds bounds ONE wait (null falls back to the configured default signal
     * timeout);
     * $only restricts the policy to the listed exception classes and their
     * subclasses (null reacts to every failure);
     * $when has the final say on a failure that passed $only, and returning false
     * from it ends the policy for that failure. Once the budget is spent, the wait
     * times out, the failure falls outside $only, or $when refuses it, the step
     * fails exactly as it would have without this policy.
     *
     * A RetryPolicy passed as $signal carries all four itself, and combining it with
     * any of them is refused. See RetryPolicy for which of its values survive a
     * deploy and which do not.
     *
     * @param  list<class-string<Throwable>>|null  $only
     * @param  ?Closure(RetryContext): bool  $when
     */
    public function retryOnSignal(
        RetryPolicy|string $signal,
        ?int $maxRetries = null,
        ?int $waitSeconds = null,
        ?array $only = null,
        ?Closure $when = null,
    ): static {
        if ($signal instanceof RetryPolicy) {
            $this->rejectPolicyWithArguments($maxRetries, $waitSeconds, $only, $when);

            // Unwrapped once, so the policy is asked as often as an argument list is
            // evaluated and every seam below goes on reading a plain string.
            $when = $signal->shouldRetry(...);
            $maxRetries = $signal->maxRetries();
            $waitSeconds = $signal->waitSeconds();
            $only = $signal->only();
            $signal = $signal->signal();
        }

        $this->rejectNegative('maxRetries', $maxRetries);
        $this->rejectNegative('waitSeconds', $waitSeconds);

        $this->retrySignal = $signal;
        $this->retryMaxRetries = $maxRetries;
        $this->retryWaitSeconds = $waitSeconds;
        $this->retryOnly = $only;
        $this->retryDecision = $when;

        return $this;
    }

    /**
     * Also compensate this step if the step itself fails (not only when a later step
     * fails). For non-atomic actions that may leave partial effects. The compensation
     * must be idempotent and tolerate "the step did nothing". Overrides the group and
     * config defaults.
     */
    public function compensateStepOnSelfFailure(bool $compensate = true): static
    {
        $this->compensateOnSelfFailure = $compensate;

        return $this;
    }

    /**
     * Internal: attach saga() group context to this step (called by SagaStepBuilder).
     * The step's own compensation/policy is set via the public methods above.
     */
    public function withSagaGroup(
        ?CompensationFailurePolicy $groupCompensationFailurePolicy,
        ?bool $groupCompensateOnSelfFailure,
        ?int $parallelGroupId,
    ): static {
        $this->groupCompensationFailurePolicy = $groupCompensationFailurePolicy;
        $this->groupCompensateOnSelfFailure = $groupCompensateOnSelfFailure;
        $this->parallelGroupId = $parallelGroupId;

        return $this;
    }

    /**
     * Resolve this step against stored history, or schedule/run it and suspend.
     *
     * @throws HistoryContractMismatchException
     * @throws Throwable
     */
    public function run(): mixed
    {
        $flowRun = $this->runtime->run();
        $sequence = $this->runtime->nextSequence();

        $existingStep = app(HistoryContractGuard::class)
            ->expectAction($flowRun->id, $sequence, $this->actionClass);

        if ($existingStep !== null) {
            return $this->resolve($existingStep, $sequence);
        }

        $suspender = app(FlowSuspender::class);

        // Compensation-only planning never starts new work: stop at the frontier.
        if ($this->runtime->isCollecting()) {
            $suspender->suspend('action', $sequence);
        }

        $dispatcher = app(ActionDispatcher::class);
        $schedule = $this->schedule();

        if ($this->runtime->mode() === RunMode::Sync) {
            $this->runInlineThenSuspend($dispatcher, $schedule, $flowRun, $sequence);
        }

        $dispatcher->dispatch($flowRun, $sequence, $schedule);

        $suspender->suspend('action', $sequence);
    }

    /**
     * Everything this builder has resolved about the step, in the shape the dispatcher
     * and the recorder consume. Both transports schedule from the same description.
     */
    private function schedule(): ActionSchedule
    {
        return new ActionSchedule(
            actionClass: $this->actionClass,
            arguments: $this->arguments,
            hasCompensation: $this->compensation !== null,
            continueOnFailure: $this->resolvedContinueOnFailure(),
            expiresAt: $this->resolvedExpiresAt(),
            actionName: $this->resolvedActionName(),
            retrySignal: $this->retrySignal,
            // Only a step that carries the policy gets a ceiling written. Storing the
            // global default on every action would leave an unrelated number in the
            // column, and awaitRetry()'s ??= would then keep it instead of the ceiling
            // the seam actually parked on when a later deploy adds retryOnSignal().
            retrySignalMaxAttempts: $this->retrySignal === null ? null : $this->resolvedMaxRetries(),
            reclaimStaleAfterSeconds: $this->reclaimStaleAfterSeconds,
            reclaimStaleEnabled: $this->reclaimStaleEnabled,
        );
    }

    /**
     * Sync mode: run the step in this process, then replay. Never returns — every path
     * out either suspends or rethrows.
     *
     * @throws FlowSuspended
     * @throws Throwable
     */
    private function runInlineThenSuspend(
        ActionDispatcher $dispatcher,
        ActionSchedule $schedule,
        FlowRun $flowRun,
        int $sequence,
    ): never {
        try {
            // The result needs no branching: a step that ran and one that was
            // superseded mid-run both resolve on the replay below, from whatever the
            // row ended up holding.
            $dispatcher->runInline($flowRun, $sequence, $schedule);
        } catch (ActionClaimFailedException $exception) {
            // A broken invariant, not a retryable business failure — nothing was
            // recorded on the row for the checks below to make sense of.
            throw $exception;
        } catch (Throwable $exception) {
            $this->settleInlineFailure($exception, $schedule, $flowRun, $sequence);
        }

        app(FlowSuspender::class)->suspendInline('action', $sequence);
    }

    /**
     * Decide what an inline throw means, in the order the outcomes exclude each other.
     * Never returns: it either suspends so replay resolves the step, or rethrows.
     *
     * @throws FlowSuspended
     * @throws Throwable
     */
    private function settleInlineFailure(
        Throwable $exception,
        ActionSchedule $schedule,
        FlowRun $flowRun,
        int $sequence,
    ): never {
        $suspender = app(FlowSuspender::class);

        // A step with a retry policy is resolved solely on replay: only the seam knows
        // the only-filter and the budget. Replay so it sees the Failed row and decides
        // whether to park or to give up — but only when the row really did fail. A
        // throw from before that (a listener, an observer, an action class that will
        // not resolve) leaves nothing for the seam to read, and replaying would suspend
        // a sync run on a job that does not exist.
        if ($this->retrySignal !== null && $this->recordedFailure($flowRun->id, $sequence)) {
            $suspender->suspendInline('action', $sequence);
        }

        // Optional step: no retries inline, so give up now — mark it OptionalFailed and
        // replay so the seam resolves the fallback.
        if ($schedule->continueOnFailure) {
            $this->markOptionalFailed($flowRun->id, $sequence);

            $suspender->suspendInline('action', $sequence);
        }

        $this->registerFailedStepCompensation($flowRun->id, $sequence);

        throw $exception;
    }

    /**
     * @throws FlowSuspended
     * @throws Throwable
     */
    private function resolve(ActionRun $step, int $sequence): mixed
    {
        switch ($step->status) {
            case ActionStatus::Completed:
                return $this->resolveCompleted($step, $sequence);
            case ActionStatus::AwaitingRetry:
                return $this->resolveAwaitingRetry($step, $sequence);
            case ActionStatus::OptionalFailed:
                // Terminal, even when the workflow now carries a policy, the row was
                // scheduled without: the give-up hook has already published
                // action.optional_failed. A row that carries the policy never lands
                // here until the seam itself ends the retries.
                return $this->resolveOptionalFailed($step, $sequence);
            case ActionStatus::Expired:
                // Monitor-enforced expiry. An optional step gives up gracefully
                // (fallback); a required one surfaces the expiry as a business error.
                if ($this->resolvedContinueOnFailure()) {
                    return $this->resolveOptionalFailed($step, $sequence);
                }

                $this->resolveExpired($step, $sequence);
                // no break — resolveExpired never returns.
            case ActionStatus::Failed:
                // The queue still owes this step attempts of its own: let them play
                // out before a retry cycle is spent on what may be transient. Without
                // this a signal could park — and re-run — a step still in flight.
                if ($this->carriesRetryPolicy($step) && $this->queueRetriesRemain($step)) {
                    app(FlowSuspender::class)->suspend('action', $sequence);
                }

                try {
                    if ($this->shouldRetryOnSignal($step)) {
                        $this->parkForRetry($step, $sequence);
                    }
                } catch (RetryPolicyReentryException $reentry) {
                    // The predicate is broken, but the step under it really did fail,
                    // and the rollback is built from this pass alone — so without this
                    // the one thing the run forgets is the compensation the caller
                    // asked for on this very step.
                    if ($this->shouldCompensateFailedStep()) {
                        $this->pushCompensation($step->id, $sequence);
                    }

                    throw $reentry;
                }

                // An optional step still has retries left: it is not yet
                // OptionalFailed, so wait rather than surface a business error.
                if ($this->resolvedContinueOnFailure()) {
                    if ($this->retrySignal !== null) {
                        return $this->giveUpAfterRetry($step, $sequence);
                    }

                    // The hook leaves a step carrying a policy for the seam to
                    // settle. If a deploy removed retryOnSignal() before the queue ran
                    // out, nothing will write OptionalFailed and no job is left.
                    if ($step->retry_signal !== null && $step->queue_attempts_exhausted) {
                        return $this->giveUpAfterRetry($step, $sequence);
                    }

                    app(FlowSuspender::class)->suspend('action', $sequence);
                }

                $this->resolveFailed($step, $sequence);
                // Still in flight (queued job not finished): suspend until resumed.
            default:
                app(FlowSuspender::class)->suspend('action', $sequence);
        }
    }

    /**
     * Replay a completed step: register its compensation (now known to have
     * succeeded) and return the stored result.
     */
    private function resolveCompleted(ActionRun $step, int $sequence): mixed
    {
        $this->pushCompensation($step->id, $sequence);

        return app(Serializer::class)->deserialize($step->result);
    }

    /**
     * Replay a failed step: optionally register its compensation (opt-in, for
     * non-atomic steps) then surface the failure as a business error.
     */
    private function resolveFailed(ActionRun $step, int $sequence): never
    {
        if ($this->shouldCompensateFailedStep()) {
            $this->pushCompensation($step->id, $sequence);
        }

        throw ActionFailedException::forAction($this->actionClass, $sequence, $this->failureMessage($step));
    }

    /**
     * Replay a required step the monitor expired: optionally register its
     * compensation (opt-in, for non-atomic steps) then surface the expiry as a
     * business error so the flow fails and rolls back.
     */
    private function resolveExpired(ActionRun $step, int $sequence): never
    {
        if ($this->shouldCompensateFailedStep()) {
            $this->pushCompensation($step->id, $sequence);
        }

        throw FlowExpiredException::forAction($this->actionClass, $sequence);
    }

    /**
     * Replay an optional step that gave up: register its compensation if opted in
     * (the step may have left partial effects), then return the fallback so the
     * workflow carries on as if the step had not happened.
     */
    private function resolveOptionalFailed(ActionRun $step, int $sequence): mixed
    {
        if ($this->shouldCompensateFailedStep()) {
            $this->pushCompensation($step->id, $sequence);
        }

        return $this->fallbackValueOnFail;
    }

    /**
     * Replay a step parked on its retry signal: retry it when the signal landed,
     * give up when the wait timed out, keep waiting otherwise.
     *
     * @throws FlowSuspended
     * @throws Throwable
     */
    private function resolveAwaitingRetry(ActionRun $step, int $sequence): mixed
    {
        $suspender = app(FlowSuspender::class);

        // Compensation-only planning never restarts work and never mutates: the
        // parked step is the live frontier, so stop here.
        if ($this->runtime->isCollecting()) {
            $suspender->suspend('action', $sequence);
        }

        $signal = app(SignalRepository::class)
            ->latestForSequence($this->runtime->run()->id, $sequence);

        // No wait-signal to resolve (history repaired or pruned): keep waiting
        // rather than invent a retry the operator never asked for.
        if ($signal === null) {
            $suspender->suspend('action', $sequence);
        }

        // The monitor timed the wait out: fail the way this step would have failed
        // if it had never carried a retry policy.
        if ($signal->status === SignalStatus::TimedOut) {
            return $this->giveUpAfterRetry($step, $sequence);
        }

        // Delivered while we were parked: bind it to this ordinal and retry.
        if ($signal->status === SignalStatus::Received) {
            $this->consumeAndRetry($signal, $step, $sequence);
        }

        // Bound by a pass that died before the retry landed: the wait is over.
        if ($signal->status === SignalStatus::Consumed) {
            $this->retryNow($step, $sequence);
        }

        // Still Waiting — but a signal delivered while this seam was looking for one
        // lands as a floating row that deliver() never matched against the wait-signal.
        // Take it here so the very first signal is not swallowed.
        $delivered = app(SignalRepository::class)
            ->earliestPendingSince($this->runtime->run()->id, $signal->name, $step->finished_at);

        // Close the spent wait-signal and bind the floating row to this ordinal, so no
        // later await consumes the same signal twice. A lost claim means a delivery
        // landed in the wait-signal itself, which the next replay resolves.
        if ($delivered !== null && $this->handOverDelivery($signal, $delivered, $sequence)) {
            $this->retryNow($step, $sequence);
        }

        $suspender->suspend('action', $sequence);
    }

    /**
     * Park a failed step on its retry signal and suspend the flow. Never returns:
     * either the retry starts immediately (a signal was already delivered) or the
     * flow goes Waiting until one is.
     *
     * @throws FlowSuspended
     * @throws Throwable
     */
    private function parkForRetry(ActionRun $step, int $sequence): never
    {
        $suspender = app(FlowSuspender::class);

        // Compensation-only planning never starts new work: stop at the frontier.
        if ($this->runtime->isCollecting()) {
            $suspender->suspend('action', $sequence);
        }

        /** @var string $signal */
        $signal = $this->retrySignal;

        $flowRun = $this->runtime->run();

        // The signal may have been delivered in the window between this attempt's
        // failure being written and this replay parking. Take it only if it arrived
        // after the attempt finished — an older floating signal belongs elsewhere.
        $delivered = app(SignalRepository::class)
            ->earliestPendingSince($flowRun->id, $signal, $step->finished_at);

        if ($delivered !== null) {
            $this->consumeAndRetry($delivered, $step, $sequence);
        }

        // Adopt a signal an earlier pass left open here instead of recording a second
        // one: delivery fulfils the OLDEST open row for a name while
        // resolveAwaitingRetry() reads the NEWEST, so two would strand the run.
        $abandoned = app(SignalRepository::class)->latestForSequence($flowRun->id, $sequence);

        if ($abandoned !== null && $abandoned->name === $signal) {
            // Delivered into that row before this replay reached it: acknowledged,
            // and it belongs to this park, so spend it rather than lose it.
            if ($abandoned->status === SignalStatus::Received) {
                $this->consumeAndRetry($abandoned, $step, $sequence);
            }

            if ($abandoned->status === SignalStatus::Waiting) {
                $this->park($step, $signal, $sequence, record: false);
            }
        }

        $this->park($step, $signal, $sequence);
    }

    /**
     * Write the parking — the wait-signal at this ordinal and the step's transition to
     * AwaitingRetry — in one transaction, so a process that dies mid-parking leaves
     * either all of it or none. Suspending stays outside: it throws, and a throw inside
     * would roll the parking back.
     *
     * $record is false when an open signal from an earlier pass was adopted.
     *
     * @throws FlowSuspended
     * @throws Throwable
     */
    private function park(ActionRun $step, string $signal, int $sequence, bool $record = true): never
    {
        $this->connection()->transaction(function () use ($step, $signal, $sequence, $record): void {
            if ($record) {
                app(SignalRecorder::class)->recordSignalWaiting(
                    $this->runtime->run(),
                    $signal,
                    $sequence,
                    $this->retryWaitSeconds === null ? null : now()->addSeconds($this->retryWaitSeconds),
                );
            }

            app(ActionRecorder::class)->awaitRetry($step, $signal, $this->budgetFor($step));
        });

        app(FlowSuspender::class)->suspend('action', $sequence);
    }

    /**
     * Close a spent wait-signal and claim the floating delivery that ended its wait,
     * atomically. Returns false when the signal is no longer Waiting: it received a
     * delivery of its own, which the next replay resolves.
     *
     * @throws Throwable
     */
    private function handOverDelivery(FlowSignal $signal, FlowSignal $delivered, int $sequence): bool
    {
        $recorder = app(SignalRecorder::class);
        $flowRun = $this->runtime->run();

        return (bool) $this->connection()
            ->transaction(function () use ($recorder, $flowRun, $signal, $delivered, $sequence): bool {
                if (! $recorder->consumeWhileWaiting($flowRun, $signal, $sequence)) {
                    return false;
                }

                $recorder->consumeSignal($flowRun, $delivered, $sequence);

                return true;
            });
    }

    /**
     * @throws FlowSuspended
     * @throws Throwable
     */
    private function consumeAndRetry(FlowSignal $signal, ActionRun $step, int $sequence): never
    {
        // Spending the signal and spending the cycle it pays for are one transition:
        // a crash between them would leave the signal Consumed and the step Failed,
        // and the next replay — which only looks for an unconsumed signal — would park
        // again for a delivery nobody owes. Starting the step stays outside.
        $this->connection()->transaction(function () use ($signal, $step, $sequence): void {
            app(SignalRecorder::class)->consumeSignal($this->runtime->run(), $signal, $sequence);

            app(ActionRecorder::class)->retryAction($step, $this->resolvedExpiresAt());
        });

        $this->startRetriedStep($step, $sequence);
    }

    /**
     * The package's own connection, for the writes of the retry protocol that have to
     * land together or not at all.
     */
    private function connection(): ConnectionInterface
    {
        return DB::connection(config('saga-lara-flow.database.connection') ?: null);
    }

    /**
     * Spend one retry cycle and run the step again at its own ordinal: rewind the row
     * to Pending, then start it. Used where the signal that pays for the cycle was
     * already consumed by an earlier pass.
     *
     * @throws FlowSuspended
     */
    private function retryNow(ActionRun $step, int $sequence): never
    {
        app(ActionRecorder::class)->retryAction($step, $this->resolvedExpiresAt());

        $this->startRetriedStep($step, $sequence);
    }

    /**
     * Run a step the seam has just rewound to Pending: inline (sync) or as a fresh
     * job (queued). Either way the flow replays from the top afterwards, so the next
     * pass decides what the new outcome means.
     *
     * @throws FlowSuspended
     */
    private function startRetriedStep(ActionRun $step, int $sequence): never
    {
        $suspender = app(FlowSuspender::class);
        $dispatcher = app(ActionDispatcher::class);

        if ($this->runtime->mode() === RunMode::Sync) {
            try {
                // retryAction() rewound this exact row to Pending in the same call
                // chain, so nothing else can have taken it before we did: losing the
                // claim here is a broken invariant. Being superseded afterwards is not
                // — the monitor and the doctor act on this row from their own
                // processes — and the replay below resolves whatever they left.
                if ($dispatcher->execute($step) === StepExecution::ClaimLost) {
                    throw ActionClaimFailedException::forAction($step->action_class, $sequence);
                }
            } catch (ActionClaimFailedException $exception) {
                throw $exception;
            } catch (Throwable) {
                // The failure is already persisted on the row; the replay below is
                // what decides whether to park again or to give up.
            }

            $suspender->suspendInline('action', $sequence);
        }

        $dispatcher->redispatch($step);

        $suspender->suspend('action', $sequence);
    }

    /**
     * The retry policy is over (budget spent, wait timed out, or the failure fell
     * outside only): resolve the step exactly as it would have resolved without the
     * policy. An optional step is marked OptionalFailed here — the queued give-up
     * hook deliberately leaves that to the seam for a step with a retry signal —
     * unless this pass is only collecting compensations, which writes nothing.
     */
    private function giveUpAfterRetry(ActionRun $step, int $sequence): mixed
    {
        // Suspending here instead would be the obvious guard and the wrong one: the
        // replay would stop at this step, and every compensation after it — the whole
        // point of collecting — would be missing from the rollback. So the give-up is
        // resolved from the row as it stands and written by the next ordinary replay.
        $collecting = $this->runtime->isCollecting();

        if (! $this->resolvedContinueOnFailure()) {
            // A timed-out wait arrives here on an AwaitingRetry row (the other ways
            // in are already Failed); leaving it would show a compensated run still
            // holding a step that claims to be waiting.
            if (! $collecting) {
                app(ActionRecorder::class)->settleAwaitingRetry($step);
            }

            $this->resolveFailed($step, $sequence);
        }

        if (! $collecting && $step->status !== ActionStatus::OptionalFailed) {
            app(ActionRecorder::class)->optionalFail($step);
        }

        return $this->resolveOptionalFailed($step, $sequence);
    }

    /**
     * Whether this replay pass should park the failed step for a signal-gated retry.
     * The seam decides alone: the only-filter is never persisted, and the budget and
     * the wait are read from the row.
     *
     * @throws InternalFlowControl
     */
    private function shouldRetryOnSignal(ActionRun $step): bool
    {
        if ($this->retrySignal === null) {
            return false;
        }

        $maxRetries = $this->budgetFor($step);

        if ($maxRetries !== null && $step->retry_signal_attempts >= $maxRetries) {
            return false;
        }

        // A wait that timed out ends the policy, budget or no budget: the deadline
        // bounds the waiting, not one wait out of many. Without this a step whose
        // signal never comes would be handed a fresh wait on every replay.
        if ($this->waitTimedOut($step)) {
            return false;
        }

        if (! $this->matchesOnly($step)) {
            return false;
        }

        return $this->policyAllows($step, $maxRetries);
    }

    /**
     * The last gate, and the only one that runs the caller's own code — hence last,
     * after three structural checks that cost a column read.
     *
     * A throw is absorbed as "do not park", the outcome the caller was already
     * prepared for; letting it out would fail the whole run on one path and truncate
     * the compensation stack in silence on the other. It is logged rather than
     * swallowed: a policy that never parks looks identical to one that always throws.
     *
     * @throws InternalFlowControl
     */
    private function policyAllows(ActionRun $step, ?int $maxRetries): bool
    {
        $decide = $this->retryDecision;

        if ($decide === null) {
            return true;
        }

        // Compensation-only planning stops at this step either way, so the answer
        // would change nothing — and it runs caller code, which that pass must not.
        if ($this->runtime->isCollecting()) {
            return true;
        }

        $flowRun = $this->runtime->run();

        // Outside the guard: that absorbs a defect in the caller's predicate, and
        // reading our own row is not one. A throw here is ours and should surface.
        $context = new RetryContext(
            runId: $flowRun->id,
            workflowClass: $flowRun->workflow_class,
            actionClass: $step->action_class,
            sequence: $step->sequence,
            signal: (string) $this->retrySignal,
            cyclesSpent: $step->retry_signal_attempts,
            cap: $maxRetries,
            executions: $step->attempts,
            failure: RecordedFailure::fromRecord($step->exception),
        );

        $this->runtime->beginDeciding();

        try {
            return $decide($context);
        } catch (InternalFlowControl|HistoryContractMismatchException|RetryPolicyReentryException $control) {
            // Not answers, and none has a safe reading: the engine suspends with the
            // first, reports the second on its own terms, and the third is a workflow
            // the caller has to fix, not a step that quietly never parks.
            throw $control;
        } catch (Throwable $exception) {
            app(AnomalyLog::class)->log(AnomalyLog::REASON_RETRY_POLICY_THREW, [
                'flow_run_id' => $flowRun->id,
                'action_run_id' => $step->id,
                'sequence' => $step->sequence,
                'signal' => $this->retrySignal,
                'exception' => $exception::class,
                'message' => $exception->getMessage(),
            ]);

            return false;
        } finally {
            $this->runtime->endDeciding();
        }
    }

    /**
     * Whether a retry policy applies at all — the one the workflow asks for now, or
     * the one the row was scheduled with. The row has to count, or removing
     * retryOnSignal() would let an early replay finally fail a step the queue is
     * still retrying.
     */
    private function carriesRetryPolicy(ActionRun $step): bool
    {
        return $this->retrySignal !== null || $step->retry_signal !== null;
    }

    /**
     * The budget this step is held to. The row carries the cap resolved when it was
     * scheduled, and that is what the events and saga-flow:show report, so that is
     * what replay enforces — otherwise a config change would silently move a cap the
     * operator is still being shown. A row with no policy yet falls back to the
     * resolved value.
     */
    private function budgetFor(ActionRun $step): ?int
    {
        return $step->retry_signal === null
            ? $this->resolvedMaxRetries()
            : $step->retry_signal_max_attempts;
    }

    /**
     * Whether the wait at this step's ordinal has run out of time. The newest row
     * there is the current wait: a spent cycle leaves it Consumed, and only the
     * monitor writes TimedOut.
     */
    private function waitTimedOut(ActionRun $step): bool
    {
        $signal = app(SignalRepository::class)
            ->latestForSequence($this->runtime->run()->id, $step->sequence);

        return $signal?->status === SignalStatus::TimedOut;
    }

    /**
     * Whether Laravel's own queue retries are still outstanding. Read off the row,
     * where the queue's failure hook wrote it, rather than re-derived from the
     * action's $tries: that value lives in code and can change under a job already in
     * flight. Sync mode has no queue behind it.
     */
    private function queueRetriesRemain(ActionRun $step): bool
    {
        if ($this->runtime->mode() === RunMode::Sync) {
            return false;
        }

        return ! $step->queue_attempts_exhausted;
    }

    /**
     * Resolve the retry budget: an explicit maxRetries: wins, then the configured
     * global cap, then null — unbounded, with the wait timeout and the run's own
     * expires_at as the remaining brakes.
     */
    private function resolvedMaxRetries(): ?int
    {
        if ($this->retryMaxRetries !== null) {
            return $this->retryMaxRetries;
        }

        $configured = config('saga-lara-flow.actions.retry_on_signal.max_retries');

        if ($configured === null) {
            return null;
        }

        $this->rejectNegative('actions.retry_on_signal.max_retries', (int) $configured);

        return (int) $configured;
    }

    /**
     * Reject a negative budget or wait before it can be persisted. The columns are
     * unsigned, so a negative value means an error on MySQL and a step that silently
     * never parks on the drivers that store it; failing here says which value is
     * wrong, the same way on every driver.
     */
    private function rejectNegative(string $name, ?int $value): void
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
    private function rejectPolicyWithArguments(
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

    /**
     * Whether the recorded failure falls inside the only: filter. Subclasses count
     * (is_a with allow_string), a null filter accepts everything, and a failure with
     * no recorded class is never retried under an explicit filter.
     */
    private function matchesOnly(ActionRun $step): bool
    {
        if ($this->retryOnly === null) {
            return true;
        }

        $failure = $step->exception['class'] ?? null;

        if (! is_string($failure)) {
            return false;
        }

        return array_any(
            $this->retryOnly,
            fn (string $candidate): bool => is_a($failure, $candidate, allow_string: true),
        );
    }

    /**
     * Sync path: an optional inline action threw. Mark its just-persisted Failed row
     * (looked up by its (flow_run_id, sequence) identity) as OptionalFailed so the
     * replay resolves the fallback instead of a business error.
     */
    /**
     * Whether the step at this ordinal is recorded as Failed — the state the seam
     * needs to decide anything. A missing row counts as no failure.
     */
    private function recordedFailure(string $flowRunId, int $sequence): bool
    {
        return app(ActionRunRepository::class)->find($flowRunId, $sequence)?->status === ActionStatus::Failed;
    }

    private function markOptionalFailed(string $flowRunId, int $sequence): void
    {
        $step = app(ActionRunRepository::class)->find($flowRunId, $sequence);

        if ($step !== null) {
            app(ActionRecorder::class)->optionalFail($step);
        }
    }

    /**
     * Sync path: the inline action threw. Register its compensation if opted in,
     * looking the just-persisted Failed row up by its (flow_run_id, sequence) identity.
     */
    private function registerFailedStepCompensation(string $flowRunId, int $sequence): void
    {
        if (! $this->shouldCompensateFailedStep()) {
            return;
        }

        $step = app(ActionRunRepository::class)->find($flowRunId, $sequence);

        if ($step !== null) {
            $this->pushCompensation($step->id, $sequence);
        }
    }

    private function pushCompensation(string $actionRunId, int $sequence): void
    {
        if ($this->compensation === null) {
            return;
        }

        $this->runtime->sagaStack()->push(new CompensationEntry(
            $actionRunId,
            $sequence,
            $this->compensation,
            $this->actionCompensationFailurePolicy,
            $this->groupCompensationFailurePolicy,
            $this->parallelGroupId,
            $this->compensationReclaimStaleAfterSeconds,
            $this->compensationReclaimStaleEnabled,
        ));
    }

    private function shouldCompensateFailedStep(): bool
    {
        return $this->compensateOnSelfFailure
            ?? $this->groupCompensateOnSelfFailure
            ?? (bool) config('saga-lara-flow.sagas.compensate_failed_step');
    }

    /**
     * Resolve whether this is an optional step: an explicit ->continueOnFailure()
     * wins; otherwise fall back to the action's #[ContinueOnFailure] attribute,
     * then to required (precedence: explicit call > attribute).
     */
    private function resolvedContinueOnFailure(): bool
    {
        return $this->continueOnFailure
            ?? app(AttributeReader::class)->action($this->actionClass)->continueOnFailure
            ?? false;
    }

    /**
     * Resolve the step's wall-clock deadline: an explicit ->expiresAt() wins;
     * otherwise the action's #[ActionTimeout] seconds from now (precedence:
     * explicit call > attribute). The recorder still applies the config default
     * when this is null.
     */
    private function resolvedExpiresAt(): ?DateTimeInterface
    {
        if ($this->expiresAt !== null) {
            return $this->expiresAt;
        }

        $seconds = app(AttributeReader::class)->action($this->actionClass)->timeoutSeconds;

        return $seconds === null ? null : now()->addSeconds($seconds);
    }

    /**
     * Resolve the step's display name from its #[ActionName] attribute (null when
     * absent — the row then falls back to the class basename for display).
     */
    private function resolvedActionName(): ?string
    {
        return app(AttributeReader::class)->action($this->actionClass)->name;
    }

    private function failureMessage(ActionRun $step): string
    {
        $exception = $step->exception;

        if (is_array($exception) && isset($exception['message']) && is_string($exception['message'])) {
            return $exception['message'];
        }

        return 'unknown error';
    }
}
