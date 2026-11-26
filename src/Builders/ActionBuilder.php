<?php

namespace DiscoveryUkraine\SagaLaraFlow\Builders;

use Closure;
use DateTimeInterface;
use DiscoveryUkraine\SagaLaraFlow\Contracts\ActionRunRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Contracts\SignalRepository;
use DiscoveryUkraine\SagaLaraFlow\Data\ActionSchedule;
use DiscoveryUkraine\SagaLaraFlow\Data\CompensationDefinition;
use DiscoveryUkraine\SagaLaraFlow\Data\SignalRetry;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationFailurePolicy;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Enums\SignalStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\StepExecution;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ActionClaimFailedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ActionFailedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowExpiredException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\HistoryContractMismatchException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\FencedWriteLost;
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
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\DB;
use InvalidArgumentException;
use Throwable;

/**
 * Constructed by the engine and reachable for subclassing only through Workflow::action(): its
 * signatures are internal.
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

    private ?SignalRetry $retry = null;

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
     * A class receives the variadic arguments in its handle(); a closure captures its own.
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
     * reclaimStaleAfter() for this step's compensation.
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
     * enableStaleReclaim() for this step's compensation.
     */
    public function enableCompensationStaleReclaim(bool $enabled = true): static
    {
        $this->compensationReclaimStaleEnabled = $enabled;

        return $this;
    }

    /**
     * Once its $tries are spent the step lands OptionalFailed and run() returns the fallback.
     */
    public function continueOnFailure(bool $continue = true): static
    {
        $this->continueOnFailure = $continue;

        return $this;
    }

    public function fallbackValueOnFail(mixed $value): static
    {
        $this->fallbackValueOnFail = $value;

        return $this;
    }

    public function expiresAt(?DateTimeInterface $expiresAt): static
    {
        $this->expiresAt = $expiresAt;

        return $this;
    }

    /**
     * Reclaim a row still Running this many seconds after started_at; enables the mechanism too.
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
     * Overrides actions.reclaim.stale_running.enabled in either direction for this step.
     */
    public function enableStaleReclaim(bool $enabled = true): static
    {
        $this->reclaimStaleEnabled = $enabled;

        return $this;
    }

    /**
     * Park the step on $signal instead of failing it; a RetryPolicy passed as $signal carries the rest.
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
        $this->retry = SignalRetry::for($signal, $maxRetries, $waitSeconds, $only, $when);

        return $this;
    }

    /**
     * The compensation must tolerate a step that did nothing.
     */
    public function compensateStepOnSelfFailure(bool $compensate = true): static
    {
        $this->compensateOnSelfFailure = $compensate;

        return $this;
    }

    /**
     * @internal
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

    private function schedule(): ActionSchedule
    {
        return new ActionSchedule(
            actionClass: $this->actionClass,
            arguments: $this->arguments,
            hasCompensation: $this->compensation !== null,
            continueOnFailure: $this->resolvedContinueOnFailure(),
            expiresAt: $this->resolvedExpiresAt(),
            actionName: $this->resolvedActionName(),
            retrySignal: $this->retry?->signal,
            // Only with a policy: awaitRetry()'s ??= would otherwise keep an unrelated global default.
            retrySignalMaxAttempts: $this->retry?->resolvedMaxRetries(),
            reclaimStaleAfterSeconds: $this->reclaimStaleAfterSeconds,
            reclaimStaleEnabled: $this->reclaimStaleEnabled,
        );
    }

    /**
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
            $dispatcher->runInline($flowRun, $sequence, $schedule);
        } catch (ActionClaimFailedException $exception) {
            // A broken invariant, not a business failure: nothing is on the row to resolve.
            throw $exception;
        } catch (Throwable $exception) {
            $this->settleInlineFailure($exception, $schedule, $flowRun, $sequence);
        }

        app(FlowSuspender::class)->suspendInline('action', $sequence);
    }

    /**
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

        // Only a recorded failure goes to the seam: a throw from before it (a listener, an action
        // class that will not resolve) would suspend a sync run on a job that does not exist.
        if ($this->retry !== null && $this->recordedFailure($flowRun->id, $sequence)) {
            $suspender->suspendInline('action', $sequence);
        }

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
                // Terminal even if a later deploy added a policy: action.optional_failed is published.
                return $this->resolveOptionalFailed($step, $sequence);
            case ActionStatus::Expired:
                if ($this->resolvedContinueOnFailure()) {
                    return $this->resolveOptionalFailed($step, $sequence);
                }

                $this->resolveExpired($step, $sequence);
                // no break — resolveExpired never returns.
            case ActionStatus::Failed:
                // The queue's own attempts play out first, or a signal could re-run a step in flight.
                if ($this->carriesRetryPolicy($step) && $this->queueRetriesRemain($step)) {
                    app(FlowSuspender::class)->suspend('action', $sequence);
                }

                try {
                    if ($this->shouldRetryOnSignal($step)) {
                        $this->parkForRetry($step, $sequence);
                    }
                } catch (RetryPolicyReentryException $reentry) {
                    // A broken predicate still leaves a failed step, and the rollback is built from this pass.
                    if ($this->shouldCompensateFailedStep()) {
                        $this->pushCompensation($step->id, $sequence);
                    }

                    throw $reentry;
                }

                if ($this->resolvedContinueOnFailure()) {
                    if ($this->retry !== null) {
                        return $this->giveUpAfterRetry($step, $sequence);
                    }

                    // A deploy removed retryOnSignal() after the hook left the step to the seam.
                    if ($step->retry_signal !== null && $step->queue_attempts_exhausted) {
                        return $this->giveUpAfterRetry($step, $sequence);
                    }

                    app(FlowSuspender::class)->suspend('action', $sequence);
                }

                $this->resolveFailed($step, $sequence);
            default:
                app(FlowSuspender::class)->suspend('action', $sequence);
        }
    }

    private function resolveCompleted(ActionRun $step, int $sequence): mixed
    {
        $this->pushCompensation($step->id, $sequence);

        return app(Serializer::class)->deserialize($step->result);
    }

    /**
     * @throws Throwable
     */
    private function resolveFailed(ActionRun $step, int $sequence): never
    {
        if ($this->shouldCompensateFailedStep()) {
            $this->pushCompensation($step->id, $sequence);
        }

        throw $this->runtime->raising(
            ActionFailedException::forAction($this->actionClass, $sequence, $this->failureMessage($step)),
        );
    }

    /**
     * @throws Throwable
     */
    private function resolveExpired(ActionRun $step, int $sequence): never
    {
        if ($this->shouldCompensateFailedStep()) {
            $this->pushCompensation($step->id, $sequence);
        }

        throw $this->runtime->raising(FlowExpiredException::forAction($this->actionClass, $sequence));
    }

    private function resolveOptionalFailed(ActionRun $step, int $sequence): mixed
    {
        if ($this->shouldCompensateFailedStep()) {
            $this->pushCompensation($step->id, $sequence);
        }

        return $this->fallbackValueOnFail;
    }

    /**
     * @throws FlowSuspended
     * @throws Throwable
     */
    private function resolveAwaitingRetry(ActionRun $step, int $sequence): mixed
    {
        $suspender = app(FlowSuspender::class);

        // The parked step is the frontier.
        if ($this->runtime->isCollecting()) {
            $suspender->suspend('action', $sequence);
        }

        $signal = app(SignalRepository::class)
            ->latestForSequence($this->runtime->run()->id, $sequence);

        // History repaired or pruned: keep waiting rather than invent a retry.
        if ($signal === null) {
            $suspender->suspend('action', $sequence);
        }

        if ($signal->status === SignalStatus::TimedOut) {
            return $this->giveUpAfterRetry($step, $sequence);
        }

        if ($signal->status === SignalStatus::Received) {
            $this->consumeAndRetry($signal, $step, $sequence);
        }

        if ($signal->status === SignalStatus::Consumed) {
            $this->retryNow($step, $sequence);
        }

        // A delivery that raced the parking floats instead of matching the wait-signal.
        $delivered = app(SignalRepository::class)
            ->earliestPendingSince($this->runtime->run()->id, $signal->name, $step->finished_at);

        if ($delivered !== null
            && $this->handOverAndRetry($signal, $delivered, $step, $sequence)
            && $this->retrySurvivedCommit($step)) {
            app(ActionRecorder::class)->publishRetried($step);

            $this->startRetriedStep($step, $sequence);
        }

        $suspender->suspend('action', $sequence);
    }

    /**
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

        /** @var SignalRetry $retry */
        $retry = $this->retry;

        $signal = $retry->signal;

        $flowRun = $this->runtime->run();

        // A floating signal older than the failed attempt belongs to something else.
        $delivered = app(SignalRepository::class)
            ->earliestPendingSince($flowRun->id, $signal, $step->finished_at);

        if ($delivered !== null) {
            $this->consumeAndRetry($delivered, $step, $sequence);
        }

        // Adopted, not recorded twice: delivery fills the oldest open row, resolution reads the newest.
        $abandoned = app(SignalRepository::class)->latestForSequence($flowRun->id, $sequence);

        if ($abandoned !== null && $abandoned->name === $signal) {
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
     * Suspending stays outside the transaction: it throws, and would roll the parking back.
     *
     * @throws FlowSuspended
     * @throws Throwable
     */
    private function park(ActionRun $step, string $signal, int $sequence, bool $record = true): never
    {
        try {
            $parking = $this->connection()->transaction(function () use ($step, $signal, $sequence, $record): Model {
                if ($record) {
                    app(SignalRecorder::class)->recordSignalWaiting(
                        $this->runtime->run(),
                        $signal,
                        $sequence,
                        $this->retry?->waitDeadline(),
                    );
                }

                // A refused park takes its wait-signal back: left standing, it would be the newest open
                // wait for its name and swallow the next delivery.
                return app(ActionRecorder::class)->awaitRetry($step, $signal, $this->budgetFor($step))
                    ?? throw new FencedWriteLost;
            });
        } catch (FencedWriteLost) {
            app(FlowSuspender::class)->suspend('action', $sequence);
        }

        if ($this->parkSurvivedCommit($step, $parking)) {
            app(ActionRecorder::class)->publishAwaitingRetry($step, $signal);
        }

        app(FlowSuspender::class)->suspend('action', $sequence);
    }

    /**
     * The witness is the parking's own history row, never changed after, read past model scopes.
     */
    private function parkSurvivedCommit(ActionRun $step, Model $parking): bool
    {
        $parked = $parking->newQueryWithoutScopes()
            ->useWritePdo()
            ->whereKey($parking->getKey())
            ->exists();

        if (! $parked) {
            app(AnomalyLog::class)->log(AnomalyLog::REASON_CLAIM_NOT_COMMITTED, [
                'entity' => 'action',
                'site' => 'await_retry',
                'flow_run_id' => $step->flow_run_id,
                'action_run_id' => $step->id,
                'sequence' => $step->sequence,
                'action_class' => $step->action_class,
            ]);
        }

        return $parked;
    }

    /**
     * One transaction, so a delivery is never consumed without the retry it bought.
     *
     * @throws Throwable
     */
    private function handOverAndRetry(
        FlowSignal $signal,
        FlowSignal $delivered,
        ActionRun $step,
        int $sequence
    ): bool {
        $recorder = app(SignalRecorder::class);
        $flowRun = $this->runtime->run();

        try {
            return (bool) $this->connection()
                ->transaction(function () use ($recorder, $flowRun, $signal, $delivered, $step, $sequence): bool {
                    if (! $recorder->consumeWhileWaiting($flowRun, $signal, $sequence)) {
                        return false;
                    }

                    $recorder->consumeSignal($flowRun, $delivered, $sequence);

                    if (! app(ActionRecorder::class)->retryAction(
                        $step,
                        $this->resolvedExpiresAt(),
                        publish: false,
                    )) {
                        throw new FencedWriteLost;
                    }

                    return true;
                });
        } catch (FencedWriteLost) {
            return false;
        }
    }

    /**
     * On PostgreSQL a host observer swallowing a failure turns the commit into a rollback.
     */
    private function retrySurvivedCommit(ActionRun $step): bool
    {
        $generation = $step->newQuery()
            ->useWritePdo()
            ->whereKey($step->getKey())
            ->value('retry_signal_attempts');

        return (int) $generation === $step->retry_signal_attempts;
    }

    /**
     * @throws FlowSuspended
     * @throws Throwable
     */
    private function consumeAndRetry(FlowSignal $signal, ActionRun $step, int $sequence): never
    {
        try {
            $this->connection()->transaction(function () use ($signal, $step, $sequence): void {
                app(SignalRecorder::class)->consumeSignal($this->runtime->run(), $signal, $sequence);

                if (! app(ActionRecorder::class)->retryAction(
                    $step,
                    $this->resolvedExpiresAt(),
                    publish: false,
                )) {
                    // Rolling back keeps the delivery for the pass that did move the row.
                    throw new FencedWriteLost;
                }
            });
        } catch (FencedWriteLost) {
            app(FlowSuspender::class)->suspend('action', $sequence);
        }

        if (! $this->retrySurvivedCommit($step)) {
            app(FlowSuspender::class)->suspend('action', $sequence);
        }

        app(ActionRecorder::class)->publishRetried($step);

        $this->startRetriedStep($step, $sequence);
    }

    private function connection(): ConnectionInterface
    {
        return DB::connection(config('saga-lara-flow.database.connection') ?: null);
    }

    /**
     * @throws FlowSuspended
     */
    private function retryNow(ActionRun $step, int $sequence): never
    {
        if (! app(ActionRecorder::class)->retryAction($step, $this->resolvedExpiresAt())) {
            // Dispatching now would send a job for a row this pass did not rewind.
            app(FlowSuspender::class)->suspend('action', $sequence);
        }

        $this->startRetriedStep($step, $sequence);
    }

    /**
     * @throws FlowSuspended
     */
    private function startRetriedStep(ActionRun $step, int $sequence): never
    {
        $suspender = app(FlowSuspender::class);
        $dispatcher = app(ActionDispatcher::class);

        if ($this->runtime->mode() === RunMode::Sync) {
            try {
                // Rewound in this call chain, so a lost claim is a broken invariant.
                if ($dispatcher->execute($step, inline: true) === StepExecution::ClaimLost) {
                    throw ActionClaimFailedException::forAction($step->action_class, $sequence);
                }
            } catch (ActionClaimFailedException $exception) {
                throw $exception;
            } catch (Throwable) {
            }

            $suspender->suspendInline('action', $sequence);
        }

        $dispatcher->redispatch($step);

        $suspender->suspend('action', $sequence);
    }

    private function giveUpAfterRetry(ActionRun $step, int $sequence): mixed
    {
        // Not a suspend: the plan needs every compensation after this step. A later replay writes it.
        $collecting = $this->runtime->isCollecting();

        if (! $this->resolvedContinueOnFailure()) {
            if (! $collecting) {
                app(ActionRecorder::class)->settleAwaitingRetry($step);
            }

            $this->resolveFailed($step, $sequence);
        }

        if (! $collecting && $step->status !== ActionStatus::OptionalFailed) {
            if (! app(ActionRecorder::class)->optionalFail($step)) {
                // The give-up did not land, so its fallback is not this pass's to return.
                app(FlowSuspender::class)->suspend('action', $sequence);
            }
        }

        return $this->resolveOptionalFailed($step, $sequence);
    }

    /**
     * @throws InternalFlowControl
     */
    private function shouldRetryOnSignal(ActionRun $step): bool
    {
        if ($this->retry === null) {
            return false;
        }

        $maxRetries = $this->budgetFor($step);

        if ($maxRetries !== null && $step->retry_signal_attempts >= $maxRetries) {
            return false;
        }

        // A timed-out wait ends the policy whatever the budget left.
        if ($this->waitTimedOut($step)) {
            return false;
        }

        if (! $this->retry->matches(RecordedFailure::fromRecord($step->exception))) {
            return false;
        }

        return $this->policyAllows($this->retry, $step, $maxRetries);
    }

    /**
     * Last: the only gate that runs the caller's code.
     *
     * @throws InternalFlowControl
     */
    private function policyAllows(SignalRetry $retry, ActionRun $step, ?int $maxRetries): bool
    {
        $flowRun = $this->runtime->run();

        // Built before the caller's code runs: a throw here is the engine's, not the predicate's.
        $context = new RetryContext(
            runId: $flowRun->id,
            workflowClass: $flowRun->workflow_class,
            actionClass: $step->action_class,
            sequence: $step->sequence,
            signal: $retry->signal,
            cyclesSpent: $step->retry_signal_attempts,
            cap: $maxRetries,
            executions: $step->attempts,
            failure: RecordedFailure::fromRecord($step->exception),
        );

        return $retry->allows($this->runtime, $context, ['action_run_id' => $step->id]);
    }

    /**
     * The row's policy counts too: removing retryOnSignal() must not fail a step the queue retries.
     */
    private function carriesRetryPolicy(ActionRun $step): bool
    {
        return $this->retry !== null || $step->retry_signal !== null;
    }

    /**
     * The row's cap, the one saga-flow:show reports, wins over a changed config.
     */
    private function budgetFor(ActionRun $step): ?int
    {
        return $step->retry_signal === null
            ? $this->retry?->resolvedMaxRetries()
            : $step->retry_signal_max_attempts;
    }

    private function waitTimedOut(ActionRun $step): bool
    {
        $signal = app(SignalRepository::class)
            ->latestForSequence($this->runtime->run()->id, $step->sequence);

        return $signal?->status === SignalStatus::TimedOut;
    }

    private function queueRetriesRemain(ActionRun $step): bool
    {
        if ($this->runtime->mode() === RunMode::Sync) {
            return false;
        }

        return ! $step->queue_attempts_exhausted;
    }

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

    private function resolvedContinueOnFailure(): bool
    {
        return $this->continueOnFailure
            ?? app(AttributeReader::class)->action($this->actionClass)->continueOnFailure
            ?? false;
    }

    private function resolvedExpiresAt(): ?DateTimeInterface
    {
        if ($this->expiresAt !== null) {
            return $this->expiresAt;
        }

        $seconds = app(AttributeReader::class)->action($this->actionClass)->timeoutSeconds;

        return $seconds === null ? null : now()->addSeconds($seconds);
    }

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
