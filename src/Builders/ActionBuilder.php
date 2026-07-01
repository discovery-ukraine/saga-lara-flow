<?php

namespace DiscoveryUkraine\SagaLaraFlow\Builders;

use Closure;
use DateTimeInterface;
use DiscoveryUkraine\SagaLaraFlow\Contracts\ActionRunRepository;
use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use DiscoveryUkraine\SagaLaraFlow\Data\CompensationDefinition;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationFailurePolicy;
use DiscoveryUkraine\SagaLaraFlow\Enums\RunMode;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\ActionFailedException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowExpiredException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\HistoryContractMismatchException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\FlowSuspended;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ActionDispatcher;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ActionRecorder;
use DiscoveryUkraine\SagaLaraFlow\Runtime\CompensationEntry;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowRuntime;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowSuspender;
use DiscoveryUkraine\SagaLaraFlow\Runtime\HistoryContractGuard;
use DiscoveryUkraine\SagaLaraFlow\Support\AttributeReader;
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

        $hasCompensation = $this->compensation !== null;
        $continueOnFailure = $this->resolvedContinueOnFailure();
        $expiresAt = $this->resolvedExpiresAt();
        $actionName = $this->resolvedActionName();

        if ($this->runtime->mode() === RunMode::Sync) {
            try {
                $dispatcher->runInline(
                    $flowRun,
                    $sequence,
                    $this->actionClass,
                    $this->arguments,
                    $hasCompensation,
                    $continueOnFailure,
                    expiresAt: $expiresAt,
                    actionName: $actionName,
                );
            } catch (Throwable $exception) {
                // Optional step: no retries inline, so give up now — mark it
                // OptionalFailed and replay so the seam resolves the fallback.
                if ($continueOnFailure) {
                    $this->markOptionalFailed($flowRun->id, $sequence);

                    $suspender->suspendInline('action', $sequence);
                }

                $this->registerFailedStepCompensation($flowRun->id, $sequence);

                throw $exception;
            }

            $suspender->suspendInline('action', $sequence);
        }

        $dispatcher->dispatch(
            $flowRun,
            $sequence,
            $this->actionClass,
            $this->arguments,
            $hasCompensation,
            $continueOnFailure,
            $expiresAt,
            $actionName,
        );

        $suspender->suspend('action', $sequence);
    }

    /**
     * @throws FlowSuspended
     */
    private function resolve(ActionRun $step, int $sequence): mixed
    {
        switch ($step->status) {
            case ActionStatus::Completed:
                return $this->resolveCompleted($step, $sequence);
            case ActionStatus::OptionalFailed:
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
                // An optional step still has retries left: it is not yet
                // OptionalFailed, so wait rather than surface a business error.
                if ($this->resolvedContinueOnFailure()) {
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
     * Sync path: an optional inline action threw. Mark its just-persisted Failed row
     * (looked up by its (flow_run_id, sequence) identity) as OptionalFailed so the
     * replay resolves the fallback instead of a business error.
     */
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
