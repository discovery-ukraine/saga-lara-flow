<?php

namespace DiscoveryUkraine\SagaLaraFlow\Support;

use DiscoveryUkraine\SagaLaraFlow\Exceptions\InvalidTenancyHookException;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Illuminate\Contracts\Container\BindingResolutionException;

/**
 * Runs a run's business code (workflow/action/compensation) inside the tenant it
 * was created for, and — critically — reverts afterwards so nothing leaks into
 * the next run sharing the same Octane/queue worker.
 *
 * capture()/restore()/end() are thin hooks over config('saga-lara-flow.tenancy.*')
 * callables; without hooks they are no-ops. Auto-restore is opt-in (config
 * tenancy.auto, overridable per class by #[Tenancy]); either way for() records the
 * current context so SagaFlow::tenancyContext() can expose it to manual code.
 *
 * Registered as a scoped binding so the recorded context is shared with the
 * facade within one request/job and reset between Octane requests.
 */
class TenancyManager
{
    /** @var array<int|string, mixed>|null */
    private ?array $current = null;

    /**
     * Execute $callback with $flowRun's tenant restored (when auto is enabled for
     * $autoClass) and always with its context recorded for discovery. Reverts to
     * the previous tenant/context in a finally, so back-to-back runs in one worker
     * never leak into one another.
     *
     * @template T
     *
     * @param  callable(): T  $callback
     * @return T
     */
    public function for(FlowRun $flowRun, ?string $autoClass, callable $callback): mixed
    {
        $auto = $this->autoEnabled($autoClass);

        // Every hook is resolved before any is called or the context is touched. A
        // broken one then refuses the step before it runs — not after it has recorded
        // its work, with the worker still inside the run's tenant.
        if ($auto) {
            $this->hook('capture');
            $this->hook('restore');
            $this->hook('end');
        }

        $previous = $auto ? $this->capture() : null;

        $heldContext = $this->current;
        $this->current = $flowRun->tenancy_context;

        try {
            // Inside the bracket: a restore that fails part of the way still reverts.
            if ($auto) {
                $this->restore($flowRun);
            }

            return $callback();
        } finally {
            try {
                if ($auto) {
                    $this->end($previous);
                }
            } finally {
                $this->current = $heldContext;
            }
        }
    }

    /**
     * The tenant context of the run currently executing, for manual host code
     * (SagaFlow::tenancyContext()). Null outside a driven run, or when no context
     * was captured at creation.
     *
     * @return array<int|string, mixed>|null
     */
    public function context(): ?array
    {
        return $this->current;
    }

    /**
     * Whether auto capture/restore applies to $class: the #[Tenancy] override wins,
     * otherwise the config default.
     */
    public function autoEnabled(?string $class): bool
    {
        $override = $class === null
            ? null
            : app(AttributeReader::class)->tenancyAuto($class);

        return $override ?? (bool) config('saga-lara-flow.tenancy.auto');
    }

    /**
     * Enter the tenant recorded on the run before execution/replay.
     */
    public function restore(FlowRun $flowRun): void
    {
        $restore = $this->hook('restore');

        if ($restore !== null) {
            $restore($flowRun->tenancy_context ?? []);
        }
    }

    /**
     * Revert tenancy after execution: the explicit tenancy.end hook when set,
     * otherwise restore the previously active context (bracket-previous).
     *
     * @param  array<int|string, mixed>|null  $previous
     */
    public function end(?array $previous): void
    {
        $end = $this->hook('end');

        if ($end !== null) {
            $end($previous);

            return;
        }

        $restore = $this->hook('restore');

        if ($restore !== null) {
            $restore($previous ?? []);
        }
    }

    /**
     * Capture the current tenant context, or null when no hook is configured.
     *
     * @return array<int|string, mixed>|null
     */
    public function capture(): ?array
    {
        $capture = $this->hook('capture');

        return $capture !== null ? $capture() : null;
    }

    /**
     * The tenancy.$name hook as something to call, or null when it is turned off.
     * Besides a plain callable, the hook may name an invokable class, or a [class,
     * method] pair whose method is not static; either is resolved from the container.
     * Those two forms are what lets a host run config:cache, which cannot store a
     * closure.
     *
     * Only null turns a hook off. Anything else that cannot be called is refused: a
     * mistyped class skipped as if it were absent would run the step in whatever
     * tenant the worker is already in.
     *
     * @throws InvalidTenancyHookException
     */
    private function hook(string $name): ?callable
    {
        $hook = config("saga-lara-flow.tenancy.{$name}");

        if ($hook === null) {
            return null;
        }

        if (is_callable($hook)) {
            return $hook;
        }

        $resolved = null;

        try {
            if (is_string($hook) && class_exists($hook)) {
                $resolved = app($hook);
            } elseif (is_array($hook) && count($hook) === 2 && is_string($hook[0] ?? null)
                && is_string($hook[1] ?? null) && class_exists($hook[0])) {
                $resolved = [app($hook[0]), $hook[1]];
            }
        } catch (BindingResolutionException $e) {
            throw InvalidTenancyHookException::for($name, $hook, $e);
        }

        if (! is_callable($resolved)) {
            throw InvalidTenancyHookException::for($name, $hook);
        }

        return $resolved;
    }
}
