<?php

namespace DiscoveryUkraine\SagaLaraFlow\Queries;

use Closure;
use DateTimeInterface;
use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\SignalStatus;
use DiscoveryUkraine\SagaLaraFlow\FlowHandle;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Illuminate\Contracts\Pagination\LengthAwarePaginator;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Collection as SupportCollection;

/**
 * Fluent, type-safe filter over flow runs. It wraps an Eloquent Builder rather
 * than exposing model query-scopes (Larastan level 5 cannot type a scope on a
 * generic Builder resolved from a config-swappable model) or __call magic.
 * Each where* method narrows the underlying query inline and returns $this;
 * terminals run it and hydrate FlowRun / FlowHandle. Reach the raw Builder via
 * builder() when you need ordering, limits, or Eloquent directly.
 */
readonly class FlowQuery
{
    /**
     * @param  Builder<FlowRun>  $builder
     */
    public function __construct(
        private Builder $builder,
    ) {}

    public function whereTag(string $key, ?string $value = null): static
    {
        $this->builder->whereHas('tags', function (Builder $query) use ($key, $value): void {
            $query->where('key', $key);

            if ($value !== null) {
                $query->where('value', $value);
            }
        });

        return $this;
    }

    public function whereStatus(FlowStatus ...$statuses): static
    {
        $this->builder->whereIn(
            'status',
            array_map(static fn (FlowStatus $status): string => $status->value, $statuses),
        );

        return $this;
    }

    public function whereWorkflow(string $workflowClass): static
    {
        $this->builder->where('workflow_class', $workflowClass);

        return $this;
    }

    /**
     * Runs whose wait for a signal is still open, whichever seam opened it: an
     * explicit awaitSignal(), or a step or child parked by retryOnSignal(). A null $name
     * matches any name.
     *
     * Only whereAwaitingRetrySignal() tells the two seams apart. Matching is on the
     * signal row rather than the run's status. A run settles its open waits as it
     * finishes, so it leaves this filter on its own; compose with signalable() to skip
     * the rows left behind by runs that finished before that behaviour existed.
     */
    public function whereAwaitingSignal(?string $name = null): static
    {
        $this->builder->whereHas('signals', function (Builder $query) use ($name): void {
            $query->where('status', SignalStatus::Waiting);

            if ($name !== null) {
                $query->where('name', $name);
            }
        });

        return $this;
    }

    /**
     * Runs holding a step or a child parked by retryOnSignal(), i.e. an action_runs or a
     * flow_children row in awaiting_retry. A null $signal matches any retry signal.
     *
     * A park opens a signal wait too, but the two settle at different moments:
     * delivery marks the wait Received and a timeout marks it TimedOut, while the
     * row stays parked until replay resumes the run. So this also finds a run whose
     * signal arrived but whose resume never did.
     */
    public function whereAwaitingRetrySignal(?string $signal = null): static
    {
        $this->builder->where(function (Builder $query) use ($signal): void {
            $query->whereHas('actions', $this->parkedOn(ActionStatus::AwaitingRetry, $signal))
                ->orWhereHas('children', $this->parkedOn(ChildStatus::AwaitingRetry, $signal));
        });

        return $this;
    }

    /**
     * @return Closure(Builder<Model>): void
     */
    private function parkedOn(ActionStatus|ChildStatus $status, ?string $signal): Closure
    {
        return function (Builder $query) use ($status, $signal): void {
            $query->where('status', $status);

            if ($signal !== null) {
                $query->where('retry_signal', $signal);
            }
        };
    }

    public function running(): static
    {
        return $this->whereStatus(FlowStatus::Running);
    }

    public function waiting(): static
    {
        return $this->whereStatus(FlowStatus::Waiting);
    }

    /**
     * Runs that can still be handed a signal — Pending, Running, or Waiting. Use
     * this, not running(), to locate a run to deliver a signal to: a flow parked
     * on awaitSignal() is Waiting, not Running.
     */
    public function active(): static
    {
        return $this->whereStatus(...FlowStatus::signalable());
    }

    /** Alias of active() that reads as intent right before ->signal(). */
    public function signalable(): static
    {
        return $this->active();
    }

    public function completed(): static
    {
        return $this->whereStatus(FlowStatus::Completed);
    }

    public function failed(): static
    {
        return $this->whereStatus(FlowStatus::Failed);
    }

    public function before(DateTimeInterface $instant): static
    {
        $this->builder->where('created_at', '<', $instant);

        return $this;
    }

    public function after(DateTimeInterface $instant): static
    {
        $this->builder->where('created_at', '>', $instant);

        return $this;
    }

    /**
     * @return Collection<int, FlowRun>
     */
    public function get(): Collection
    {
        return $this->builder->get();
    }

    public function first(): ?FlowRun
    {
        return $this->builder->first();
    }

    public function count(): int
    {
        return $this->builder->count();
    }

    public function paginate(int $perPage = 15): LengthAwarePaginator
    {
        return $this->builder->paginate($perPage);
    }

    /**
     * Hydrate the matched runs as operable handles.
     *
     * @return SupportCollection<int, FlowHandle>
     */
    public function handles(): SupportCollection
    {
        return $this->get()->map(static fn (FlowRun $run): FlowHandle => new FlowHandle($run));
    }

    /**
     * Escape hatch to the underlying Eloquent builder for ordering, limits, etc.
     *
     * @return Builder<FlowRun>
     */
    public function builder(): Builder
    {
        return $this->builder;
    }
}
