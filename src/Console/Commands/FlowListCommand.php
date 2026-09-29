<?php

namespace DiscoveryUkraine\SagaLaraFlow\Console\Commands;

use DiscoveryUkraine\SagaLaraFlow\Enums\ActionStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\FlowManager;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowChild;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowTag;
use Illuminate\Console\Command;
use Illuminate\Support\Collection;

/**
 * Lists flow runs with optional filters, newest first — a thin CLI over
 * SagaFlow::query() (whereStatus/whereTag/whereWorkflow).
 */
class FlowListCommand extends Command
{
    protected $signature = 'saga-flow:list
        {--status= : Filter by status (pending, running, waiting, completed, failed, cancelling, cancelled, expired)}
        {--tag= : Filter by tag, "key" or "key=value"}
        {--workflow= : Filter by workflow class}
        {--limit=50 : Maximum number of rows to show}';

    protected $description = 'List saga flow runs with optional filters.';

    public function handle(FlowManager $manager): int
    {
        $query = $manager->query();

        if (is_string($status = $this->option('status')) && $status !== '') {
            $resolved = FlowStatus::tryFrom($status);

            if ($resolved === null) {
                $this->error("Unknown status [$status].");

                return self::FAILURE;
            }

            $query->whereStatus($resolved);
        }

        if (is_string($tag = $this->option('tag')) && $tag !== '') {
            [$key, $value] = array_pad(explode('=', $tag, 2), 2, null);
            $query->whereTag((string) $key, $value);
        }

        if (is_string($workflow = $this->option('workflow')) && $workflow !== '') {
            $query->whereWorkflow($workflow);
        }

        $runs = $query->builder()
            ->with('tags')
            ->latest('created_at')
            ->limit((int) $this->option('limit'))
            ->get();

        if ($runs->isEmpty()) {
            $this->info('No flow runs found.');

            return self::SUCCESS;
        }

        $parked = $this->retrySignalsByRun($runs);

        $this->table(
            ['ID', 'Workflow', 'Status', 'Created', 'Tags'],
            $runs->map(fn (FlowRun $run): array => [
                $run->id,
                class_basename($run->workflow_class),
                $this->formatStatus($run, $parked),
                (string) $run->created_at,
                $this->formatTags($run),
            ])->all(),
        );

        return self::SUCCESS;
    }

    /**
     * The signals the listed runs are parked on, keyed by run id. A run parked by
     * retryOnSignal() is plainly Waiting like any other, so without this the signal
     * that would unblock it is invisible until the operator opens the run. One query
     * per table that parks, for the whole page.
     *
     * @param  Collection<int, FlowRun>  $runs
     * @return Collection<string, string>
     */
    private function retrySignalsByRun(Collection $runs): Collection
    {
        /** @var class-string<ActionRun> $steps */
        $steps = config('saga-lara-flow.models.action_run');

        /** @var class-string<FlowChild> $children */
        $children = config('saga-lara-flow.models.flow_child');

        $ids = $runs->pluck('id')->all();

        $parkedSteps = $steps::query()
            ->whereIn('flow_run_id', $ids)
            ->where('status', ActionStatus::AwaitingRetry)
            ->whereNotNull('retry_signal')
            ->get(['flow_run_id', 'retry_signal'])
            ->map(fn (ActionRun $step): array => ['run' => $step->flow_run_id, 'signal' => $step->retry_signal]);

        $parkedChildren = $children::query()
            ->whereIn('parent_flow_run_id', $ids)
            ->where('status', ChildStatus::AwaitingRetry)
            ->get(['parent_flow_run_id', 'retry_signal'])
            ->map(fn (FlowChild $link): array => ['run' => $link->parent_flow_run_id, 'signal' => $link->retry_signal]);

        return $parkedSteps->toBase()
            ->concat($parkedChildren)
            ->groupBy('run')
            ->map(fn (Collection $parked): string => $parked
                ->pluck('signal')
                ->unique()
                ->implode(', '));
    }

    /**
     * The status cell, with the signal a parked run waits on appended to it.
     *
     * Only a Waiting run is annotated: a run mid-rollback is Cancelling, which is not
     * terminal, so it still holds its AwaitingRetry step — and naming a signal there
     * would send the operator after a delivery the run no longer accepts.
     *
     * @param  Collection<string, string>  $parked
     */
    private function formatStatus(FlowRun $run, Collection $parked): string
    {
        $signals = $run->status === FlowStatus::Waiting ? $parked->get($run->id) : null;

        return $signals === null
            ? $run->status->value
            : "{$run->status->value} (retry: {$signals})";
    }

    private function formatTags(FlowRun $run): string
    {
        $labels = [];

        /** @var FlowTag $tag */
        foreach ($run->tags as $tag) {
            $labels[] = $tag->value === null ? $tag->key : "{$tag->key}={$tag->value}";
        }

        return implode(', ', $labels);
    }
}
