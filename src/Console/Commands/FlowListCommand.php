<?php

namespace DiscoveryUkraine\SagaLaraFlow\Console\Commands;

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\FlowManager;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowTag;
use DiscoveryUkraine\SagaLaraFlow\Runtime\ParkedRetrySignals;
use Illuminate\Console\Command;

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

    public function handle(FlowManager $manager, ParkedRetrySignals $retries): int
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

        // A parked run is plainly Waiting, so without this the signal that unblocks it is
        // invisible until the operator opens the run.
        $parked = $retries->byRun($runs->pluck('id')->all());

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
     * The status cell, with the signal a parked run waits on appended to it.
     *
     * Only a Waiting run is annotated: a run mid-rollback is Cancelling, which is not
     * terminal, so it still holds its AwaitingRetry step — and naming a signal there
     * would send the operator after a delivery the run no longer accepts.
     *
     * @param  array<string, list<string>>  $parked
     */
    private function formatStatus(FlowRun $run, array $parked): string
    {
        $signals = $run->status === FlowStatus::Waiting ? ($parked[$run->id] ?? null) : null;

        return $signals === null
            ? $run->status->value
            : "{$run->status->value} (retry: ".implode(', ', $signals).')';
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
