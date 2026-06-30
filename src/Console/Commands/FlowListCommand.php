<?php

namespace DiscoveryUkraine\SagaLaraFlow\Console\Commands;

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\FlowManager;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowTag;
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

        $this->table(
            ['ID', 'Workflow', 'Status', 'Created', 'Tags'],
            $runs->map(fn (FlowRun $run): array => [
                $run->id,
                class_basename($run->workflow_class),
                $run->status->value,
                (string) $run->created_at,
                $this->formatTags($run),
            ])->all(),
        );

        return self::SUCCESS;
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
