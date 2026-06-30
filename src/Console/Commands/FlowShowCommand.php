<?php

namespace DiscoveryUkraine\SagaLaraFlow\Console\Commands;

use DiscoveryUkraine\SagaLaraFlow\FlowManager;
use DiscoveryUkraine\SagaLaraFlow\Models\ActionRun;
use DiscoveryUkraine\SagaLaraFlow\Models\CompensationRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowEvent;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowSignal;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowTag;
use DiscoveryUkraine\SagaLaraFlow\Runtime\History;
use Illuminate\Console\Command;

/**
 * Inspects a single flow run: a header, its action table, and (unless --compact)
 * its signals, compensations, and full event history.
 */
class FlowShowCommand extends Command
{
    protected $signature = 'saga-flow:show
        {run : The flow run id}
        {--compact : Show only the header and actions}';

    protected $description = 'Show a saga flow run, its actions, signals, compensations and history.';

    public function handle(FlowManager $manager): int
    {
        $run = $manager->findRun((string) $this->argument('run'));

        if ($run === null) {
            $this->error("Flow run [{$this->argument('run')}] not found.");

            return self::FAILURE;
        }

        $this->renderHeader($run);
        $this->renderActions($run);

        if (! $this->option('compact')) {
            $this->renderSignals($run);
            $this->renderCompensations($run);
            $this->renderHistory($run);
        }

        return self::SUCCESS;
    }

    private function renderHeader(FlowRun $run): void
    {
        $this->table(['Field', 'Value'], [
            ['ID', $run->id],
            ['Workflow', $run->workflow_class],
            ['Name', $run->workflow_name ?? '—'],
            ['Version', $run->workflow_version ?? '—'],
            ['Status', $run->status->value],
            ['Started', (string) $run->started_at],
            ['Finished', (string) $run->finished_at],
            ['Expires', (string) $run->expires_at],
            ['Connection', $run->connection ?? '—'],
            ['Queue', $run->queue ?? '—'],
            ['Tags', $this->formatTags($run)],
        ]);
    }

    private function renderActions(FlowRun $run): void
    {
        $actions = $run->actions()->orderBy('sequence')->get();

        if ($actions->isEmpty()) {
            $this->line('No actions recorded.');

            return;
        }

        $rows = [];

        /** @var ActionRun $action */
        foreach ($actions as $action) {
            $rows[] = [
                $action->sequence,
                $action->status->value,
                $action->action_name ?? class_basename($action->action_class),
                $action->attempts,
                (string) $action->finished_at,
            ];
        }

        $this->table(['Seq', 'Status', 'Action', 'Attempts', 'Finished'], $rows);
    }

    private function renderSignals(FlowRun $run): void
    {
        if ($run->signals->isEmpty()) {
            return;
        }

        $rows = [];

        /** @var FlowSignal $signal */
        foreach ($run->signals as $signal) {
            $rows[] = [$signal->name, $signal->status->value, (string) $signal->received_at];
        }

        $this->table(['Signal', 'Status', 'Received'], $rows);
    }

    private function renderCompensations(FlowRun $run): void
    {
        if ($run->compensations->isEmpty()) {
            return;
        }

        $rows = [];

        /** @var CompensationRun $compensation */
        foreach ($run->compensations as $compensation) {
            $rows[] = [
                $compensation->sequence,
                $compensation->compensation_class ?? $compensation->compensation_type,
                $compensation->status->value,
            ];
        }

        $this->table(['Seq', 'Type', 'Status'], $rows);
    }

    private function renderHistory(FlowRun $run): void
    {
        $events = new History($run)->events();

        $this->line("History: {$events->count()} event(s)");

        if ($events->isEmpty()) {
            return;
        }

        $rows = [];

        /** @var FlowEvent $event */
        foreach ($events as $event) {
            $rows[] = [$event->type->value, $event->sequence ?? '—', (string) $event->recorded_at];
        }

        $this->table(['Type', 'Seq', 'Recorded'], $rows);
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
