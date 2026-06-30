<?php

namespace DiscoveryUkraine\SagaLaraFlow\Console\Commands;

use Carbon\CarbonImmutable;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowPruner;
use Illuminate\Console\Command;
use Throwable;

/**
 * Deletes old terminal flow runs and their related rows. The retention window is
 * --before (an explicit date/time) > --days > config('saga-lara-flow.prune.retention_days').
 * Use --dry-run to report what would be deleted without deleting anything.
 */
class FlowPruneCommand extends Command
{
    protected $signature = 'saga-flow:prune
        {--days= : Retention window in days (overrides config)}
        {--before= : Delete terminal runs created before this date/time (overrides --days)}
        {--dry-run : Report what would be deleted without deleting}';

    protected $description = 'Prune old terminal saga flow runs and their related rows.';

    /**
     * @throws Throwable
     */
    public function handle(FlowPruner $pruner): int
    {
        $before = $this->resolveBefore();
        $dryRun = (bool) $this->option('dry-run');

        $report = $pruner->prune($before, $dryRun);

        $this->info(sprintf(
            '%s %d flow run(s) and %d related row(s) created before %s.',
            $dryRun ? 'Would prune' : 'Pruned',
            $report->runs,
            $report->relatedRows,
            $before->toDateTimeString(),
        ));

        return self::SUCCESS;
    }

    private function resolveBefore(): CarbonImmutable
    {
        if (is_string($before = $this->option('before')) && $before !== '') {
            return CarbonImmutable::parse($before);
        }

        $days = is_string($days = $this->option('days')) && $days !== ''
            ? (int) $days
            : (int) config('saga-lara-flow.prune.retention_days', 90);

        return CarbonImmutable::now()->subDays($days);
    }
}
