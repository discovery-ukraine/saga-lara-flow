<?php

namespace DiscoveryUkraine\SagaLaraFlow\Console\Commands;

use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowDoctor;
use Illuminate\Console\Command;

/**
 * Runs one doctor repair pass: re-dispatch stuck sequential
 * Pending actions and re-wake stuck Waiting runs whose jobs were lost. Schedule
 * it (e.g. everyMinute()) independently of saga-flow:monitor. Opt-in via
 * saga-lara-flow.repair.enabled.
 */
class FlowRepairCommand extends Command
{
    protected $signature = 'saga-flow:repair';

    protected $description = 'Recover saga flows whose progress was lost to a dropped job (re-dispatch / re-wake).';

    public function handle(FlowDoctor $doctor): int
    {
        if (! config('saga-lara-flow.repair.enabled')) {
            $this->info('Saga flow repair is disabled (saga-lara-flow.repair.enabled).');

            return self::SUCCESS;
        }

        $report = $doctor->repair();

        $this->info(sprintf(
            'Doctor repaired: %d action(s) re-dispatched, %d flow(s) re-woken, %d skipped.',
            $report->redispatchedActions,
            $report->rewokenFlows,
            $report->skipped,
        ));

        return self::SUCCESS;
    }
}
