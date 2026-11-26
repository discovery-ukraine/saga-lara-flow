<?php

namespace DiscoveryUkraine\SagaLaraFlow\Console\Commands;

use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowDoctor;
use Illuminate\Console\Command;

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
            'Doctor repaired: %d action(s) re-dispatched, %d flow(s) re-woken, %d stalled rollback(s) resumed, %d skipped.',
            $report->redispatchedActions,
            $report->rewokenFlows,
            $report->resumedRollbacks,
            $report->skipped,
        ));

        return self::SUCCESS;
    }
}
