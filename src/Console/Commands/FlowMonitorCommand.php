<?php

namespace DiscoveryUkraine\SagaLaraFlow\Console\Commands;

use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowMonitor;
use Illuminate\Console\Command;
use Throwable;

/**
 * Runs one expiration sweep (§15). Schedule it (e.g. everyMinute()) to expire
 * overdue runs/actions and time out stuck signal waits without durable timers.
 */
class FlowMonitorCommand extends Command
{
    protected $signature = 'saga-flow:monitor';

    protected $description = 'Expire overdue saga flows and actions and time out stuck signal waits.';

    /**
     * @throws Throwable
     */
    public function handle(FlowMonitor $monitor): int
    {
        if (! config('saga-lara-flow.monitor.enabled')) {
            $this->info('Saga flow monitor is disabled (saga-lara-flow.monitor.enabled).');

            return self::SUCCESS;
        }

        $report = $monitor->sweep();

        $this->info(sprintf(
            'Monitor swept: %d run(s) expired, %d signal(s) timed out, %d action(s) expired.',
            $report['runs'],
            $report['signals'],
            $report['actions'],
        ));

        return self::SUCCESS;
    }
}
