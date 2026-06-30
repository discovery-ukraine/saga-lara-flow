<?php

namespace DiscoveryUkraine\SagaLaraFlow\Console\Commands;

use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowNotFoundException;
use DiscoveryUkraine\SagaLaraFlow\FlowManager;
use Illuminate\Console\Command;
use Throwable;

/**
 * Cancels a non-terminal run. With --compensate it rolls back the run's completed
 * compensatable steps first (a synchronous compensation-only replay), then cancels.
 */
class FlowCancelCommand extends Command
{
    protected $signature = 'saga-flow:cancel
        {run : The flow run id}
        {--compensate : Roll back completed compensatable steps before cancelling}';

    protected $description = 'Cancel a saga flow run (optionally compensating it first).';

    /**
     * @throws Throwable
     */
    public function handle(FlowManager $manager): int
    {
        try {
            $handle = $manager->loadFlow((string) $this->argument('run'));
        } catch (FlowNotFoundException) {
            $this->error("Flow run [{$this->argument('run')}] not found.");

            return self::FAILURE;
        }

        if ($handle->status()->isTerminal()) {
            $this->warn("Flow run [{$handle->id()}] is terminal ({$handle->status()->value}); nothing to cancel.");

            return self::SUCCESS;
        }

        if ($this->option('compensate')) {
            $handle->compensate();

            $this->info("Flow run [{$handle->id()}] compensated and cancelled.");

            return self::SUCCESS;
        }

        $handle->cancel('Cancelled via saga-flow:cancel');

        $this->info("Flow run [{$handle->id()}] cancelled.");

        return self::SUCCESS;
    }
}
