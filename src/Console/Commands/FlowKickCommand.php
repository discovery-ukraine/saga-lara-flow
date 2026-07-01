<?php

namespace DiscoveryUkraine\SagaLaraFlow\Console\Commands;

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowDoctor;
use Illuminate\Console\Command;

/**
 * Manually re-drive one run — the escape hatch when a flow is
 * stuck (a crashed Running iteration, an exhausted automatic repair). Unlike the
 * automatic pass this is unthrottled; replay decides what happens once it resumes.
 */
class FlowKickCommand extends Command
{
    protected $signature = 'saga-flow:kick {run : The flow run id to re-drive}';

    protected $description = 'Manually re-drive a stuck saga flow run.';

    public function handle(FlowRepository $flows, FlowDoctor $doctor): int
    {
        $id = (string) $this->argument('run');

        $run = $flows->find($id);

        if ($run === null) {
            $this->error("Flow run [{$id}] not found.");

            return self::FAILURE;
        }

        if ($run->isTerminal()) {
            $this->warn("Flow run [{$id}] is terminal ({$run->status->value}); nothing to re-drive.");

            return self::SUCCESS;
        }

        $doctor->kick($run);

        $this->info("Flow run [{$id}] re-driven.");

        return self::SUCCESS;
    }
}
