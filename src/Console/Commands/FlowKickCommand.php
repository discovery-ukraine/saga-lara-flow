<?php

namespace DiscoveryUkraine\SagaLaraFlow\Console\Commands;

use DiscoveryUkraine\SagaLaraFlow\Contracts\FlowRepository;
use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationStatus;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use DiscoveryUkraine\SagaLaraFlow\Runtime\FlowDoctor;
use Illuminate\Console\Command;
use Illuminate\Support\Carbon;

class FlowKickCommand extends Command
{
    protected $signature = 'saga-flow:kick {run : The flow run id to re-drive}';

    protected $description = 'Manually re-drive a stuck saga flow run.';

    public function handle(FlowRepository $flows, FlowDoctor $doctor): int
    {
        $id = (string) $this->argument('run');

        $run = $flows->findOnWriter($id);

        if ($run === null) {
            $this->error("Flow run [{$id}] not found.");

            return self::FAILURE;
        }

        if ($run->status === FlowStatus::Cancelling) {
            if (! $doctor->takeOverStalledRollback($run)) {
                $this->warn("Flow run [{$id}] is cancelling; nothing to re-drive.");
            } elseif ($this->heldByAWorker($flows->findOnWriter($id) ?? $run)) {
                $this->warn("Flow run [{$id}] rollback taken over, but a compensation a worker still holds keeps it waiting; kick it again once that compensation has finished.");
            } else {
                $this->info("Flow run [{$id}] rollback taken over and re-driven.");
            }

            return self::SUCCESS;
        }

        $run = $doctor->kick($run);

        if (! $run->status->canStartWork()) {
            $this->warn("Flow run [{$id}] is {$run->status->value}; nothing to re-drive.");

            return self::SUCCESS;
        }

        $this->info("Flow run [{$id}] re-driven.");

        return self::SUCCESS;
    }

    /**
     * A takeover sends no job for a level whose compensation a worker that may be alive holds.
     */
    private function heldByAWorker(FlowRun $run): bool
    {
        return $run->status === FlowStatus::Cancelling && $run->compensations()->useWritePdo()
            ->where('status', CompensationStatus::Running)
            ->where(fn ($query) => $query->whereNull('reclaim_stale_at')->orWhere('reclaim_stale_at', '>', Carbon::now()))
            ->exists();
    }
}
