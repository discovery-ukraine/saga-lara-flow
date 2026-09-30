<?php

namespace DiscoveryUkraine\SagaLaraFlow\Console\Commands;

use DiscoveryUkraine\SagaLaraFlow\Console\Concerns\ReadsPayloadOption;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\CannotSignalFlowException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowNotFoundException;
use DiscoveryUkraine\SagaLaraFlow\FlowManager;
use Illuminate\Console\Command;

/**
 * Delivers the signal a run's parked step or child waits on, without naming it, and wakes
 * the run. --signal names the signal instead.
 */
class FlowSignalRetryCommand extends Command
{
    use ReadsPayloadOption;

    protected $signature = 'saga-flow:signal-retry
        {run : The flow run id}
        {--signal= : The signal name, instead of the one the run is parked on}
        {--payload= : JSON-encoded payload object/array}';

    protected $description = 'Deliver the retry signal a saga flow run is parked on.';

    public function handle(FlowManager $manager): int
    {
        try {
            $handle = $manager->loadFlow((string) $this->argument('run'));
        } catch (FlowNotFoundException) {
            $this->error("Flow run [{$this->argument('run')}] not found.");

            return self::FAILURE;
        }

        $payload = $this->payloadOption();

        if ($payload === null) {
            return self::FAILURE;
        }

        $name = $this->option('signal');
        $name = is_string($name) && $name !== '' ? $name : null;

        try {
            $handle->signalRetry($name, $payload);
        } catch (CannotSignalFlowException $refused) {
            $this->warn($refused->getMessage());

            return self::SUCCESS;
        } catch (FlowNotFoundException) {
            $this->error("Flow run [{$handle->id()}] not found.");

            return self::FAILURE;
        }

        $this->info($name === null
            ? "Retry signal delivered to flow run [{$handle->id()}]."
            : "Signal [$name] delivered to flow run [{$handle->id()}].");

        return self::SUCCESS;
    }
}
