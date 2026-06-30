<?php

namespace DiscoveryUkraine\SagaLaraFlow\Console\Commands;

use DiscoveryUkraine\SagaLaraFlow\Exceptions\CannotSignalTerminalFlowException;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\FlowNotFoundException;
use DiscoveryUkraine\SagaLaraFlow\FlowManager;
use Illuminate\Console\Command;

/**
 * Delivers an external signal to a run and wakes it. An optional --payload is a
 * JSON-encoded object/array passed to the waiting handle().
 */
class FlowSignalCommand extends Command
{
    protected $signature = 'saga-flow:signal
        {run : The flow run id}
        {name : The signal name}
        {--payload= : JSON-encoded payload object/array}';

    protected $description = 'Deliver a signal to a saga flow run.';

    public function handle(FlowManager $manager): int
    {
        try {
            $handle = $manager->loadFlow((string) $this->argument('run'));
        } catch (FlowNotFoundException) {
            $this->error("Flow run [{$this->argument('run')}] not found.");

            return self::FAILURE;
        }

        $payload = [];

        if (is_string($raw = $this->option('payload')) && $raw !== '') {
            $decoded = json_decode($raw, true);

            if (!is_array($decoded)) {
                $this->error('Payload must be a JSON object or array.');

                return self::FAILURE;
            }

            $payload = $decoded;
        }

        $name = (string) $this->argument('name');

        try {
            $handle->signal($name, $payload);
        } catch (CannotSignalTerminalFlowException) {
            $this->warn("Flow run [{$handle->id()}] is terminal; signal [$name] not delivered.");

            return self::SUCCESS;
        }

        $this->info("Signal [$name] delivered to flow run [{$handle->id()}].");

        return self::SUCCESS;
    }
}
