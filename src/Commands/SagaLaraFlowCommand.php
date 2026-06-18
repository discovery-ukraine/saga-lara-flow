<?php

namespace DiscoveryUkraine\SagaLaraFlow\Commands;

use Illuminate\Console\Command;

class SagaLaraFlowCommand extends Command
{
    public $signature = 'saga-lara-flow';

    public $description = 'My command';

    public function handle(): int
    {
        $this->comment('All done');

        return self::SUCCESS;
    }
}
