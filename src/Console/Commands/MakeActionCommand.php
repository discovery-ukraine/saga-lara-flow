<?php

namespace DiscoveryUkraine\SagaLaraFlow\Console\Commands;

use Illuminate\Console\GeneratorCommand;

/**
 * Generates a new Action class from the package stub (app/Actions by default).
 */
class MakeActionCommand extends GeneratorCommand
{
    protected $name = 'make:action';

    protected $description = 'Create a new saga action class';

    protected $type = 'Action';

    protected function getStub(): string
    {
        return __DIR__.'/../../../resources/stubs/action.stub';
    }

    protected function getDefaultNamespace($rootNamespace): string
    {
        return $rootNamespace.'\\Actions';
    }
}
