<?php

namespace DiscoveryUkraine\SagaLaraFlow\Console\Commands;

use Illuminate\Console\GeneratorCommand;

/**
 * Generates a new Workflow class from the package stub (app/Workflows by default).
 */
class MakeWorkflowCommand extends GeneratorCommand
{
    protected $name = 'make:workflow';

    protected $description = 'Create a new saga workflow class';

    protected $type = 'Workflow';

    protected function getStub(): string
    {
        return __DIR__.'/../../../resources/stubs/workflow.stub';
    }

    protected function getDefaultNamespace($rootNamespace): string
    {
        return $rootNamespace.'\\Workflows';
    }
}
