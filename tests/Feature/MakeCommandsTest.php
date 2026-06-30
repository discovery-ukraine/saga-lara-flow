<?php

use Illuminate\Support\Facades\Artisan;

afterEach(function () {
    foreach ([app_path('Workflows/SampleFlow.php'), app_path('Actions/SampleAction.php')] as $path) {
        if (file_exists($path)) {
            unlink($path);
        }
    }
});

it('generates a workflow class from the stub', function () {
    $path = app_path('Workflows/SampleFlow.php');

    if (file_exists($path)) {
        unlink($path);
    }

    expect(Artisan::call('make:workflow', ['name' => 'SampleFlow']))->toBe(0)
        ->and(file_exists($path))->toBeTrue();

    $contents = (string) file_get_contents($path);

    expect($contents)->toContain('namespace App\\Workflows;')
        ->toContain('class SampleFlow extends Workflow')
        ->toContain('public function handle()');
});

it('generates an action class from the stub', function () {
    $path = app_path('Actions/SampleAction.php');

    if (file_exists($path)) {
        unlink($path);
    }

    expect(Artisan::call('make:action', ['name' => 'SampleAction']))->toBe(0)
        ->and(file_exists($path))->toBeTrue();

    $contents = (string) file_get_contents($path);

    expect($contents)->toContain('namespace App\\Actions;')
        ->toContain('class SampleAction extends Action');
});
