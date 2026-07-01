<?php

// The engine's migration ships as a .stub and the provider calls runsMigrations(),
// so it is loaded into the migrator directly — a host app runs `php artisan migrate`
// with no `vendor:publish --tag=...-migrations` step. This guards that contract:
// resolving the migrator must expose the package migration among its paths.
it('registers its migration with the migrator so a plain migrate picks it up', function (): void {
    $registered = collect(app('migrator')->paths())
        ->contains(fn (string $path): bool => str_contains($path, 'create_saga_lara_flow_initial_tables'));

    expect($registered)->toBeTrue();
});
