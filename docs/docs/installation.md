---
id: installation
title: Installation
sidebar_position: 2
---

# Installation

Install the package via Composer:

```bash
composer require discovery-ukraine/saga-lara-flow
```

Run the migrations:

```bash
php artisan migrate
```

The engine's migration ships with the package and is loaded into the migrator directly, so
`migrate` picks it up with **no publish step**. Future versions add their migrations the same way —
`composer update` then `php artisan migrate` is all a host app needs.

Optionally publish the config file:

```bash
php artisan vendor:publish --tag="saga-lara-flow-config"
```

If you need to customize the schema before it runs, you can publish the migration too — a published
copy overrides the package's own:

```bash
php artisan vendor:publish --tag="saga-lara-flow-migrations"
```

## Requirements

- **PHP `^8.5`**
- **Laravel 13** (`illuminate/*: ^13`)
- A queue connection you can run workers against (database, Redis, SQS, …). The `sync` driver works
  for `runSync()` but not for the queued, suspend-and-replay paths.

The package auto-registers its service provider and the `SagaFlow` facade through Laravel's package
discovery — no manual wiring required.

## What gets installed

- A single migration, `create_saga_lara_flow_initial_tables`, creating the engine's tables
  (flow runs, action runs, events, signals, tags, children, compensations, side effects), all
  prefixed with `saga_` by default.
- The `config/saga-lara-flow.php` config file (see [Configuration](./configuration.md)).
- The `SagaFlow` facade and a set of `saga-flow:*` Artisan commands.
