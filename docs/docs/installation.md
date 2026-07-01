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

Publish and run the migrations:

```bash
php artisan vendor:publish --tag="saga-lara-flow-migrations"
php artisan migrate
```

Optionally publish the config file:

```bash
php artisan vendor:publish --tag="saga-lara-flow-config"
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
