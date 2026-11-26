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

The engine's migrations ship with the package and are loaded into the migrator directly, so
`migrate` picks them up with **no publish step**. Future versions add their migrations the same way —
`composer update` then `php artisan migrate` is all a host app needs.

:::danger Always migrate after an upgrade
Skipping `php artisan migrate` after upgrading is not a "the new feature is unavailable" situation:
the engine fails with an unknown-column error wherever it reaches a column the release added, and
that is not only the newest feature. See
[UPGRADING.md](https://github.com/discovery-ukraine/saga-lara-flow/blob/main/UPGRADING.md).
:::

Optionally publish the config file:

```bash
php artisan vendor:publish --tag="saga-lara-flow-config"
```

Customize the schema through config — `database.table_prefix`, `database.connection`, and the
swappable `models.*` — rather than by editing the migration.

:::caution Don't publish the migration
The migration runs automatically from the package. If you also `vendor:publish` it, `migrate` will
try to run **both** the published copy and the package's own and fail with a duplicate-table error.
Publishing the migration is not part of the install flow.
:::

## Schedule the monitor

Optional, but required for any deadline to work. The package has no durable timers: `expiresAt()`,
`timeoutAfter()`, `#[FlowTimeout]` and the `monitor.expiration.defaults` all store a deadline that
something has to notice.

```php
use Illuminate\Support\Facades\Schedule;

Schedule::command('saga-flow:monitor')->everyMinute();
```

Skip it and nothing breaks — deadlines simply never fire. See
[Expiration & monitoring](./expiration-and-monitoring.md) for the queue-looping alternative and for
the repair doctor.

## Requirements

- **PHP `^8.5`**
- **Laravel 13** (`illuminate/*: ^13`)
- A queue connection you can run workers against (database, Redis, SQS, …). The `sync` driver works
  for `runSync()` but not for the queued, suspend-and-replay paths.

The package auto-registers its service provider and the `SagaFlow` facade through Laravel's package
discovery — no manual wiring required.

## What gets installed

- The migrations. `create_saga_lara_flow_initial_tables` creates the engine's tables (flow runs,
  action runs, events, signals, tags, children, compensations, side effects), all prefixed with
  `saga_` by default; the others add columns and indexes to them.
- The `config/saga-lara-flow.php` config file (see [Configuration](./configuration.md)).
- The `SagaFlow` facade and a set of `saga-flow:*` Artisan commands.
