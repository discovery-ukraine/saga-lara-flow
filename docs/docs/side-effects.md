---
id: side-effects
title: Side effects
sidebar_position: 8
---

# Side effects

Because `handle()` is replayed from the start on every resume, anything **non-deterministic** must be
wrapped in `sideEffect()` so replay reuses the *recorded* value instead of computing a new one:

```php
use Illuminate\Support\Str;

public function handle(): void
{
    $reference = $this->sideEffect('reference', fn () => (string) Str::uuid());
    $timestamp = $this->sideEffect('started_at', fn () => now()->toIso8601String());

    $this->action(CreateInvoice::class, $reference, $timestamp)->run();
}
```

The first execution runs the factory and records its return value; every later replay of the run
returns the same stored value. The `key` you pass identifies the recorded value — keep it stable
across replays.

## What belongs in a side effect

- `now()`, `Carbon::now()`, timestamps
- Random values, UUIDs, tokens
- Reads from external systems whose result you want frozen for the run (a rate, a feature flag, a
  configuration snapshot)

## What does not

- Calls to actions, child workflows, signals, or parallel blocks — those are already recorded and
  replayed by the engine. Wrapping them in a side effect would be redundant and wrong.

By default a side-effect reuse only dispatches the `SideEffectReused` event (no extra `flow_events`
row), keeping the event log bounded. Enable `history.record_side_effect_reuse` if you need a full
audit trail of every reuse. See also [Determinism rules](./determinism-rules.md).
