---
id: determinism-rules
title: Determinism rules
sidebar_position: 20
---

# Determinism rules

`handle()` is **replayed** from the start on every resume, so it must be deterministic: given the
same recorded history, it must reach the same operations in the same order. The engine reuses
recorded results for completed operations and only executes the next un-run one.

## Do

- ✅ Call actions, child workflows, signals, and parallel blocks through the DSL — their results are
  recorded and reused on replay.
- ✅ Wrap any nondeterminism in `sideEffect()`: `now()`, random values, UUIDs, tokens, external reads
  whose result you want frozen for the run.
- ✅ Derive control flow from recorded values (action results, signal payloads, side effects).

## Don't

- ❌ Branch on ambient state that can change between replays — wall-clock time, `rand()`, direct
  DB/HTTP reads — outside a `sideEffect()`.
- ❌ Reorder, add, or remove already-recorded steps in a version that has in-flight runs (see
  [Versioning](./versioning.md)).
- ❌ Drive another workflow from inside `handle()` with `SagaFlow::create(...)->runSync()` or
  `->run()`. Those calls take no ordinal, so nothing recognizes the run one of them started, and the
  next replay starts another. [`child()`](./child-workflows.md) is the seam that records it.
- ❌ Catch the engine's control-flow exceptions (`FlowSuspended` / `InternalFlowControl`) as if they
  were errors. If you use a broad `catch (\Throwable $e)`, re-throw them:

  ```php
  } catch (\Throwable $e) {
      if ($this->isFlowControl($e)) {
          throw $e;
      }
      // real error handling
  }
  ```

## The history contract

When a replay diverges from the recorded history — a different operation appears at a given
`sequence` than the one recorded — the engine raises `HistoryContractMismatchException`. Treat it as
a signal that `handle()` became non-deterministic or that its code changed incompatibly for an
in-flight run.

A [signal-gated retry](./retry-on-signal.md) does not disturb this: a retried step keeps its own
ordinal and reuses its `action_runs` row, and the waiting itself consumes no ordinal — so downstream
steps land in the same place whether the step retried or not.

## Example

```php
public function handle(string $orderId): void
{
    // ❌ non-deterministic: a new timestamp on every replay
    // $ref = 'INV-'.now()->timestamp;

    // ✅ recorded once, reused on replay
    $ref = $this->sideEffect('invoice_ref', fn () => 'INV-'.now()->timestamp);

    $this->action(CreateInvoice::class, $orderId, $ref)->run();
}
```
