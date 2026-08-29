<!--
Please read CONTRIBUTING.md before filling this in.
Open an issue first for anything breaking or larger than a focused fix.
-->

## What this changes

<!-- The behaviour after this PR, in a few sentences. Link the issue it closes. -->

## Checklist

- [ ] One feature/fix; the diff is focused
- [ ] `composer lint` passes (Pint + PHPStan level 5)
- [ ] `composer test` passes
- [ ] `CHANGELOG.md` is untouched
- [ ] README **and** `/docs` updated if public behaviour changed, and `cd docs && npm run build` passes
- [ ] Breaking changes are described in `UPGRADING.md`, under the section for the version in progress

## Tests

- [ ] Every regression test was verified to fail without its own fix
- [ ] No existing assertion was deleted or weakened to fit the new behaviour
- [ ] No mocks; faults are injected through model config or a fixture static

<!-- Say which test covers what, and how you verified it fails without the fix. -->

## Concurrency and drivers

<!-- Delete this section only if the PR touches no code under
     src/Runtime, src/States, src/Repositories, src/Jobs or src/Models. -->

- [ ] No decision is taken from a model's in-memory status where the row could have moved
- [ ] Conditional updates do not fence on a column that may already hold the value written
      (MySQL counts *changed* rows, not matched ones)
- [ ] Reads that decide something use `useWritePdo()`
- [ ] No new write to `flow_runs.updated_at` as a side effect (it is the repair staleness clock)
- [ ] Events raised inside a new transaction implement `ShouldDispatchAfterCommit`
- [ ] No query failure is caught and ignored inside a transaction
      (PostgreSQL refuses everything after it and turns the commit into a rollback)
- [ ] Existing lock order (`flow_runs` → `action_runs` / `flow_signals`) is preserved

CI runs the suite on SQLite, MySQL and PostgreSQL, so a driver difference shows up on its own —
but only for behaviour a test actually reaches. Locking, transaction isolation and anything that
needs two workers at once are still staged from one process and prove nothing. Say which of those
this change depends on:

<!-- your reasoning here, or "not driver-dependent" -->
