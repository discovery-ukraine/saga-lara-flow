# Contributing

Contributions are welcome and will be fully credited. Please read and understand this document
before opening an issue or pull request.

## Reporting issues

- Search existing issues first — yours may already be reported.
- Include the package version, PHP/Laravel versions, and a minimal reproduction.
- For security vulnerabilities, follow [SECURITY.md](SECURITY.md) instead of opening a public issue.

## Before you open a pull request

- **Open an issue first** for anything breaking, anything that changes a public method's contract,
  and anything larger than a focused fix. Agreeing on the shape first saves a rewrite.
- **One feature/fix per PR.** Keep the diff focused.
- **Do not edit `CHANGELOG.md`.** It is written by maintainers at release time and has no
  `Unreleased` section. A PR that adds one will be asked to remove it.
- **`UPGRADING.md`** is where a breaking change is described, under the section for the version in
  progress; do not open a section of your own. It has three parts — *Action required* for anything
  a host must change or decide, *Behaviour changed* for a line or two plus a link to the docs page
  that covers it, *Additions* for what needs nothing done. Mechanism belongs on that docs page, and
  rationale in the issue and the PR; neither belongs here.

## Development setup

This is a Laravel **package**, tested against a throwaway app via
[Orchestra Testbench](https://packages.tools/testbench). It requires **PHP `^8.5`**.

If your host PHP differs, run the tooling inside the project's Docker image:

```bash
docker compose run --rm app composer install
docker compose run --rm app composer test
docker compose run --rm app composer lint
```

The suite runs on an in-memory SQLite database, so the command above starts one container and
no database server. Some of the engine's writes behave differently on a real server, so the same
suite also runs against MySQL and PostgreSQL, each behind a compose profile of its own:

```bash
docker compose --profile mysql run --rm app-mysql vendor/bin/pest
docker compose --profile pgsql run --rm app-pgsql vendor/bin/pest
```

Run **MySQL before a commit and before opening a PR** — it is where a conditional write answers
differently, which is what most of the engine rests on. **PostgreSQL at a milestone**, and whenever
a change touches transactions, locking or migrations. CI runs both on any pull request that touches
code or the harness, so a local run buys you the answer sooner, not a different one.

Nothing starts a server except a command that asks for it by name. `SAGA_TEST_DB=mysql` or `=pgsql`
is all the suite reads, so a host with its own server can run against that instead —
`SAGA_TEST_DB_HOST`, `_PORT`, `_DATABASE`, `_USERNAME` and `_PASSWORD` point it there.

**Whatever database it is pointed at is emptied.** The first test drops every table in it and
rebuilds the schema; each test after that deletes every row. Give the suite a database of its own.

Or, on a matching PHP toolchain, directly:

```bash
composer install
composer test        # Pest
composer analyse     # PHPStan (larastan, level 5)
composer format      # Laravel Pint
composer lint        # Pint + PHPStan together
```

## Engine rules

This is a workflow engine: an operator, a queue worker, the monitor and the doctor all touch the
same rows, and nothing serialises them. The queue's per-run lock covers jobs only — not the CLI,
not `FlowHandle`, not the monitor's inline sweep. Changes to `src/Runtime`, `src/States`,
`src/Repositories` and `src/Jobs` are held to the rules below.

### Deciding on state

- **Never decide from a model's in-memory status.** `FlowQuery::handles()` hands out snapshots taken
  in one pass, so by the time a bulk loop reaches the hundredth handle the first ones are stale. A
  check that must hold at write time belongs in the write.
- **Fence writes with a conditional `UPDATE`** on the value you read (`where('status', $from)`).
  It is the only form every supported driver enforces atomically — `lockForUpdate()` is a no-op on
  SQLite.
- **Zero affected rows is not proof the fence failed.** MySQL's `rowCount()` counts rows it
  *changed*, not rows it *matched*, and Laravel never sets `PDO::MYSQL_ATTR_FOUND_ROWS`. An `UPDATE`
  whose every value already equals what is stored reports zero there and one on SQLite and
  PostgreSQL. Never fence on an update whose only written column may already hold its value, and
  refuse only after reading back what actually holds the row. See `FlowStateMachine::write()`.
- **Never swallow a query failure inside a `transaction()`.** PostgreSQL aborts the whole
  transaction on the first failed statement and turns the eventual commit into a rollback, while
  reporting success; SQLite and MySQL carry on. A caught-and-ignored failure therefore commits on
  two drivers and silently discards everything on the third. This reaches listener code too: the
  events the engine fires inside its own transactions run there.
- **A transaction that runs caller code must verify its own outcome.** The commit reporting success
  is the caller's word for it, and the paragraph above is why that word is worth nothing. Read the
  row back afterwards and act on what it says — see `ActionRecorder::claimSurvivedCommit()`. Moving
  the event out of the transaction is not the same fix: a model observer on a row the transaction
  writes runs there whatever the event does, which is what `TransactionIntegrityTest`'s observer
  case pins. What such a read proves is visibility on the writing connection, which equals
  durability only while the engine's transaction is the outermost one — see the boundary below.
- **A read that decides something must use `useWritePdo()`.** A lagging replica will answer with the
  very state the fence was guarding against.
- **Do not `refresh()` a model the caller still holds.** It discards their unsaved attributes;
  `FlowHandle` hands that same instance back.

### Status boundaries

`FlowStatus::terminal()`, `FlowStatus::signalable()` and `FlowStatus::mayStartWork()` are different
sets and are not interchangeable. `Cancelling` is **not** terminal — it is a run mid-rollback.
Swapping one boundary for the other changes public behaviour and needs its own exception and its own
documentation.

The one that decides a write is `mayStartWork()` versus `live()`: ask the narrower one wherever work
would **begin** (the action claim, the repair rules that send another job, the retry that spends a
signal to start a fresh cycle), and `live()` wherever a row already started is settled or written
down. A rollback plans the stack it will undo once, so a
step that starts under `Cancelling` lands outside every plan there is; but the same rollback needs
its compensations claimed and its bookkeeping written, and those are not new work.

### A host transaction around an engine call is out of scope

The engine's entry points must be called with no transaction of the caller's open. This is a
**documented boundary, not an open defect** — do not file it as one, and do not add code that tries
to detect or survive it.

Measured identically on all three drivers: with a host transaction rolled back around them,
`runSync()` leaves no row of the run at all, `compensate()` performs the rollback and records
nothing (leaving the run compensatable a second time — `["undo:a", "undo:a"]` against one
`compensation_runs` row), and `signal()` and `cancel()` are discarded while the caller is handed a
`FlowRun` saying otherwise. Only the queued `run()` is safe, and only while `queue.after_commit` is
on.

The engine cannot get out from under it. Inside a host transaction its own `transaction()` is a
savepoint (nesting level 2), so it commits nothing and every read-back proves visibility rather than
durability; a second connection would escape the scope but could not then see the host's uncommitted
rows at all. Nor can a `transactionLevel() > 0` check sit at the writes: the engine's own writers
legitimately run above zero — `FlowStateMachine::finish()` settles every open step inside its
transaction, and `park()` writes the step and its signal inside one. And detection would come too
late for what the boundary actually costs, which is work already performed. So it is documented in
`docs/docs/queues-locks-idempotency.md` and enforced nowhere.

### Side effects

- **`flow_runs.updated_at` is the repair staleness clock** (`EloquentFlowRepository::dueForRepair()`).
  Do not write it as a side effect of something else; you will push stuck runs out of the doctor's
  reach.
- **Models are config-swappable** (`saga-lara-flow.models.*`). Never build an update payload from
  `newInstance()->getAttributes()` — a host model's default attributes ride along and overwrite real
  columns. Name the columns you mean to write.
- **Events dispatched inside a transaction are seen before it commits.** If you wrap existing writes
  in a transaction, check every event they raise: it must implement `ShouldDispatchAfterCommit`, or
  listeners will act on a delivery that may still roll back — while holding your row locks.
- **Keep the lock order.** Existing writers take `flow_runs` first, then `action_runs` and
  `flow_signals`. Do not reverse it.
- **A new index has to leave room for the table prefix.** With the documented ceiling of 24 bytes a
  name has 40 left: MySQL refuses an identifier past 64 characters, and PostgreSQL truncates one
  past 63 bytes, where two names on a table can meet. A longer name lowers that ceiling for
  everyone, so name the index explicitly rather than letting Laravel derive one —
  `TablePrefixCeilingTest` fails on MySQL when it does not fit.

## Tests

- **Add tests.** The suite runs in random order and fails on risky or warning-producing tests.
- **There are no mocks in this project.** Inject faults through model config
  (`saga-lara-flow.models.*`) or a static on a fixture — see `tests/Fixtures/`.
- **Verify every regression test fails without its own fix**, and say so in the PR description.
  A test written after the code often only restates it.
- **Do not weaken an existing test to fit new behaviour.** If a test carries a comment explaining
  what it guards, that guard has to survive somewhere — move it, don't delete it.
- **Queued tests** run against a real database queue driven with `queue:work --stop-when-empty`,
  not the `sync` driver.
- **Run the suite on MySQL before you commit.** SQLite answers a conditional write differently,
  and a test that fences on one is meaningless there — see `ConditionalWriteFenceTest`, which is
  load-bearing on MySQL and trivially green on SQLite.
- **`FlowRun`'s relations carry their order; don't append one that fights it.** Each reads by the
  column its rows are meaningful in (see `src/Models/FlowRun.php`), because without one the answer
  is the driver's to choose — SQLite and MySQL usually give insertion order, PostgreSQL gives
  physical order, which moves as rows are updated. An order you append lands after the default, so
  to read in the other direction call `reorder()` first — and before any id-cursor traversal
  (`chunkById()` and friends), whose cursor the default would otherwise outrank. A relation added
  later needs an order of its own: `RelationOrderTest` fails until it has one.
- **A test may be written for one driver, but say so in the file.** `LongTablePrefixTest` skips
  everywhere but PostgreSQL because no other driver truncates an identifier, and
  `TransactionIntegrityTest` skips one case on PostgreSQL against a defect that is filed rather
  than hidden. A skip without a stated reason is indistinguishable from a test nobody runs.
- **Never assert the key order of a map read back out of a `json` column.** MySQL's `json` type is
  a binary format that sorts an object's keys; SQLite keeps the text as written. Key order is the
  driver's business, not the engine's contract — assert key by key with `toBe`, which stays strict
  about the values, rather than dropping the whole map to a loose `toEqual`.

Run a single test:

```bash
vendor/bin/pest tests/Feature/CreateWorkflowTest.php
vendor/bin/pest --filter="cancels a non-terminal run"
```

### What CI does not check

CI runs the suite on SQLite across the `os × stability` matrix, and once more on each of **MySQL**
and **PostgreSQL**. Two gaps remain, and a green suite is not evidence in either:

- **Real concurrency.** Every interleaving in the suite is staged from one process. Nothing runs
  two workers at once, so a lock held too long or a deadlock retried wrongly would pass.
- **Transaction isolation.** For the same reason: `READ COMMITTED` and `REPEATABLE READ` differ
  only for a read-back that races another writer, and nothing here races.

## Documentation

- **Update both** the README and the docs under `/docs` — they carry overlapping tables and
  examples, and a change to one alone leaves the other lying.
- **State facts.** No before/after narration, no justification of the change, no retelling of the
  decision, no issue or PR numbers. Describe how the package behaves now.
- **Check the prose you did not write.** A behaviour change usually falsifies a sentence somewhere
  else in the same page.
- `cd docs && npm run build` must pass — broken links fail the build.

## Code style

- **Run `composer lint`** before pushing — CI enforces Pint formatting and PHPStan level 5.
- **Follow existing conventions.** Match the surrounding code's naming, structure, and idioms.
- **Comments earn their place.** Keep what does not follow from the code and would otherwise be
  refactored away; delete what the documentation already says.
- **Do not add Eloquent query scopes to the models.** `FlowQuery` deliberately wraps a Builder
  instead — larastan cannot type a scope on a generic Builder resolved from a config-swappable
  model. Put the predicate where it is used.

**Happy coding!**
