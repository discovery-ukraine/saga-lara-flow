<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\ChildClosePolicy;
use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationFailurePolicy;
use DiscoveryUkraine\SagaLaraFlow\Enums\DispatchMode;
use DiscoveryUkraine\SagaLaraFlow\Enums\ParallelFailurePolicy;
use DiscoveryUkraine\SagaLaraFlow\Models;

return [

    /*
    |--------------------------------------------------------------------------
    | Database
    |--------------------------------------------------------------------------
    | Dedicated connection/schema for the package tables. A null connection
    | falls back to the application's default database connection. The prefix
    | is capped at 24 bytes: it rides along in every index name, which MySQL
    | caps at 64 characters and PostgreSQL truncates past 63 bytes.
    */
    'database' => [
        'connection' => env('SAGA_LARA_FLOW_DB_CONNECTION'),
        'table_prefix' => env('SAGA_LARA_FLOW_TABLE_PREFIX', 'saga_'),
    ],

    /*
    |--------------------------------------------------------------------------
    | Models
    |--------------------------------------------------------------------------
    | Swap any model for your own subclass to extend behaviour.
    */
    'models' => [
        'flow_run' => Models\FlowRun::class,
        'action_run' => Models\ActionRun::class,
        'flow_event' => Models\FlowEvent::class,
        'flow_signal' => Models\FlowSignal::class,
        'flow_tag' => Models\FlowTag::class,
        'flow_child' => Models\FlowChild::class,
        'compensation_run' => Models\CompensationRun::class,
        'side_effect' => Models\SideEffect::class,
    ],

    /*
    |--------------------------------------------------------------------------
    | Queue
    |--------------------------------------------------------------------------
    */
    'queue' => [
        'connection' => env('SAGA_LARA_FLOW_QUEUE_CONNECTION'),
        'queue' => env('SAGA_LARA_FLOW_QUEUE', 'default'),
        'after_commit' => env('SAGA_LARA_FLOW_AFTER_COMMIT', true),
        'dispatch_mode' => DispatchMode::Queue,
    ],

    /*
    |--------------------------------------------------------------------------
    | Locks (WithoutOverlapping)
    |--------------------------------------------------------------------------
    */
    'locks' => [
        'enabled' => true,
        'store' => env('SAGA_LARA_FLOW_LOCK_STORE'),
        'workflow_ttl_seconds' => 900,
        'action_ttl_seconds' => 900,

        // Added in 1.2.0. An application that published this file earlier has a
        // 'locks' array without this key — config merging is shallow, so it is not
        // filled in for them. Missing/zero falls back to action_ttl_seconds (a
        // compensation is a step, so the value tuned for steps applies), never to
        // zero: zero means "no expiry" on Redis, and a lock a killed worker never
        // releases would wedge the row forever.
        'compensation_ttl_seconds' => 900,

        'block_seconds' => 5,
        'prefix' => 'saga-lara-flow',
    ],

    /*
    |--------------------------------------------------------------------------
    | Logging
    |--------------------------------------------------------------------------
    */
    'logging' => [
        // A claim lost to a competing worker, an outcome write rejected because the
        // row changed hands, a batch already closed by a duplicate delivery: all are
        // normal under at-least-once delivery — not errors, and none of them fails a
        // job. This log line is the only trace they leave, so the case can be found
        // and debugged afterwards. null = silent.
        'anomaly_level' => env('SAGA_LARA_FLOW_ANOMALY_LOG_LEVEL', 'info'),

        // null = the application's default channel.
        'channel' => env('SAGA_LARA_FLOW_LOG_CHANNEL'),
    ],

    /*
    |--------------------------------------------------------------------------
    | Serialization
    |--------------------------------------------------------------------------
    */
    'serialization' => [
        'json_flags' => JSON_THROW_ON_ERROR | JSON_UNESCAPED_UNICODE,
    ],

    /*
    |--------------------------------------------------------------------------
    | Monitor (expiration / stuck runs)
    |--------------------------------------------------------------------------
    */
    'monitor' => [
        'enabled' => true,
        'expiration' => [
            'enabled' => true,
            'batch_size' => 100,

            // Default deadlines (in seconds) applied at write time when none is set
            // explicitly: 'run' on create, 'action' on schedule, 'signal' on await.
            // null = off (no implicit deadline). There is no per-entity opt-out flag;
            // to bypass a default for one entity pass an explicit (far-future) deadline.
            'defaults' => ['run' => null, 'action' => null, 'signal' => null],
        ],
        'queue_looping' => ['enabled' => false, 'throttle_seconds' => 30],
    ],

    /*
    |--------------------------------------------------------------------------
    | Repair / doctor (recover progress lost to a dropped job)
    |--------------------------------------------------------------------------
    | A separate mechanism from the expiration monitor above: it recovers a flow
    | whose progress was lost to a dropped job (an action that never ran, a resume
    | that never fired), not one that hit a deadline. Opt-in and configured,
    | scheduled (saga-flow:repair), and looped independently of the monitor. It
    | only ever re-dispatches existing jobs or re-wakes flows (replay decides),
    | never creating duplicate work or mutating a business result.
    */
    'repair' => [
        'enabled' => false,

        // Minimum age (seconds) before an entity is considered stuck — guards
        // against racing a job that is simply still in flight.
        'grace_seconds' => 60,
        'batch_size' => 100,

        // Per-entity cap: after this many repair attempts the doctor leaves the
        // entity alone (use saga-flow:kick to re-drive it manually).
        'max_attempts' => 10,

        // Exponential backoff between repair attempts for one entity.
        'backoff' => ['base_seconds' => 10, 'max_seconds' => 300],

        // R1: re-dispatch stuck sequential Pending actions whose queue job was lost.
        'redispatch_lost_actions' => true,
        // R2: re-wake flows stuck in the Waiting status after a resume that never fired.
        'wake_stuck_flows' => true,
        // R3: re-dispatch stuck sequential Running actions past their own
        // actions.reclaim.stale_running threshold (a worker that died mid-execution).
        // Has no effect while that mechanism is off (its default) — this only
        // decides whether the doctor additionally acts on rows it makes claimable.
        // Parallel actions and compensations are out of scope: recovering them would
        // mean re-adding a job to their Bus::batch, which needs the batch's id — not
        // stored anywhere in this package's tables — looked up by name in Laravel's
        // own job_batches table, a detail of the database batch driver specifically
        // and not guaranteed for every host.
        'redispatch_stale_running_actions' => true,

        'queue_looping' => ['enabled' => false, 'throttle_seconds' => 60],
    ],

    /*
    |--------------------------------------------------------------------------
    | Prune (saga-flow:prune)
    |--------------------------------------------------------------------------
    | Default retention window (in days) for old terminal runs. saga-flow:prune
    | deletes runs created before now() minus this many days, together with all
    | of their related rows. Override per invocation with --days / --before.
    */
    'prune' => [
        'retention_days' => 90,
    ],

    /*
    |--------------------------------------------------------------------------
    | Signals
    |--------------------------------------------------------------------------
    */
    'signals' => [
        'wake_workflow_on_signal' => true,
    ],

    /*
    |--------------------------------------------------------------------------
    | Child workflows
    |--------------------------------------------------------------------------
    */
    'children' => [
        'default_close_policy' => ChildClosePolicy::Abandon,
    ],

    /*
    |--------------------------------------------------------------------------
    | Actions
    |--------------------------------------------------------------------------
    | retryOnSignal() parks a failed step until a named signal arrives, then
    | retries that step alone. max_retries caps how many signal-gated retries a
    | step may spend when the call site does not pass its own maxRetries; null
    | leaves it unbounded, with the per-wait timeout and the run's expires_at as
    | the remaining brakes. A value set on the call site always wins. Use null
    | rather than a negative number: a negative cap is rejected.
    */
    'actions' => [
        'retry_on_signal' => [
            'max_retries' => null,
        ],

        // A step's own atomic claim (see startAction()) accepts a row still Running
        // only when it has sat that long since started_at — meant to recognize a
        // worker that died mid-execution, not to interrupt one that is still alive.
        //
        // Off by default, and that default has a consequence worth knowing: with no
        // window configured, a Running row is never claimed again by anything. A
        // worker killed mid-step therefore leaves it Running for good — replay reads
        // it as still in flight and parks the run, and saga-flow:kick does the same.
        // The two ways to get automatic recovery are this setting (the step runs
        // again) and monitor.expiration.defaults.action with saga-flow:monitor (the
        // step is marked Expired and the run fails as a business error).
        //
        // Distinct from locks.action_ttl_seconds (a cache-lock TTL, a different layer
        // entirely) — see the reclaim documentation for how the two relate.
        'reclaim' => [
            'stale_running' => [
                'enabled' => false,
                'after_seconds' => 900,
            ],
        ],
    ],

    /*
    |--------------------------------------------------------------------------
    | Parallel actions
    |--------------------------------------------------------------------------
    | Default policy for a parallel() block when a step fails. FailFast cancels
    | the block on the first hard failure (pending siblings never start);
    | WaitAllThenFail lets every step settle before failing. Override per block
    | via ->failFast() / ->waitAllThenFail().
    */
    'parallel' => [
        'default_failure_policy' => ParallelFailurePolicy::FailFast,
    ],

    /*
    |--------------------------------------------------------------------------
    | Sagas / compensations
    |--------------------------------------------------------------------------
    */
    'sagas' => [
        'default_compensation_failure_policy' => CompensationFailurePolicy::Stop,
        'parallel_compensation' => false,

        // By default only successfully completed steps are compensated on failure
        // (classic saga). Enable this to also compensate a step that FAILED — useful
        // for non-atomic actions that may leave partial effects. Such compensations
        // must be idempotent and safe when the step actually did nothing. Override
        // per action/group via compensateStepOnSelfFailure() (precedence action > group > config).
        'compensate_failed_step' => false,

        // Same mechanism as actions.reclaim, independently switched: a compensation
        // row still Running is claimable again only once it has sat that long since
        // started_at. Off by default. Compensations have no automatic retry of their
        // own — this only concerns a worker that died mid-compensation.
        'reclaim' => [
            'stale_running' => [
                'enabled' => false,
                'after_seconds' => 900,
            ],
        ],
    ],

    /*
    |--------------------------------------------------------------------------
    | History
    |--------------------------------------------------------------------------
    */
    'history' => [
        'store_payloads' => true,
        'store_exceptions' => true,
        'max_payload_preview_length' => 2000,

        // Side effects are recorded once and then resolved from storage on every
        // replay. By default, a reuse only dispatches the SideEffectReused event
        // (no flow_events row), so the event log stays bounded — a single run
        // replays each side effect once per later step. Enable this to also
        // persist a side_effect.reused flow_events row on every reuse when you
        // need a full audit trail (note: this grows with the number of replays).
        'record_side_effect_reuse' => env('SAGA_LARA_FLOW_RECORD_SIDE_EFFECT_REUSE', false),
    ],

    /*
    |--------------------------------------------------------------------------
    | Multi-tenancy / Octane hooks
    |--------------------------------------------------------------------------
    | Callables resolved from the container or plain closures.
    */
    'tenancy' => [
        // Auto capture/restore around workflow & action handle(). Off by default —
        // opt in globally here, or per class with #[Tenancy(auto: true|false)].
        // When off, the run's tenant is still captured at creation and readable via
        // SagaFlow::tenancyContext() so host code can enter/leave tenancy itself.
        'auto' => false,
        'capture' => null, // fn (): array $context — snapshot the current tenant
        'restore' => null, // fn (array $context): void — enter the run's tenant
        'end' => null,     // fn (?array $previous): void — optional explicit revert
    ],
];
