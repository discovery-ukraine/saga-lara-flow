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

        // Missing or zero falls back to action_ttl_seconds, never to zero: on Redis zero means no
        // expiry, and a lock a killed worker never releases would wedge the row.
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
        // Lost claims, refused writes, batches closed by a duplicate delivery: normal under
        // at-least-once delivery, and this line is their only trace. null = silent.
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

            // A run whose rollback cannot be planned steps aside for this long (exponential, per
            // run). There is no attempt cap.
            'backoff' => ['base_seconds' => 60, 'max_seconds' => 3600],

            // An expiry whose rollback nothing has moved for this long is carried to the end by
            // the sweep. Keep it above the longest your queue makes a job wait.
            'rollback_grace_seconds' => 60,

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
        // R3: re-dispatch stuck sequential Running actions past actions.reclaim.stale_running (no
        // effect while that is off, its default). Parallel actions and compensations are out of reach.
        'redispatch_stale_running_actions' => true,
        // R4: send the first job again for a Pending run whose RunWorkflowJob was lost.
        'redispatch_lost_starts' => true,
        // R5: carry a rollback to Failed or Cancelled to its end once nothing has moved it for
        // grace_seconds. A rollback to Expired is the monitor's.
        'resume_stalled_rollbacks' => true,

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

        // A Running row is claimable again once it has sat this long since started_at. Off by
        // default: a worker killed mid-step then leaves its row Running for good. See the reclaim
        // documentation for this and locks.action_ttl_seconds.
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

        // Also compensate a step that itself failed. Such a compensation must be safe when the step
        // did nothing. Overridden per action or group by compensateStepOnSelfFailure().
        'compensate_failed_step' => false,

        // actions.reclaim for compensations, switched independently. Off by default.
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
    | Each hook is an invokable class name, or a [Class::class, 'method'] pair,
    | resolved from the container — give the full namespace, this file imports
    | nothing. Only null turns a hook off; one that cannot be called throws. A
    | closure works too, but `php artisan config:cache` cannot store one.
    */
    'tenancy' => [
        // Auto capture/restore around workflow & action handle(). Off by default —
        // opt in globally here, or per class with #[Tenancy(auto: true|false)].
        // When off, the run's tenant is still captured at creation and readable via
        // SagaFlow::tenancyContext() so host code can enter/leave tenancy itself.
        'auto' => false,
        'capture' => null, // (): array $context — snapshot the current tenant
        'restore' => null, // (array $context): void — enter the run's tenant
        'end' => null,     // (?array $previous): void — optional explicit revert
    ],
];
