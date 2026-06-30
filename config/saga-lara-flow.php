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
    | falls back to the application's default database connection.
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
        'block_seconds' => 5,
        'prefix' => 'saga-lara-flow',
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

        'redispatch_actions' => true, // R1: re-dispatch stuck sequential Pending actions
        'wake_waiting' => true,       // R2: re-wake stuck Waiting flows

        'queue_looping' => ['enabled' => false, 'throttle_seconds' => 60],
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
        'capture' => null, // fn (): array $context
        'restore' => null, // fn (array $context): void
    ],

    /*
    |--------------------------------------------------------------------------
    | HTTP routes (disabled by default)
    |--------------------------------------------------------------------------
    */
    'routes' => [
        'enabled' => false,
        'prefix' => 'saga-flows',
        'middleware' => ['api'],
    ],
];
