<?php

use DiscoveryUkraine\SagaLaraFlow\Enums\ChildClosePolicy;
use DiscoveryUkraine\SagaLaraFlow\Enums\CompensationFailurePolicy;
use DiscoveryUkraine\SagaLaraFlow\Enums\DispatchMode;
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
        'expiration' => ['enabled' => true, 'batch_size' => 100],
        'queue_looping' => ['enabled' => false, 'throttle_seconds' => 30],
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
    | Sagas / compensations
    |--------------------------------------------------------------------------
    */
    'sagas' => [
        'default_compensation_failure_policy' => CompensationFailurePolicy::Stop,
        'parallel_compensation' => false,
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
