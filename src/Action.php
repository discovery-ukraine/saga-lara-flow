<?php

namespace DiscoveryUkraine\SagaLaraFlow;

/**
 * Base class for user-defined actions — the unit of work a workflow schedules.
 * Author a handle() method; native Laravel dependency injection is available in
 * the constructor and in handle(). The public $tries/$timeout properties carry
 * native Laravel queue retry/timeout semantics when the action runs queued.
 */
abstract class Action
{
    /**
     * Maximum number of attempts when executed as a queued job.
     */
    public int $tries = 1;

    /**
     * Maximum execution time in seconds when executed as a queued job (0 = none).
     */
    public int $timeout = 0;
}
