<?php

namespace DiscoveryUkraine\SagaLaraFlow\Middleware;

use Illuminate\Queue\Middleware\WithoutOverlapping;

/**
 * Builds WithoutOverlapping job middleware. The lock is keyed on the job's class and
 * its row, so two jobs of one class never run at once for the same run or step, while
 * a run's RunWorkflowJob and a ResumeWorkflowJob do not wait for each other. Returns no
 * middleware when locking is disabled in config.
 */
class LockMiddlewareFactory
{
    /**
     * Used when a configured TTL is missing or non-positive — see build().
     */
    private const int FALLBACK_TTL_SECONDS = 900;

    /**
     * @return array<int, object>
     */
    public function workflowMiddleware(string $flowRunId): array
    {
        return $this->build("run:{$flowRunId}", (int) config('saga-lara-flow.locks.workflow_ttl_seconds'));
    }

    /**
     * @return array<int, object>
     */
    public function actionMiddleware(string $actionRunId): array
    {
        return $this->build("action:{$actionRunId}", (int) config('saga-lara-flow.locks.action_ttl_seconds'));
    }

    /**
     * Package config is merged only at the top level, so an application that published
     * this file before compensation_ttl_seconds existed keeps a 'locks' array without
     * it. Such a host inherits action_ttl_seconds: a compensation is a step, so the
     * value already tuned for how long a step may run applies to it.
     *
     * @return array<int, object>
     */
    public function compensationMiddleware(string $compensationRunId): array
    {
        $ttl = config('saga-lara-flow.locks.compensation_ttl_seconds')
            ?? config('saga-lara-flow.locks.action_ttl_seconds');

        return $this->build("compensation:{$compensationRunId}", (int) $ttl);
    }

    /**
     * @return array<int, object>
     */
    private function build(string $key, int $ttl): array
    {
        if (! config('saga-lara-flow.locks.enabled')) {
            return [];
        }

        $prefix = config('saga-lara-flow.locks.prefix');

        // Zero reaches Redis as SETNX with no expiry at all (RedisLock::acquire()), so
        // a lock held by a worker that is killed before WithoutOverlapping's finally
        // runs would never be released, wedging that row for good. A missing or
        // nonsensical TTL must degrade to a long one, never to an eternal lock.
        $ttl = $ttl > 0 ? $ttl : self::FALLBACK_TTL_SECONDS;

        $middleware = new WithoutOverlapping("{$prefix}:{$key}")->expireAfter($ttl);

        $block = (int) config('saga-lara-flow.locks.block_seconds');

        if ($block > 0) {
            $middleware->releaseAfter($block);
        }

        return [$middleware];
    }
}
