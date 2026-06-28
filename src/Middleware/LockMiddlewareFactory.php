<?php

namespace DiscoveryUkraine\SagaLaraFlow\Middleware;

use Illuminate\Queue\Middleware\WithoutOverlapping;

/**
 * Builds WithoutOverlapping job middleware so that only one job at a time runs
 * for a given workflow run (and for a given action run). This single-threading
 * is what keeps replay correct and prevents races between a resume and an
 * incoming signal. Returns no middleware when locking is disabled in config.
 */
class LockMiddlewareFactory
{
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
     * @return array<int, object>
     */
    private function build(string $key, int $ttl): array
    {
        if (! config('saga-lara-flow.locks.enabled')) {
            return [];
        }

        $prefix = config('saga-lara-flow.locks.prefix');

        $middleware = (new WithoutOverlapping("{$prefix}:{$key}"))->expireAfter($ttl);

        $block = (int) config('saga-lara-flow.locks.block_seconds');

        if ($block > 0) {
            $middleware->releaseAfter($block);
        }

        return [$middleware];
    }
}
