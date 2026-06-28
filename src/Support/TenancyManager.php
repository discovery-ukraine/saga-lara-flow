<?php

namespace DiscoveryUkraine\SagaLaraFlow\Support;

use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

/**
 * Thin tenancy hook runner. Capture/restore are no-ops unless the host app
 * configures callables under config('saga-lara-flow.tenancy.*'). Full tenancy
 * snapshotting on create() lands in a later phase; restore is wired here so the
 * executor and jobs run inside the recorded tenant.
 */
class TenancyManager
{
    /**
     * Restore the tenant recorded on the run before execution/replay.
     */
    public function restore(FlowRun $flowRun): void
    {
        $restore = config('saga-lara-flow.tenancy.restore');

        if (is_callable($restore)) {
            $restore($flowRun->tenancy_context ?? []);
        }
    }

    /**
     * Capture the current tenant context, or null when no hook is configured.
     *
     * @return array<string, mixed>|null
     */
    public function capture(): ?array
    {
        $capture = config('saga-lara-flow.tenancy.capture');

        return is_callable($capture) ? $capture() : null;
    }
}
