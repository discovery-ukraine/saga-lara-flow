<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal\FlowSuspended;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;

/**
 * Asked by the seams that begin something other than a step — a child workflow, a side
 * effect — whether work may still begin under this run. A step folds the same question
 * into its own claim; these have no comparable write to fold it into.
 *
 * The status is read from the connection that wrote it because the instance the seam was
 * handed is exactly what must not answer: a pass that outlived a rollback holds one from
 * before it. Only the status is read — the pass keeps the instance it has, which is what
 * every transition below it is fenced on.
 */
final readonly class StartWorkGuard
{
    public function __construct(
        private FlowSuspender $suspender,
    ) {}

    /**
     * End the pass unless the run may still start work.
     *
     * @throws FlowSuspended
     */
    public function expect(FlowRun $flowRun, string $reason, int $sequence): void
    {
        if ($this->mayStartWork($flowRun)) {
            return;
        }

        $this->suspender->suspend($reason, $sequence);
    }

    /**
     * A pruned run answers null and starts nothing: whatever it wrote would point at a
     * run that is not there.
     */
    private function mayStartWork(FlowRun $flowRun): bool
    {
        /** @var ?FlowStatus $status */
        $status = $flowRun->newQuery()
            ->useWritePdo()
            ->whereKey($flowRun->getKey())
            ->value('status');

        return $status?->canStartWork() ?? false;
    }
}
