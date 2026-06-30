<?php

namespace DiscoveryUkraine\SagaLaraFlow\Runtime;

use DateTimeInterface;
use DiscoveryUkraine\SagaLaraFlow\Enums\FlowStatus;
use DiscoveryUkraine\SagaLaraFlow\Models\FlowRun;
use Illuminate\Database\ConnectionInterface;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Facades\DB;
use Throwable;

/**
 * Deletes old terminal runs and everything attached to them. There is no DB-level
 * foreign-key cascade (ULID keys, a swappable connection/prefix), so the cascade
 * is done in code: every child table keyed by flow_run_id, plus flow_children by
 * its parent/child run ids, then the runs — all inside one transaction on the
 * package's configured connection so a prune is all-or-nothing.
 */
final readonly class FlowPruner
{
    /**
     * @throws Throwable
     */
    public function prune(DateTimeInterface $before, bool $dryRun = false): FlowPruneReport
    {
        /** @var class-string<FlowRun> $flowRun */
        $flowRun = config('saga-lara-flow.models.flow_run');

        $ids = $flowRun::query()
            ->whereIn('status', [
                FlowStatus::Completed,
                FlowStatus::Failed,
                FlowStatus::Cancelled,
                FlowStatus::Expired,
            ])
            ->where('created_at', '<', $before)
            ->pluck('id')
            ->all();

        if ($ids === []) {
            return new FlowPruneReport;
        }

        if ($dryRun) {
            return new FlowPruneReport(count($ids), $this->countRelated($ids));
        }

        return $this->connection()->transaction(function () use ($ids, $flowRun): FlowPruneReport {
            $related = 0;

            foreach ($this->childModels() as $model) {
                $related += $model::query()->whereIn('flow_run_id', $ids)->delete();
            }

            /** @var class-string<Model> $flowChild */
            $flowChild = config('saga-lara-flow.models.flow_child');

            $related += $flowChild::query()
                ->whereIn('parent_flow_run_id', $ids)
                ->orWhereIn('child_flow_run_id', $ids)
                ->delete();

            $runs = $flowRun::query()->whereIn('id', $ids)->delete();

            return new FlowPruneReport($runs, $related);
        });
    }

    /**
     * @param  array<int, mixed>  $ids
     */
    private function countRelated(array $ids): int
    {
        $related = 0;

        foreach ($this->childModels() as $model) {
            $related += $model::query()->whereIn('flow_run_id', $ids)->count();
        }

        /** @var class-string<Model> $flowChild */
        $flowChild = config('saga-lara-flow.models.flow_child');

        $related += $flowChild::query()
            ->whereIn('parent_flow_run_id', $ids)
            ->orWhereIn('child_flow_run_id', $ids)
            ->count();

        return $related;
    }

    /**
     * The child tables keyed by flow_run_id (flow_children is keyed differently
     * and handled separately).
     *
     * @return array<int, class-string<Model>>
     */
    private function childModels(): array
    {
        return array_map(
            static fn (string $key): string => config("saga-lara-flow.models.{$key}"),
            ['action_run', 'flow_event', 'flow_signal', 'compensation_run', 'side_effect', 'flow_tag'],
        );
    }

    /**
     * The package's configured database connection (matching the models' own
     * UsesSagaFlowConnection resolution), so the cascade and its transaction target
     * the right database even on a dedicated connection.
     */
    private function connection(): ConnectionInterface
    {
        return DB::connection(config('saga-lara-flow.database.connection') ?: null);
    }
}
