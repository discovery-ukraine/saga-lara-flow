<?php

namespace DiscoveryUkraine\SagaLaraFlow\Models\Concerns;

/**
 * Binds a model to the package's configured connection and applies the
 * configurable table prefix. Each model declares its own $baseTable.
 */
trait UsesSagaFlowConnection
{
    public function getConnectionName(): ?string
    {
        return config('saga-lara-flow.database.connection') ?: $this->connection;
    }

    public function getTable(): string
    {
        if (isset($this->table)) {
            return $this->table;
        }

        return ((string) config('saga-lara-flow.database.table_prefix', '')).$this->baseTable;
    }
}
