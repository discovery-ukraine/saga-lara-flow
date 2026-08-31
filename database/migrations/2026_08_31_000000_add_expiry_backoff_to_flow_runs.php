<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        $schema = Schema::connection($this->connection());

        $schema->table($this->prefix().'flow_runs', function (Blueprint $table): void {
            $table->unsignedInteger('expiry_attempts')->default(0)->after('repair_available_at');
            $table->timestamp('expiry_available_at')->nullable()->after('expiry_attempts');
        });
    }

    public function down(): void
    {
        $schema = Schema::connection($this->connection());

        $schema->table($this->prefix().'flow_runs', function (Blueprint $table): void {
            $table->dropColumn(['expiry_attempts', 'expiry_available_at']);
        });
    }

    private function connection(): ?string
    {
        return config('saga-lara-flow.database.connection');
    }

    private function prefix(): string
    {
        return (string) config('saga-lara-flow.database.table_prefix', '');
    }
};
