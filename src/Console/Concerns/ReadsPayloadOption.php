<?php

namespace DiscoveryUkraine\SagaLaraFlow\Console\Concerns;

trait ReadsPayloadOption
{
    /**
     * The decoded --payload option, [] when it is absent. Reports the error and returns null
     * when it is not a JSON object or array.
     *
     * @return array<int|string, mixed>|null
     */
    private function payloadOption(): ?array
    {
        $raw = $this->option('payload');

        if (! is_string($raw) || $raw === '') {
            return [];
        }

        $decoded = json_decode($raw, true);

        if (! is_array($decoded)) {
            $this->error('Payload must be a JSON object or array.');

            return null;
        }

        return $decoded;
    }
}
