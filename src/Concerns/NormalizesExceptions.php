<?php

namespace DiscoveryUkraine\SagaLaraFlow\Concerns;

use Throwable;

trait NormalizesExceptions
{
    /**
     * @return array<string, mixed>
     */
    protected function exceptionToArray(Throwable $exception): array
    {
        return [
            'class' => $exception::class,
            'message' => $exception->getMessage(),
            'code' => $exception->getCode(),
        ];
    }
}
