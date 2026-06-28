<?php

namespace DiscoveryUkraine\SagaLaraFlow\Serialization;

use DiscoveryUkraine\SagaLaraFlow\Contracts\Serializer;
use Illuminate\Contracts\Support\Arrayable;
use Illuminate\Database\Eloquent\Model;
use JsonSerializable;

/**
 * Native serializer (no third-party codecs). Scalars and arrays pass through;
 * Arrayable/JsonSerializable objects are reduced to their array/JSON form;
 * Eloquent models are stored as a {_model, id, connection} reference and
 * rehydrated via find(). For long-running flows prefer passing IDs over models.
 */
class LaravelSerializer implements Serializer
{
    /**
     * Marker key identifying a serialized Eloquent model reference.
     */
    private const string MODEL_KEY = '_model';

    public function serialize(mixed $value): mixed
    {
        if ($value === null || is_scalar($value)) {
            return $value;
        }

        if ($value instanceof Model) {
            return [
                self::MODEL_KEY => $value::class,
                'id' => $value->getKey(),
                'connection' => $value->getConnectionName(),
            ];
        }

        if ($value instanceof Arrayable) {
            return $this->serialize($value->toArray());
        }

        if ($value instanceof JsonSerializable) {
            return $this->serialize($value->jsonSerialize());
        }

        if (is_array($value)) {
            return array_map(fn (mixed $item): mixed => $this->serialize($item), $value);
        }

        // Fallback: cast objects to array of public properties.
        return $this->serialize((array) $value);
    }

    public function deserialize(mixed $payload): mixed
    {
        if (! is_array($payload)) {
            return $payload;
        }

        if ($this->isModelReference($payload)) {
            return $this->restoreModel($payload);
        }

        return array_map(fn (mixed $item): mixed => $this->deserialize($item), $payload);
    }

    /**
     * @param  array<int|string, mixed>  $payload
     */
    private function isModelReference(array $payload): bool
    {
        return array_key_exists(self::MODEL_KEY, $payload)
            && is_string($payload[self::MODEL_KEY])
            && is_subclass_of($payload[self::MODEL_KEY], Model::class);
    }

    /**
     * @param  array<int|string, mixed>  $payload
     */
    private function restoreModel(array $payload): ?Model
    {
        /** @var class-string<Model> $class */
        $class = $payload[self::MODEL_KEY];

        $model = new $class;

        if (isset($payload['connection']) && is_string($payload['connection'])) {
            $model->setConnection($payload['connection']);
        }

        return $model->newQuery()->find($payload['id'] ?? null);
    }
}
