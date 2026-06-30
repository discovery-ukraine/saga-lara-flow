<?php

namespace DiscoveryUkraine\SagaLaraFlow\Support;

use DiscoveryUkraine\SagaLaraFlow\Attributes\ActionName;
use DiscoveryUkraine\SagaLaraFlow\Attributes\ActionTimeout;
use DiscoveryUkraine\SagaLaraFlow\Attributes\ChildPolicy;
use DiscoveryUkraine\SagaLaraFlow\Attributes\ContinueOnFailure;
use DiscoveryUkraine\SagaLaraFlow\Attributes\Flow;
use DiscoveryUkraine\SagaLaraFlow\Attributes\FlowQueue;
use DiscoveryUkraine\SagaLaraFlow\Attributes\FlowTimeout;
use DiscoveryUkraine\SagaLaraFlow\Attributes\Tag;
use DiscoveryUkraine\SagaLaraFlow\Enums\ChildClosePolicy;
use ReflectionClass;

/**
 * Reads the package's declarative attributes off workflow/action classes via
 * reflection and resolves them into plain DTOs. Results are cached per class —
 * attributes are compile-time constants, so a single read per class is enough.
 * The reader never imposes precedence; callers combine its output with their
 * explicit builder values and config (explicit call > attribute > config).
 */
final class AttributeReader
{
    /** @var array<class-string, WorkflowAttributes> */
    private static array $workflowCache = [];

    /** @var array<class-string, ActionAttributes> */
    private static array $actionCache = [];

    /** @var array<class-string, ?ChildClosePolicy> */
    private static array $childPolicyCache = [];

    /**
     * @param  class-string  $class
     */
    public function workflow(string $class): WorkflowAttributes
    {
        return self::$workflowCache[$class] ??= $this->readWorkflow($class);
    }

    /**
     * @param  class-string  $class
     */
    public function action(string $class): ActionAttributes
    {
        return self::$actionCache[$class] ??= $this->readAction($class);
    }

    /**
     * @param  class-string  $class
     */
    public function childPolicy(string $class): ?ChildClosePolicy
    {
        return self::$childPolicyCache[$class] ??= $this->readChildPolicy($class);
    }

    /**
     * @param  class-string  $class
     */
    private function readWorkflow(string $class): WorkflowAttributes
    {
        if (! class_exists($class)) {
            return new WorkflowAttributes;
        }

        $reflection = new ReflectionClass($class);

        $flow = $this->firstAttribute($reflection, Flow::class);
        $queue = $this->firstAttribute($reflection, FlowQueue::class);
        $timeout = $this->firstAttribute($reflection, FlowTimeout::class);

        $tags = [];

        foreach ($reflection->getAttributes(Tag::class) as $attribute) {
            $tag = $attribute->newInstance();
            $tags[] = ['key' => $tag->key, 'value' => $tag->value];
        }

        return new WorkflowAttributes(
            name: $flow?->name,
            version: $flow?->version,
            connection: $queue?->connection,
            queue: $queue?->queue,
            timeoutSeconds: $timeout?->seconds,
            tags: $tags,
        );
    }

    /**
     * @param  class-string  $class
     */
    private function readAction(string $class): ActionAttributes
    {
        if (! class_exists($class)) {
            return new ActionAttributes;
        }

        $reflection = new ReflectionClass($class);

        $name = $this->firstAttribute($reflection, ActionName::class);
        $timeout = $this->firstAttribute($reflection, ActionTimeout::class);
        $continue = $this->firstAttribute($reflection, ContinueOnFailure::class);

        return new ActionAttributes(
            name: $name?->name,
            timeoutSeconds: $timeout?->seconds,
            continueOnFailure: $continue?->continue,
        );
    }

    /**
     * @param  class-string  $class
     */
    private function readChildPolicy(string $class): ?ChildClosePolicy
    {
        if (! class_exists($class)) {
            return null;
        }

        return $this->firstAttribute(new ReflectionClass($class), ChildPolicy::class)?->policy;
    }

    /**
     * @template T of object
     *
     * @param  ReflectionClass<object>  $reflection
     * @param  class-string<T>  $attribute
     * @return T|null
     */
    private function firstAttribute(ReflectionClass $reflection, string $attribute): ?object
    {
        $attributes = $reflection->getAttributes($attribute);

        return $attributes === [] ? null : $attributes[0]->newInstance();
    }
}
