<?php

namespace DiscoveryUkraine\SagaLaraFlow\Attributes;

use Attribute;

/**
 * Declarative queryable tag on a workflow, read at create time. Repeatable. An
 * explicit ->withTags() entry with the same key overrides the attribute's value.
 */
#[Attribute(Attribute::TARGET_CLASS | Attribute::IS_REPEATABLE)]
final class Tag
{
    public function __construct(
        public string $key,
        public ?string $value = null,
    ) {}
}
