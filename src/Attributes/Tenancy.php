<?php

namespace DiscoveryUkraine\SagaLaraFlow\Attributes;

use Attribute;

/**
 * Per-class override for automatic tenancy capture/restore around handle().
 * When present it wins over the config('saga-lara-flow.tenancy.auto') default:
 * #[Tenancy(auto: true)] opts a workflow/action into auto-restore even when the
 * global default is off, and #[Tenancy(auto: false)] opts it out for manual
 * control (read the current tenant via SagaFlow::tenancyContext()).
 */
#[Attribute(Attribute::TARGET_CLASS)]
final class Tenancy
{
    public function __construct(
        public bool $auto = true,
    ) {}
}
