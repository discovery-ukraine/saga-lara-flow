<?php

namespace DiscoveryUkraine\SagaLaraFlow\Enums;

/**
 * Runtime execution mode for a drive loop pass.
 *
 * Distinct from {@see DispatchMode}, which is the config-level default: RunMode
 * is the concrete mode the executor is currently driving a run in.
 */
enum RunMode: string
{
    case Sync = 'sync';
    case Queued = 'queued';
}
