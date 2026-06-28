<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal;

use Exception;

/**
 * Base class for internal control-flow signals used to suspend and replay a
 * workflow. These are NOT business failures: only the FlowExecutor drive loop
 * is allowed to catch them, and they must never be treated as errors.
 *
 * User workflow code must never throw or swallow these. The base Workflow
 * exposes isFlowControl() so accidental catch (Throwable) blocks can rethrow.
 */
abstract class InternalFlowControl extends Exception {}
