<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions\Internal;

use Exception;

/**
 * Thrown inside a transaction whose action_runs write was refused, purely to roll the
 * rest of that transaction back — the flow_signals row written beside it must not stand
 * for a step transition that did not happen.
 *
 * It is caught by the seam that opened the transaction and never leaves it — three of
 * them park, consume-and-retry, and hand a floating delivery over. It is not an
 * InternalFlowControl: the drive loop must not read it as a suspension, and the seam
 * decides what the losing pass does instead.
 */
final class FencedWriteLost extends Exception {}
