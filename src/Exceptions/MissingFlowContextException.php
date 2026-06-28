<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions;

/**
 * Thrown when a workflow runtime operation is attempted without a flow run
 * bound to the runtime — i.e. outside of FlowExecutor::drive().
 */
class MissingFlowContextException extends FlowException {}
