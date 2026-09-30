<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions;

/**
 * A signal was refused before delivery. Catch this to handle every refusal there is;
 * catch a subclass to tell a finished run, one that is rolling back, and one with no
 * retry to wake apart.
 */
class CannotSignalFlowException extends FlowException {}
