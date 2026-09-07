<?php

namespace DiscoveryUkraine\SagaLaraFlow\Exceptions;

/**
 * A signal was refused before delivery. Catch this to handle every refusal there is;
 * catch a subclass to tell a finished run from one that is rolling back.
 */
class CannotSignalFlowException extends FlowException {}
