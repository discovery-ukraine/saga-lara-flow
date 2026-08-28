<?php

namespace DiscoveryUkraine\SagaLaraFlow\Retry;

use Throwable;

/**
 * What action_runs.exception records about the attempt that failed: the class, the
 * message and the code, and nothing else. It is a snapshot of a column, not a live
 * Throwable — by the time a policy is asked, the object that was thrown is gone (a
 * queued step fails in another process, and every later replay reads the row). There
 * is no stack trace and no previous exception for the same reason.
 *
 * $class is null when the row records none. is() answers false for such a failure,
 * the same way an explicit only: filter has always refused one.
 */
final readonly class RecordedFailure
{
    public function __construct(
        public ?string $class = null,
        public string $message = '',
        public int|string $code = 0,
    ) {}

    /**
     * Rebuild a failure from the row's exception column, tolerating a partial or
     * absent record rather than assuming the three keys are there.
     *
     * @param  ?array<int|string, mixed>  $exception
     */
    public static function fromRecord(?array $exception): self
    {
        $class = $exception['class'] ?? null;
        $message = $exception['message'] ?? null;
        $code = $exception['code'] ?? null;

        return new self(
            is_string($class) ? $class : null,
            is_string($message) ? $message : '',
            is_int($code) || is_string($code) ? $code : 0,
        );
    }

    /**
     * Whether the failure is one of these classes, subclasses included.
     *
     * @param  class-string<Throwable>  ...$classes
     */
    public function is(string ...$classes): bool
    {
        $failure = $this->class;

        if ($failure === null) {
            return false;
        }

        return array_any(
            $classes,
            fn (string $candidate): bool => is_a($failure, $candidate, allow_string: true),
        );
    }
}
