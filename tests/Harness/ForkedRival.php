<?php

namespace DiscoveryUkraine\SagaLaraFlow\Tests\Harness;

use Closure;
use Illuminate\Support\Facades\DB;
use PHPUnit\Framework\AssertionFailedError;
use RuntimeException;
use Symfony\Component\Uid\Ulid;
use Throwable;

/**
 * A rival in a process of its own, for an interleaving in which one side blocks: a
 * statement waiting on a lock holds its whole process, so the side that releases the
 * lock cannot be the same one.
 *
 * Each side holds one end of a socket pair and the other side's ForkedRival. arrive()
 * tells the other side a barrier is passed, await() waits for the other side's, and
 * every wait ends in a failure naming its barrier rather than in a hang.
 *
 * The child inherits every session the parent had open. Closing one of them, even by
 * letting PHP shut down, would end it on the server for the parent too, so the child
 * keeps them referenced, opens sessions of its own, and ends with SIGKILL.
 */
final class ForkedRival
{
    /** @var array<int, self> */
    private static array $started = [];

    /** @var list<mixed> */
    private static array $inherited = [];

    private string $buffer = '';

    /** @var array<string, true> */
    private array $reached = [];

    private bool $returnedYet = false;

    private mixed $returned = null;

    /**
     * @param  resource  $socket
     */
    private function __construct(private readonly int $pid, private $socket) {}

    /**
     * @param  Closure(self): mixed  $body  runs in the child, given the parent's side;
     *                                      what it returns comes back from finish()
     */
    public static function start(Closure $body): self
    {
        [$parentEnd, $childEnd] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP)
            ?: throw new RuntimeException('No socket pair for the rival.');

        $pid = pcntl_fork();

        if ($pid === -1) {
            throw new RuntimeException('Could not fork the rival.');
        }

        if ($pid === 0) {
            fclose($parentEnd);
            self::runChild(new self(posix_getppid(), $childEnd), $body);
        }

        fclose($childEnd);

        return self::$started[$pid] = new self($pid, $parentEnd);
    }

    public function arrive(string $barrier): void
    {
        $this->send(['arrive' => $barrier]);
    }

    public function await(string $barrier, float $seconds = 5.0): void
    {
        $this->wait(fn (): bool => isset($this->reached[$barrier]), "barrier '{$barrier}'", $seconds);
    }

    /**
     * Whether the other side has passed the barrier yet, without waiting for it.
     */
    public function reached(string $barrier): bool
    {
        $this->take("barrier '{$barrier}'");

        if (! isset($this->reached[$barrier])) {
            $this->read(0, "barrier '{$barrier}'");
            $this->take("barrier '{$barrier}'");
        }

        return isset($this->reached[$barrier]);
    }

    /**
     * Wait for the child's body to return, and fail with whatever it threw. The child
     * ends the moment it has reported, so once this returns it holds nothing.
     */
    public function finish(float $seconds = 5.0): mixed
    {
        $this->wait(fn (): bool => $this->returnedYet, 'the end of its body', $seconds);

        pcntl_waitpid($this->pid, $status);
        fclose($this->socket);
        unset(self::$started[$this->pid]);

        return $this->returned;
    }

    /**
     * @param  Closure(): bool  $passed
     */
    private function wait(Closure $passed, string $what, float $seconds): void
    {
        $deadline = hrtime(true) + (int) ($seconds * 1e9);

        while (true) {
            $this->take($what);

            if ($passed()) {
                return;
            }

            if ($this->returnedYet) {
                throw new AssertionFailedError("The other process finished without reaching {$what}.");
            }

            $left = $deadline - hrtime(true);

            if ($left <= 0) {
                throw new AssertionFailedError(sprintf(
                    'The other process did not reach %s within %.1f s.', $what, $seconds,
                ));
            }

            $this->read($left, $what);
        }
    }

    /**
     * Add to the buffer whatever the other side has sent, waiting at most $nanoseconds.
     * A select a signal interrupts returns with nothing read; the caller's deadline decides.
     */
    private function read(int $nanoseconds, string $what): void
    {
        $read = [$this->socket];
        $write = $except = null;

        if (! @stream_select($read, $write, $except, intdiv($nanoseconds, 1_000_000_000), intdiv($nanoseconds % 1_000_000_000, 1000))) {
            return;
        }

        $chunk = fread($this->socket, 8192);

        if ($chunk === '' || $chunk === false) {
            throw new AssertionFailedError("The other process ended before {$what}.");
        }

        $this->buffer .= $chunk;
    }

    /**
     * Act on every whole message in the buffer.
     */
    private function take(string $what): void
    {
        while (($end = strpos($this->buffer, "\n")) !== false) {
            $message = json_decode(substr($this->buffer, 0, $end), true, flags: JSON_THROW_ON_ERROR);
            $this->buffer = substr($this->buffer, $end + 1);

            match (true) {
                isset($message['fail']) => throw new AssertionFailedError(
                    "The other process failed before {$what}: {$message['fail']}",
                ),
                array_key_exists('returned', $message) => [$this->returnedYet, $this->returned] = [true, $message['returned']],
                default => $this->reached[$message['arrive']] = true,
            };
        }
    }

    /**
     * Kill and collect every child this process started; a child that already ended
     * is collected all the same.
     */
    public static function reapAll(): void
    {
        foreach (self::$started as $pid => $rival) {
            posix_kill($pid, SIGKILL);
            pcntl_waitpid($pid, $status);
            fclose($rival->socket);
        }

        self::$started = [];
    }

    private static function runChild(self $parent, Closure $body): never
    {
        // Whatever happens here, this copy of the test process must not outlive it: leaving
        // by any other way runs PHPUnit on, or shuts down and closes the parent's sessions.
        // exit() skips finally, so a shutdown function stands in for it; PHP runs shutdown
        // functions before it destroys a single object.
        register_shutdown_function(static fn () => posix_kill(getmypid(), SIGKILL));

        try {
            // The parent's children are the parent's to reap.
            self::$started = [];

            foreach (DB::getConnections() as $name => $connection) {
                self::$inherited[] = [$connection, $connection->getRawPdo(), $connection->getRawReadPdo()];

                DB::purge($name);
            }

            // The child is a copy of the parent's memory, generators included. Symfony's
            // ULID keeps the last millisecond and random part in statics and, until the
            // clock moves past that millisecond, only increments them — both sides would
            // hand out the same ids. Forget it, and reseed mt_rand for the same reason.
            Closure::bind(static function (): void {
                self::$time = '';
            }, null, Ulid::class)();
            mt_srand();

            $parent->send(['returned' => $body($parent)]);
        } catch (Throwable $e) {
            $parent->send(['fail' => $e::class.': '.$e->getMessage()]);
        } finally {
            posix_kill(getmypid(), SIGKILL);
        }

        exit(1);
    }

    /**
     * @param  array<string, mixed>  $message
     */
    private function send(array $message): void
    {
        // A side that has already finished needs no more barriers; if its absence
        // matters, the next await() on it says so.
        // A message carrying bytes that are not UTF-8 still arrives, with them replaced.
        @fwrite($this->socket, json_encode($message, JSON_THROW_ON_ERROR | JSON_INVALID_UTF8_SUBSTITUTE)."\n");
    }
}
