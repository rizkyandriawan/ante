from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, Callable

from .codec import Parser
from .protocol import AnteError, Connection

if TYPE_CHECKING:
    from .client import Ante, Delivery, Handler

log = logging.getLogger("ante")


class Consumer:
    def __init__(
        self,
        ante: Ante,
        queue: str,
        handler: Handler,
        *,
        prefetch: int,
        lease_ms: int,
        wait_ms: int,
        parse: Parser | None,
        on_error: Callable[[BaseException, Delivery], str] | None,
        max_attempts: int | None,
        shutdown_timeout: float,
    ) -> None:
        if prefetch < 1:
            raise ValueError("prefetch must be at least 1")
        self._ante = ante
        self._queue = queue
        self._handler = handler
        self._prefetch = prefetch
        self._lease_ms = lease_ms
        self._wait_ms = wait_ms
        self._parse = parse
        self._on_error = on_error
        self._max_attempts = max_attempts
        self._shutdown_timeout = shutdown_timeout

        self._stopped = threading.Event()
        self._condition = threading.Condition()
        self._active = 0
        self._thread: threading.Thread | None = None
        self._executor: ThreadPoolExecutor | None = None
        self._take_connection: Connection | None = None

    @property
    def queue(self) -> str:
        return self._queue

    @property
    def active(self) -> int:
        with self._condition:
            return self._active

    def start(self) -> Consumer:
        if self._thread is not None:
            return self
        timeout = max(10.0, self._wait_ms / 1000.0 + 10.0)
        self._take_connection = Connection.open(
            self._ante.host,
            self._ante.port,
            timeout,
        )
        self._executor = ThreadPoolExecutor(
            max_workers=self._prefetch,
            thread_name_prefix="ante",
        )
        self._thread = threading.Thread(
            target=self._loop,
            name=f"ante-{self._queue}",
            daemon=True,
        )
        self._thread.start()
        return self

    def wait(self) -> None:
        if self._thread is not None:
            self._thread.join()

    def stop(self) -> None:
        if self._stopped.is_set():
            return
        self._stopped.set()
        with self._condition:
            self._condition.notify_all()
        if self._take_connection is not None:
            self._take_connection.interrupt()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(self._shutdown_timeout)

        deadline = time.monotonic() + self._shutdown_timeout
        with self._condition:
            while self._active > 0 and time.monotonic() < deadline:
                self._condition.wait(0.1)
            remaining = self._active
        if remaining:
            log.warning(
                "ante: %s consumer stopped with %d handler(s) still running",
                self._queue,
                remaining,
            )

        if self._executor is not None:
            self._executor.shutdown(wait=False, cancel_futures=True)
            self._executor = None
        if self._take_connection is not None:
            self._take_connection.close()
            self._take_connection = None
        self._ante._forget(self)

    def __enter__(self) -> Consumer:
        return self.start()

    def __exit__(self, *_exc: object) -> None:
        self.stop()

    def _loop(self) -> None:
        assert self._take_connection is not None
        while not self._stopped.is_set():
            with self._condition:
                while self._active >= self._prefetch and not self._stopped.is_set():
                    self._condition.wait(0.2)
                capacity = self._prefetch - self._active
            if self._stopped.is_set() or capacity <= 0:
                continue

            try:
                deliveries = self._ante._take_on(
                    self._take_connection,
                    self._queue,
                    min(capacity, 256),
                    self._lease_ms,
                    self._wait_ms,
                    self._parse,
                    self._ante._control,
                )
            except AnteError as error:
                if self._stopped.is_set():
                    break
                log.warning("ante: take failed on %s: %s", self._queue, error)
                self._stopped.wait(0.5)
                continue
            except (OSError, ConnectionError) as error:
                if self._stopped.is_set():
                    break
                log.warning("ante: take connection lost on %s: %s", self._queue, error)
                self._stopped.wait(0.5)
                continue

            for delivery in deliveries:
                with self._condition:
                    self._active += 1
                assert self._executor is not None
                self._executor.submit(self._handle, delivery)

    def _handle(self, delivery: Delivery) -> None:
        try:
            try:
                self._handler(delivery.payload, delivery)
            except Exception as error:
                self._fail(error, delivery)
            else:
                if not delivery.settled:
                    try:
                        delivery.ack()
                    except (AnteError, OSError, ConnectionError) as error:
                        log.warning(
                            "ante: could not ack %s on %s: %s",
                            delivery.token,
                            self._queue,
                            error,
                        )
        finally:
            with self._condition:
                self._active -= 1
                self._condition.notify_all()

    def _fail(self, error: Exception, delivery: Delivery) -> None:
        if delivery.settled:
            return
        try:
            action = "requeue"
            if self._on_error is not None:
                action = self._on_error(error, delivery)
            elif self._max_attempts is not None and delivery.attempts >= self._max_attempts:
                action = "drop"

            log.warning(
                "ante: handler failed on %s (attempt %d, %s): %s",
                self._queue,
                delivery.attempts,
                action,
                error,
            )
            if action == "requeue":
                delivery.requeue()
            else:
                delivery.ack()
        except AnteError as settle_error:
            log.warning(
                "ante: could not settle delivery %s on %s: %s",
                delivery.token,
                self._queue,
                settle_error,
            )
        except (OSError, ConnectionError) as settle_error:
            log.warning(
                "ante: settle connection lost on %s: %s",
                self._queue,
                settle_error,
            )

    def _sleep(self, seconds: float) -> None:
        self._stopped.wait(seconds)