import asyncio
import os
import signal

import httpx
from loguru import logger

from .queue import Queue
from .settings import settings
from .task import Task


async def fetch(
    queue: Queue, client: httpx.AsyncClient, message_id: str, event: Task
) -> None:
    """
    Send request to url.
    """
    try:
        logger.info(f"{event.method} {event.url} {event.headers}")
        response = await client.request(
            event.method,
            event.url,
            headers=event.headers,
            content=event.payload,
            timeout=settings.retry.timeout,
        )
        response.raise_for_status()
        await queue.ack(message_id, event.id)
        logger.info(
            f"{event.method} {event.url} {event.headers} {response.status_code}"
        )
        return
    except httpx.HTTPError as exc:
        err_message = str(exc)
        logger.error(f"{event.method} {event.url} {event.headers} {err_message}")
        max_retries = settings.retry.max_retries
        err_count = await queue.store_latest_error(event.id, err_message)
        if max_retries is not None and (max_retries <= err_count):
            logger.warning(
                f"{event.method} {event.url} {event.headers} max retries reached"
            )
            return await queue.ack(message_id, event.id)

        delay_seconds = min(
            settings.retry.min_interval
            * (2 ** min(err_count, settings.retry.max_doubling)),
            settings.retry.max_interval,
        )
        delay_milliseconds = delay_seconds * 1000
        await queue.retry(message_id, event.id, delay_milliseconds)
        return


async def worker(exit_signal: asyncio.Event) -> None:
    async with (
        Queue(settings.redis_dsn) as queue,
        httpx.AsyncClient(http2=True) as client,
    ):
        logger.info("Worker started.")

        while not exit_signal.is_set():
            for message_id, event in await queue.autoclaim(
                settings.consumer_name, int((settings.retry.timeout + 60) * 1000)
            ):
                asyncio.create_task(fetch(queue, client, message_id, event))

            pending = await queue.pending
            if pending["pending"] >= settings.speed_limit.max_concurrent:
                await asyncio.sleep(0.2)
                continue

            message_id, event = await queue.pull(settings.consumer_name)
            if message_id is None or event is None:  # stream is empty
                await asyncio.sleep(0.2)
                continue

            asyncio.create_task(fetch(queue, client, message_id, event))

        logger.info("Worker stopped.")


def main():
    exit_signal = asyncio.Event()
    for sig in (
        signal.SIGINT,  # Sent by Ctrl+C.
        signal.SIGTERM  # Sent by `kill <pid>`. Not sent on Windows.
        if os.name != "nt"
        else signal.SIGBREAK,  # Sent by `Ctrl+Break` on Windows.
    ):
        signal.signal(sig, lambda sig, frame: exit_signal.set())

    asyncio.run(worker(exit_signal))


if __name__ == "__main__":
    main()
