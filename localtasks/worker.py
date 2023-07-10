import asyncio
import os
import signal

import httpx
from loguru import logger

from .queue import Queue
from .settings import settings
from .task import Task


async def fetch(
    queue: Queue, client: httpx.AsyncClient, message_id: str, task: Task
) -> None:
    """
    Send request to url.
    """
    try:
        logger.info(f"{task.method} {task.url} {task.headers}")
        response = await client.request(
            task.method,
            task.url,
            headers=task.headers,
            content=task.payload,
            timeout=settings.retry.timeout,
        )
        response.raise_for_status()
        await queue.ack(message_id, task.id)
        logger.info(
            f"{task.method} {task.url} {task.headers} {response.status_code}"
        )
        return
    except httpx.HTTPError as exc:
        err_message = str(exc)
        logger.error(f"{task.method} {task.url} {task.headers} {err_message}")
        max_retries = settings.retry.max_retries
        err_count = await queue.store_latest_error(task.id, err_message)
        if max_retries is not None and (max_retries <= err_count):
            logger.warning(
                f"{task.method} {task.url} {task.headers} max retries reached"
            )
            return await queue.ack(message_id, task.id)

        delay_seconds = min(
            settings.retry.min_interval
            * (2 ** min(err_count, settings.retry.max_doubling)),
            settings.retry.max_interval,
        )
        delay_milliseconds = delay_seconds * 1000
        await queue.retry(message_id, task.id, delay_milliseconds)
        return


async def worker(exit_signal: asyncio.Event) -> None:
    async with (
        Queue(settings.redis_dsn) as queue,
        httpx.AsyncClient(http2=True) as client,
    ):
        logger.info("Worker started.")

        while not exit_signal.is_set():
            for message_id, task in await queue.autoclaim(
                settings.consumer_name, int((settings.retry.timeout + 60) * 1000)
            ):
                asyncio.create_task(fetch(queue, client, message_id, task))

            pending = await queue.pending
            if pending["pending"] >= settings.speed_limit.max_concurrent:
                await asyncio.sleep(0.2)
                continue

            message_id, task = await queue.pull(settings.consumer_name)
            if message_id is None or task is None:  # stream is empty
                await asyncio.sleep(0.2)
                continue

            asyncio.create_task(fetch(queue, client, message_id, task))

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
