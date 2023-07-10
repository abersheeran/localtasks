import asyncio
from typing import AsyncGenerator

from kui.asgi import Kui
from kui.asgi.lifespan import asynccontextmanager_lifespan

from localtasks.queue import Queue
from localtasks.settings import settings


async def queue_lifespan(app: Kui) -> AsyncGenerator[None, None]:
    async with Queue(settings.redis_dsn) as queue:
        app.state.queue = queue
        yield


init_queue, close_queue = asynccontextmanager_lifespan(queue_lifespan)


async def always_pull_delayed(queue: Queue) -> None:
    while True:
        if not await queue.set_delay_task():
            # No delayed tasks, wait a bit.
            await asyncio.sleep(0.5)


async def start_pull_delayed(app: Kui) -> None:
    task = asyncio.create_task(always_pull_delayed(app.state.queue))
    task.add_done_callback(lambda _: asyncio.create_task(start_pull_delayed(app)))
