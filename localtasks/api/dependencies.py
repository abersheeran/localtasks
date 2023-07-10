from kui.asgi import request

from localtasks.queue import Queue


async def get_queue() -> Queue:
    return request.app.state.queue
