import time
from typing import Self, TypedDict

import redis.asyncio as redis
import redis.exceptions as redis_exceptions
from loguru import logger

from .task import Task

ACK_SCRIPT = """
local stream_name = KEYS[1]
local group_name = KEYS[2]
local message_id = ARGV[1]
local task_id = ARGV[2]

redis.call('XACK', stream_name, group_name, message_id)
redis.call('XDEL', stream_name, message_id)
redis.call('DEL', task_id)
"""

RETRY_SCRIPT = """
local stream_name = KEYS[1]
local group_name = KEYS[2]
local delay_queue_name = KEYS[3]
local message_id = ARGV[1]
local task_id = ARGV[2]
local milliseconds = tonumber(ARGV[3])

redis.call('XACK', stream_name, group_name, message_id)
redis.call('XDEL', stream_name, message_id)
local res = redis.call('HDEL', task_id, 'message_id')
if res == 0 then  -- task_id not exists
    return nil
end

redis.call('ZADD', delay_queue_name, 'NX', milliseconds, task_id)
"""

STROE_LATEST_ERROR_SCRIPT = """
local task_id = KEYS[1]
local error = ARGV[1]

redis.call('HSET', task_id, 'latest_error', error)
local error_count = redis.call('HINCRBY', task_id, 'error_count', 1)
return error_count
"""

ADD_DELAY_SCRIPT = """
local delay_queue_name = KEYS[1]
local key = ARGV[1]
local milliseconds = tonumber(ARGV[2])
local task_json = ARGV[3]

local res = redis.call('HSETNX', key, 'json', task_json)
if res == 0 then
    return 0
end

redis.call('ZADD', delay_queue_name, 'NX', milliseconds, key)
return 1
"""

SET_DELAY_SCRIPT = """
local delay_queue_name = KEYS[1]
local stream_name = KEYS[2]
local now_milliseconds = tonumber(ARGV[1])

local member = redis.call('ZPOPMIN', delay_queue_name, 1)
local task_id = member[1]
local milliseconds = tonumber(member[2])

if task_id == nil then
    return nil
end

if milliseconds > now_milliseconds then
    redis.call('ZADD', delay_queue_name, milliseconds, task_id)
    return nil
end

local task_json = redis.call('HGET', task_id, 'json')
-- https://redis.io/commands/hget/
-- What's the fuck? boolean???
if (type(task_json) == "boolean" and not task_json) or task_json == nil then
    return nil
end

local message_id = redis.call('XADD', stream_name, '*', 'json', task_json)
if message_id == nil then
    return nil
end

redis.call('HSET', task_id, 'message_id', message_id)

return true
"""

ADD_NO_DELAY_SCRIPT = """
local stream_name = KEYS[1]
local task_id = ARGV[1]
local task_json = ARGV[2]

local res = redis.call('HSETNX', task_id, 'json', task_json)
if res == 0 then
    return 0
end

local message_id = redis.call('XADD', stream_name, '*', 'json', task_json)
redis.call('HSET', task_id, 'message_id', message_id)

return 1
"""

DELETE_TASK_SCRIPT = """
local stream_name = KEYS[1]
local delay_queue_name = KEYS[2]
local task_id = ARGV[1]

local message_id = redis.call('HGET', task_id, "message_id")
if message_id then
    redis.call('XDEL', stream_name, message_id)
else
    redis.call('ZREM', delay_queue_name, task_id)
end
redis.call('DEL', task_id)
"""


class PendingResponseType(TypedDict):
    pending: int
    min: str | None
    max: str | None
    consumers: list[tuple[str, int]]


class Queue:
    """
    Message queue power by redis stream.
    """

    def __init__(self, redis_dsn: str) -> None:
        self.connection = redis.Redis.from_url(redis_dsn, decode_responses=True)

        self._ack_script = self.connection.register_script(ACK_SCRIPT)
        self._retry_script = self.connection.register_script(RETRY_SCRIPT)
        self._store_latest_error_script = self.connection.register_script(
            STROE_LATEST_ERROR_SCRIPT
        )
        self._add_delay_task_script = self.connection.register_script(ADD_DELAY_SCRIPT)
        self._set_delay_task_script = self.connection.register_script(SET_DELAY_SCRIPT)
        self._add_no_delay_task_script = self.connection.register_script(
            ADD_NO_DELAY_SCRIPT
        )
        self._delete_task_script = self.connection.register_script(DELETE_TASK_SCRIPT)

    @property
    def stream_name(self) -> str:
        return "localtasks"

    @property
    def delay_queue_name(self) -> str:
        return "localtasks:delay"

    @property
    def group_name(self) -> str:
        return "workers"

    async def _add_delay_task(self, task: Task, delay_milliseconds: int) -> bool:
        milliseconds = (time.time_ns() // 1000000) + delay_milliseconds

        # if not await self.connection.hsetnx(
        #     task.id,
        #     "json",
        #     task.model_dump_json(),
        # ):
        #     return False

        # return 1 == await self.connection.zadd(
        #     self.delay_queue_name, {task.id: milliseconds}, nx=True
        # )

        return bool(
            await self._add_delay_task_script(
                keys=[self.delay_queue_name],
                args=[task.id, milliseconds, task.model_dump_json()],
            )
        )

    async def set_delay_task(self) -> bool:
        """
        Set delay task to stream. Return True if setted.
        """
        now_milliseconds = time.time_ns() // 1000000

        # member: list[tuple[str, int]] = await self.connection.zpopmin(
        #     self.delay_queue_name, 1
        # )
        # if not member:
        #     return False

        # # only one member
        # task_id, milliseconds = member[0]

        # # not now
        # if milliseconds > now_milliseconds:
        #     await self.connection.zadd(self.delay_queue_name, {task_id: milliseconds})
        #     return False

        # task_json = await self.connection.hget(task_id, "json")
        # if not task_json:
        #     return False

        # await self.connection.xadd(self.stream_name, {"json": task_json})
        # return True

        return bool(
            await self._set_delay_task_script(
                keys=[self.delay_queue_name, self.stream_name],
                args=[now_milliseconds],
            )
        )

    async def push(self, task: Task, delay_milliseconds: int = 0) -> bool:
        """
        Push task to queue. Return True if pushed.
        """
        if not delay_milliseconds:
            logger.debug(f"Pushing task {task} to queue")
            return bool(
                await self._add_no_delay_task_script(
                    keys=[self.stream_name], args=[task.id, task.model_dump_json()]
                )
            )
        else:
            logger.debug(
                f"Pushing task {task} to delay queue, {delay_milliseconds}ms"
            )
            return await self._add_delay_task(task, delay_milliseconds)

    async def pull(self, consumer: str) -> tuple[str, Task] | tuple[None, None]:
        """
        Pull task from queue. Return (message_id, task).
        """
        # pull delay task
        await self.set_delay_task()

        res = await self.connection.xreadgroup(
            self.group_name, consumer, {self.stream_name: ">"}, count=1
        )
        # [["stream_name", [("message_id", {"json": "task_json"})]]
        logger.debug(f"Pulled as consumer {consumer}: {res}")

        if not res:
            logger.debug("No task to pull.")
            return None, None

        for stream_name, message_list in res:
            for msg in message_list:
                message_id, fields = msg
                logger.debug(f"Pulled message: {message_id}, {fields['json']}")
                return message_id, Task.model_validate_json(fields["json"])

        logger.debug("No task to pull.")
        return None, None

    async def ack(self, message_id: str, task_id: str) -> None:
        """
        Ack message, then delete it and task.
        """
        # await self.connection.xack(self.stream_name, self.group_name, message_id)
        # await self.connection.xdel(self.stream_name, message_id)
        # await self.connection.delete(task_id)
        await self._ack_script(
            keys=[self.stream_name, self.group_name], args=[message_id, task_id]
        )
        logger.debug(f"Acked message {message_id} and task {task_id}")

    async def store_latest_error(self, task_id: str, error: str) -> int:
        """
        Store latest error, return error count.
        """
        # await self.connection.hset(task_id, "latest_error", error)
        # return await self.connection.hincrby(task_id, "error_count", 1)
        return int(await self._store_latest_error_script(keys=[task_id], args=[error]))

    async def retry(
        self, message_id: str, task_id: str, delay_milliseconds: int
    ) -> None:
        """
        Retry task, ack and del message.
        """
        # await self.connection.xack(self.stream_name, self.group_name, message_id)
        # await self.connection.xdel(self.stream_name, message_id)
        # await self.connection.hdel(task_id, "message_id")
        # await self.connection.zadd(
        #     self.delay_queue_name, {task_id: delay_milliseconds}, nx=True
        # )
        await self._retry_script(
            keys=[self.stream_name, self.group_name, self.delay_queue_name],
            args=[
                message_id,
                task_id,
                (time.time_ns() // 1000000) + delay_milliseconds,
            ],
        )
        logger.debug(
            f"Retried message {message_id} and task {task_id}, {delay_milliseconds}ms"
        )

    async def delete(self, task_id: str) -> None:
        """
        Delete task that not in pending.
        """
        # message_id: str | None = await self.connection.hget(task_id, "message_id")
        # if message_id:
        #     await self.connection.xdel(self.stream_name, message_id)
        # else:
        #     await self.connection.zrem(self.delay_queue_name, task_id)
        # await self.connection.delete(task_id)
        await self._delete_task_script(
            keys=[self.stream_name, self.delay_queue_name], args=[task_id]
        )

    async def autoclaim(
        self, consumer: str, min_idle_time: int
    ) -> list[tuple[str, Task]]:
        """
        Auto claim tasks.
        """
        logger.debug(f"Auto claim as consumer {consumer}")
        res = await self.connection.xautoclaim(
            self.stream_name, self.group_name, consumer, min_idle_time
        )
        return [
            (message_id, Task.model_validate_json(fields["json"]))
            for message_id, fields in res[1]
        ]

    async def range_todo(self, start: str, length: int) -> tuple[str, list[Task]]:
        res = await self.connection.xrange(self.stream_name, start, "+", count=length)
        if not res:
            return start, []
        return res[-1][0], [
            Task.model_validate_json(fields["json"]) for _, fields in res
        ]

    async def range_delay(self, start: int, length: int) -> tuple[int, list[Task]]:
        res = await self.connection.zrange(
            self.delay_queue_name,
            start,
            "+inf",  # type: ignore
            byscore=True,
            withscores=True,
            offset=0,
            num=length,
        )

        if not res:
            return start, []
        return res[-1][1], [
            Task.model_validate_json(
                await self.connection.hget(task_id, "json"),  # type: ignore
            )
            for task_id, score in res
        ]

    @property
    async def length(self) -> int:
        return await self.connection.xlen(
            self.stream_name
        ) + await self.connection.zcard(self.delay_queue_name)

    @property
    async def pending(self) -> PendingResponseType:
        return await self.connection.xpending(self.stream_name, self.group_name)

    async def init(self) -> None:
        try:
            await self.connection.xgroup_create(
                self.stream_name, self.group_name, 0, mkstream=True
            )
        except redis_exceptions.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def close(self) -> None:
        await self.connection.close(close_connection_pool=True)

    async def __aenter__(self) -> Self:
        await self.init()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()
