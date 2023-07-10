import pytest
import pytest_asyncio

from localtasks.queue import Queue
from localtasks.task import Task


@pytest_asyncio.fixture
async def queue():
    queue = Queue("redis://localhost:6379/0")
    async with queue:
        yield queue
        await queue.connection.flushdb()


@pytest.mark.asyncio
async def test_delete(queue: Queue):
    task = Task(
        id="task_id",
        url="https://aber.sh",
        method="GET",
        headers={"User-Agent": "localtasks"},
        payload=b"",
    )
    assert await queue.push(task)
    await queue.delete(task.id)
    assert await queue.range_todo("-", 1) == ("-", [])


@pytest.mark.asyncio
async def test_delete_pending_task(queue: Queue):
    task = Task(
        id="task_id",
        url="https://aber.sh",
        method="GET",
        headers={"User-Agent": "localtasks"},
        payload=b"",
    )
    assert await queue.push(task)
    message_id, pulled_task = await queue.pull("test")
    assert message_id is not None and task is not None
    assert pulled_task == task
    await queue.delete(task.id)
    assert await queue.range_todo("-", 1) == ("-", [])
    await queue.retry(message_id, task.id, 0)
    assert await queue.range_delay(0, 1) == (0, [])


@pytest.mark.asyncio
async def test_delete_no_exists_task(queue: Queue):
    await queue.delete("no_exists_task_id")


@pytest.mark.asyncio
async def test_pull_and_ack_task(queue: Queue):
    task = Task(
        id="task_id",
        url="https://aber.sh",
        method="GET",
        headers={"User-Agent": "localtasks"},
        payload=b"",
    )
    assert await queue.push(task)
    message_id, pulled_task = await queue.pull("test")
    assert message_id is not None and task is not None
    assert pulled_task == task
    assert (await queue.pending)["pending"] == 1
    await queue.ack(message_id, task.id)
    assert (await queue.pending)["pending"] == 0
    assert await queue.range_todo("-", 1) == ("-", [])
