from typing import Annotated, Any, Literal

from kui.asgi import (
    Body,
    Depends,
    HttpView,
    JSONResponse,
    Path,
    Query,
    Routes,
)

from localtasks.queue import Queue
from localtasks.task import Task

from .dependencies import get_queue
from .schemas import CreateTaskDto, ListTaskEntity

routes = Routes()


@routes.http("/task")
class TaskView(HttpView):
    @staticmethod
    async def get(
        start: Annotated[str | int | None, Query(None, description="latest id")],
        length: Annotated[int, Query(10, description="number of tasks to return")],
        type: Annotated[Literal["todo", "delay"], Query("todo")],
        queue: Annotated[Queue, Depends(get_queue)],
    ) -> Annotated[ListTaskEntity, JSONResponse[200, {}, ListTaskEntity]]:
        """
        List tasks
        """
        match type:
            case "todo":
                start = "-" if start is None else str(start)
                latest_id, tasks = await queue.range_todo(start, length)
            case "delay":
                start = 0 if start is None else int(start)
                latest_id, tasks = await queue.range_delay(start, length)
        return {"latest_id": latest_id, "tasks": tasks}

    @staticmethod
    async def post(
        dto: Annotated[CreateTaskDto, Body(..., exclusive=True)],
        queue: Annotated[Queue, Depends(get_queue)],
    ) -> Annotated[tuple[Task, Literal[201]], JSONResponse[201, {}, Task]]:
        """
        Create a task
        """
        obj = dto.model_dump(exclude_defaults=True)

        while True:
            task = Task.model_validate(obj)
            if not await queue.push(task, dto.delay_milliseconds):
                if obj.get("id") is None:  # if id is not set, we can retry
                    continue

            return task, 201


@routes.http.delete("/task/{id}")
async def delete(
    id: Annotated[str, Path(..., description="task id")],
    queue: Annotated[Queue, Depends(get_queue)],
) -> Annotated[tuple[Literal[b""], Literal[204]], JSONResponse[204]]:
    """
    Delete a not-pending task
    """
    await queue.delete(id)
    return b"", 204


@routes.http.get("/info")
async def info(
    queue: Annotated[Queue, Depends(get_queue)]
) -> Annotated[Any, JSONResponse[200]]:
    """
    Get queue information
    """
    return {
        "length": await queue.length,
        "pending_info": await queue.pending,
    }
