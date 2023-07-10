from typing import Annotated

from pydantic import Field
from typing_extensions import TypedDict

from localtasks.task import Task


class CreateTaskDto(Task):
    delay_milliseconds: Annotated[
        int, Field(default=0, ge=0, description="delay in milliseconds")
    ]


class ListTaskEntity(TypedDict):
    latest_id: str | int
    tasks: list[Task]
