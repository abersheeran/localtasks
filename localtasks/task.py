import uuid
from typing import Annotated, Literal

from pydantic import BaseModel, BeforeValidator, Field, PlainSerializer


class Task(BaseModel):
    id: Annotated[str, Field(default_factory=lambda: uuid.uuid4().hex)]
    url: str
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"]
    headers: dict[str, str]
    payload: Annotated[
        bytes,
        Field(default=b"", description="Payload in hex format"),
        BeforeValidator(lambda v: (isinstance(v, str) and bytes.fromhex(v)) or v),
        PlainSerializer(lambda v: v.hex(), return_type=str),
    ]
