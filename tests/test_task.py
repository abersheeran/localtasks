import json

from localtasks.task import Task


def test_task():
    task = Task.model_validate(
        {
            "url": "https://example.com",
            "method": "GET",
            "headers": {},
            "payload": "1234567890",
        }
    )
    assert task.id
    assert task.payload == bytes.fromhex("1234567890"), task.payload

    assert json.loads(task.model_dump_json())["payload"] == "1234567890"
