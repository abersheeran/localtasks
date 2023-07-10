import asyncio
import os

import httpx
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager


@pytest_asyncio.fixture
async def app():
    os.environ["REDIS"] = "redis://localhost:6379/0"
    os.environ["LOG_LEVEL"] = "TRACE"

    from localtasks.api import app

    async with LifespanManager(app) as manager:
        yield manager.app
        await app.state.queue.connection.flushdb()


@pytest_asyncio.fixture
async def client(app):
    async with httpx.AsyncClient(app=app, base_url="http://testserver") as client:
        yield client


@pytest.mark.asyncio
async def test_list_tasks(client: httpx.AsyncClient):
    response = await client.get("/task")
    assert response.status_code == 200
    assert response.json() == {"latest_id": "-", "tasks": []}

    resp = await client.post(
        "/task",
        json={
            "url": "https://aber.sh",
            "method": "GET",
            "headers": {"User-Agent": "localtasks"},
            "payload": "",
        },
    )
    assert resp.status_code == 201, resp.text
    response = await client.get("/task")
    assert response.status_code == 200
    assert len(response.json()["tasks"]) == 1


@pytest.mark.asyncio
async def test_list_todo_tasks(client: httpx.AsyncClient):
    resp = await client.post(
        "/task",
        json={
            "url": "https://aber.sh",
            "method": "GET",
            "headers": {"User-Agent": "localtasks"},
            "payload": "",
            "delay_milliseconds": 1,
        },
    )
    assert resp.status_code == 201, resp.text
    await asyncio.sleep(1)
    response = await client.get("/task", params={"type": "todo"})
    assert response.status_code == 200
    assert len(response.json()["tasks"]) == 1


@pytest.mark.asyncio
async def test_list_delay_tasks(client: httpx.AsyncClient):
    resp = await client.post(
        "/task",
        json={
            "url": "https://aber.sh",
            "method": "GET",
            "headers": {"User-Agent": "localtasks"},
            "payload": "",
            "delay_milliseconds": 10 * 1000,
        },
    )
    assert resp.status_code == 201, resp.text

    response = await client.get("/task", params={"type": "todo"})
    assert response.status_code == 200
    assert len(response.json()["tasks"]) == 0, response.json()

    response = await client.get("/task", params={"type": "delay"})
    assert response.status_code == 200
    assert len(response.json()["tasks"]) == 1, response.json()


@pytest.mark.asyncio
async def test_delete_task(client: httpx.AsyncClient):
    resp = await client.post(
        "/task",
        json={
            "url": "https://aber.sh",
            "method": "GET",
            "headers": {"User-Agent": "localtasks"},
            "payload": "",
            "delay_milliseconds": 10 * 1000,
        },
    )
    assert resp.status_code == 201, resp.text
    task_id = resp.json()["id"]

    response = await client.delete(f"/task/{task_id}")
    assert response.status_code == 204, response.text


@pytest.mark.asyncio
async def test_info(client: httpx.AsyncClient):
    resp = await client.get("/info")
    assert resp.status_code == 200, resp.text
    assert resp.json()["length"] == 0
    assert resp.json()["pending_info"] == {
        "pending": 0,
        "min": None,
        "max": None,
        "consumers": [],
    }
