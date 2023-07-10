# localtasks

Google Cloud Tasks Queue substitute for local

- **Reliable**: All tasks will be guaranteed to be executed at least once (very few repetitions).
- **Simple**: Just need one Redis server. (You can also select clusters)
- **Fast**: All operations use only one Redis command or Lua script.
- **Scalable**: You can deploy multiple Workers to handle more tasks.
- **Multifunction**: Not only supports real-time tasks, but also supports delayed tasks.

## Quick Start

Download [docker-compose.yml](https://github.com/abersheeran/localtasks/blob/main/docker-compose.yml), then run `docker-compose up -d`.

LocalTasks will be available at `http://localhost:9998`. Operate according to the API documentation given on the page.

## Expansion

If a single worker process cannot handle your tasks in time, you can deploy multiple workers at will. Local Tasks will ensure that a task will only be processed by one Worker.

There is no need to worry if the Worker is offline. When at least one Worker is working, it will automatically take over the unfinished tasks of other Workers (this may cause repeated execution of tasks, but it is rare).

Deploying multiple API services is also allowed.
