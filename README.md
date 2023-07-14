# localtasks

Google Cloud Tasks Queue substitute in local

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

## Configuration

### Environment Variables

| Name           | Default                  | Description                                          |
| -------------- | ------------------------ | ---------------------------------------------------- |
| LOG_LEVEL      | INFO                     | Log level                                            |
| REDIS          | redis://localhost:6379/0 | Redis connection string                              |
| API_TOKEN      |                          | API token, if not set, no token is required          |
| CONSUMER_NAME  | random uuid4 hex         | Consumer name, used to distinguish different workers |
| MAX_CONCURRENT | 300                      | Maximum number of concurrent tasks                   |
| TIMEOUT        | 1800.0                   | Task timeout (seconds)                               |
| MAX_RETRIES    |                          | Maximum number of retries, default is no limit       |
| MIN_INTERVAL   | 0.1                      | Minimum interval between retries (seconds)           |
| MAX_INTERVAL   | 3600.0                   | Maximum interval between retries (seconds)           |
| MAX_DOUBLING   | 16                       | Maximum number of times of doubling.                 |

* If `API_TOKEN` is seted, the token must be passed in the `api-token` header of the request.
* When an executing task is still not acked after the timeout plus one minute, it will be taken over by the running Worker and re-executed.
* The waiting time before each retry will be double the waiting time of the previous retry. Use `MIN_INTERVAL`, `MAX_INTERVAL` and `MAX_DOUBLING` to limit the waiting time.
