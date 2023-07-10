from kui.asgi import Kui, OpenAPI

from .lifespan import close_queue, init_queue, start_pull_delayed
from .routes import routes

openapi = OpenAPI(
    {"title": "Local Tasks API", "version": "1.0.0"},
    template_name="redoc",
)

app = Kui(
    routes=openapi.routes[1:] + routes,
    on_startup=[init_queue, start_pull_delayed],
    on_shutdown=[close_queue],
)

try:
    import uvicorn

    origin_handle_exit = uvicorn.Server.handle_exit

    def handle_exit(self: uvicorn.Server, sig, frame):
        app.should_exit = True
        return origin_handle_exit(self, sig, frame)

    uvicorn.Server.handle_exit = handle_exit
except ImportError as exc:
    if exc.name != "uvicorn":
        raise


try:
    import runweb.runner.uvicorn
    import uvicorn

    def singleprocess(application: str, bind_address: str) -> None:
        config = uvicorn.Config(
            runweb.runner.uvicorn.parse_application(application),
            access_log=False,
        )
        server = uvicorn.Server(config)
        server.run([runweb.runner.uvicorn.parse_bind(bind_address)])

    runweb.runner.uvicorn.singleprocess = singleprocess
except ImportError as exc:
    if exc.name != "runweb":
        raise
