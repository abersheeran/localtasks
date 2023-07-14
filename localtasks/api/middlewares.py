from typing import Annotated

from kui.asgi import Depends, HTTPException, api_key_auth_dependency

from localtasks.settings import settings


async def api_auth(endpoint):
    async def api_auth_wrapper(
        api_token: Annotated[str, Depends(api_key_auth_dependency("api-token"))]
    ):
        if api_token != settings.api_token:
            raise HTTPException(403, content="Invalid API token")
        return await endpoint()

    return api_auth_wrapper
