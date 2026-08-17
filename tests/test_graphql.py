from contextlib import asynccontextmanager

import pytest
from aiohttp import web

from plugin import GraphQLRequestError, TGBFPlugin


@asynccontextmanager
async def graphql_server(payload: dict, status: int = 200):
    async def handle_graphql(request: web.Request) -> web.Response:
        return web.json_response(payload, status=status)

    app = web.Application()
    app.router.add_post("/graphql", handle_graphql)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]

    try:
        yield f"http://127.0.0.1:{port}/graphql"
    finally:
        await runner.cleanup()


def build_plugin(name: str = "chart") -> TGBFPlugin:
    plugin = object.__new__(TGBFPlugin)
    plugin._name = name
    return plugin


async def test_fetch_graphql_preserves_error_details_and_request_context():
    payload = {
        "data": {"allEvents": None},
        "errors": [
            {
                "message": "canceling statement due to statement timeout",
                "path": ["allEvents"],
                "extensions": {"code": "INTERNAL_SERVER_ERROR"},
            }
        ],
    }
    query = "query GetSwapEvents { allEvents { nodes { id } } }"

    async with graphql_server(payload) as endpoint:
        with pytest.raises(GraphQLRequestError) as caught:
            await build_plugin().fetch_graphql(query, endpoint=endpoint)

    error = caught.value
    message = str(error)
    assert "canceling statement due to statement timeout" in message
    assert "allEvents" in message
    assert "INTERNAL_SERVER_ERROR" in message
    assert "plugin=chart" in message
    assert "operation=GetSwapEvents" in message
    assert f"endpoint={endpoint}" in message
    assert error.status == 200
    assert error.errors == payload["errors"]


async def test_fetch_graphql_handles_null_error_extensions_without_losing_context():
    payload = {
        "errors": [
            {
                "message": "database connection was closed",
                "path": ["allEvents"],
                "extensions": None,
            }
        ]
    }

    async with graphql_server(payload) as endpoint:
        with pytest.raises(GraphQLRequestError) as caught:
            await build_plugin().fetch_graphql(
                "query GetSwapEvents { allEvents { nodes { id } } }",
                endpoint=endpoint,
            )

    assert "database connection was closed" in str(caught.value)
    assert "plugin=chart" in str(caught.value)
    assert caught.value.errors == payload["errors"]
