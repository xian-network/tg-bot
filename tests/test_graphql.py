import asyncio
from contextlib import asynccontextmanager

import pytest
from aiohttp import web

from plugin import GraphQLRequestError, TGBFPlugin, _redact_url_for_log


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


async def test_fetch_graphql_timeout_preserves_request_context():
    async def handle_graphql(request: web.Request) -> web.Response:
        await asyncio.sleep(0.1)
        return web.json_response({"data": {"allEvents": {"nodes": []}}})

    app = web.Application()
    app.router.add_post("/graphql", handle_graphql)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    endpoint = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}/graphql"

    try:
        with pytest.raises(GraphQLRequestError) as caught:
            await build_plugin().fetch_graphql(
                "query GetSwapEvents { allEvents { nodes { id } } }",
                endpoint=endpoint,
                timeout=0.01,
                retry_delay=0,
            )
    finally:
        await runner.cleanup()

    error = caught.value
    assert isinstance(error.__cause__, TimeoutError)
    assert error.status is None
    assert "request timed out after 0.01s" in str(error)
    assert "on 3 attempts" in str(error)
    assert "plugin=chart" in str(error)
    assert "operation=GetSwapEvents" in str(error)
    assert f"endpoint={endpoint}" in str(error)


async def test_fetch_graphql_retries_timed_out_queries():
    requests = 0

    async def handle_graphql(request: web.Request) -> web.Response:
        nonlocal requests
        requests += 1
        if requests == 1:
            await asyncio.sleep(0.05)
        return web.json_response({"data": {"allEvents": {"nodes": []}}})

    app = web.Application()
    app.router.add_post("/graphql", handle_graphql)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    endpoint = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}/graphql"

    try:
        result = await build_plugin().fetch_graphql(
            "query GetSwapEvents { allEvents { nodes { id } } }",
            endpoint=endpoint,
            timeout=0.01,
            retry_delay=0,
        )
    finally:
        await runner.cleanup()

    assert result == {"data": {"allEvents": {"nodes": []}}}
    assert requests == 2


async def test_fetch_graphql_never_retries_mutation_hidden_by_comment():
    requests = 0

    async def handle_graphql(request: web.Request) -> web.Response:
        nonlocal requests
        requests += 1
        await asyncio.sleep(0.05)
        return web.json_response({"data": {"send": True}})

    app = web.Application()
    app.router.add_post("/graphql", handle_graphql)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    endpoint = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}/graphql"

    try:
        with pytest.raises(GraphQLRequestError):
            await build_plugin().fetch_graphql(
                "# query ReadOnly\nmutation SendTokens { send }",
                endpoint=endpoint,
                timeout=0.01,
                retry_delay=0,
            )
    finally:
        await runner.cleanup()

    assert requests == 1


def test_redact_url_for_log_removes_credentials_query_and_fragment():
    assert (
        _redact_url_for_log("https://user:secret@example.com:8443/graphql?token=abc#private")
        == "https://example.com:8443/graphql"
    )


async def test_fetch_graphql_does_not_log_raw_exception_url():
    async def handle_graphql(request: web.Request) -> web.Response:
        return web.Response(text="not json", content_type="text/plain")

    app = web.Application()
    app.router.add_post("/graphql", handle_graphql)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    endpoint = (
        f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
        "/graphql?token=SENSITIVE#private"
    )
    messages = []
    sink = build_plugin().log.add(lambda message: messages.append(str(message)), level="ERROR")

    try:
        with pytest.raises(Exception):
            await build_plugin().fetch_graphql(
                endpoint=endpoint,
                query="query Health { __typename }",
            )
    finally:
        build_plugin().log.remove(sink)
        await runner.cleanup()

    output = "".join(messages)
    assert "SENSITIVE" not in output
    assert "private" not in output
    assert "endpoint=http://127.0.0.1:" in output
    assert "/graphql" in output
