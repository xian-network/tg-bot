from plg.chart.chart import Chart


def build_chart() -> Chart:
    chart = object.__new__(Chart)
    chart._name = "chart"
    return chart


async def test_fetch_swap_events_paginates_from_latest_timestamp():
    chart = build_chart()
    calls = []

    async def fetch_graphql(query: str, variables: dict) -> dict:
        calls.append((query, variables.copy()))
        if variables["after"] is None:
            return {
                "data": {
                    "allEvents": {
                        "edges": [{"node": {"created": "2026-08-17T10:01:00"}}],
                        "pageInfo": {"hasNextPage": True, "endCursor": "cursor-1"},
                    }
                }
            }
        return {
            "data": {
                "allEvents": {
                    "edges": [{"node": {"created": "2026-08-17T10:02:00"}}],
                    "pageInfo": {"hasNextPage": False, "endCursor": "cursor-2"},
                }
            }
        }

    chart.fetch_graphql = fetch_graphql

    events = await chart.fetch_swap_events_from_graphql(
        "17",
        created_after="2026-08-17T10:00:00",
    )

    assert [event["node"]["created"] for event in events] == [
        "2026-08-17T10:01:00",
        "2026-08-17T10:02:00",
    ]
    query = calls[0][0]
    assert "greaterThanOrEqualTo: $createdAfter" in query
    assert "pageInfo" in query
    assert "id" in query
    assert [variables for _, variables in calls] == [
        {
            "pairId": "17",
            "createdAfter": "2026-08-17T10:00:00",
            "first": 500,
            "after": None,
        },
        {
            "pairId": "17",
            "createdAfter": "2026-08-17T10:00:00",
            "first": 500,
            "after": "cursor-1",
        },
    ]


async def test_trade_refresh_keeps_late_same_timestamp_event_and_backfills_legacy_id():
    chart = build_chart()
    existing_data = {"amount0In": 1, "amount1Out": 2}
    inserted = []
    backfilled = []
    requested_ranges = []

    async def exec_sql(sql: str, *args) -> dict:
        if "SELECT timestamp" in sql:
            assert args == ("17",)
            return {"success": True, "data": [["2026-08-17T10:00:00+00:00"]]}
        if "MIN(timestamp)" in sql:
            assert args == ("17_*",)
            return {"success": True, "data": [["2026-08-17T10:00:00+00:00"]]}
        if "SELECT id, data, timestamp" in sql:
            assert args == ("17", "2026-08-17T10:00:00+00:00")
            return {
                "success": True,
                "data": [[
                    "17_2026-08-17T10:00:00Z",
                    '{"amount0In": 1, "amount1Out": 2}',
                    "2026-08-17T10:00:00+00:00",
                ]],
            }
        if "UPDATE chart_trades" in sql:
            backfilled.append(args)
            return {"success": True, "data": []}
        if "INSERT OR IGNORE" in sql:
            inserted.append(args)
            return {"success": True, "data": []}
        raise AssertionError(sql)

    async def fetch_swap_events(pair_id: str, created_after=None) -> list:
        requested_ranges.append((pair_id, created_after))
        return [
            {
                "node": {
                    "id": 101,
                    "created": "2026-08-17T10:00:00Z",
                    "data": existing_data,
                }
            },
            {
                "node": {
                    "id": 102,
                    "created": "2026-08-17T10:00:00Z",
                    "data": {"amount0In": 3, "amount1Out": 4},
                }
            },
        ]

    chart.exec_sql = exec_sql
    chart.fetch_swap_events_from_graphql = fetch_swap_events

    await chart.fetch_and_store_new_trades("17")

    assert requested_ranges == [("17", "2026-08-17T10:00:00+00:00")]
    assert backfilled == [("101", "17_2026-08-17T10:00:00Z")]
    assert [args[0] for args in inserted] == ["102"]


async def test_trade_refresh_replays_24h_overlap_and_deduplicates_stable_ids():
    chart = build_chart()
    requested_ranges = []
    inserted = []

    async def exec_sql(sql: str, *args) -> dict:
        if "SELECT timestamp" in sql:
            return {"success": True, "data": [["2026-08-17T10:00:00+00:00"]]}
        if "MIN(timestamp)" in sql:
            assert args == ("17_*",)
            return {"success": True, "data": [[None]]}
        if "SELECT id, data, timestamp" in sql:
            assert args == ("17", "2026-08-16T10:00:00+00:00")
            return {
                "success": True,
                "data": [["101", '{"amount0In": 1}', "2026-08-17T10:00:00+00:00"]],
            }
        if "INSERT OR IGNORE" in sql:
            inserted.append(args)
            return {"success": True, "data": []}
        raise AssertionError(sql)

    async def fetch_swap_events(pair_id: str, created_after=None) -> list:
        requested_ranges.append((pair_id, created_after))
        return [
            {
                "node": {
                    "id": 101,
                    "created": "2026-08-17T10:00:00Z",
                    "data": {"amount0In": 1},
                }
            },
            {
                "node": {
                    "id": 102,
                    "created": "2026-08-17T09:00:00Z",
                    "data": {"amount0In": 2},
                }
            },
        ]

    chart.exec_sql = exec_sql
    chart.fetch_swap_events_from_graphql = fetch_swap_events

    await chart.fetch_and_store_new_trades("17")

    assert requested_ranges == [("17", "2026-08-16T10:00:00+00:00")]
    assert [args[0] for args in inserted] == ["102"]


async def test_failed_legacy_id_update_inserts_event_then_removes_legacy_row():
    chart = build_chart()
    inserted = []
    deleted = []

    async def exec_sql(sql: str, *args) -> dict:
        if "SELECT timestamp" in sql:
            return {"success": True, "data": [["2026-08-17T10:00:00+00:00"]]}
        if "MIN(timestamp)" in sql:
            return {"success": True, "data": [["2026-08-17T10:00:00+00:00"]]}
        if "SELECT id, data, timestamp" in sql:
            return {
                "success": True,
                "data": [[
                    "17_2026-08-17T10:00:00Z",
                    '{"amount0In": 1}',
                    "2026-08-17T10:00:00+00:00",
                ]],
            }
        if "UPDATE chart_trades" in sql:
            return {"success": False, "data": "simulated update failure"}
        if "INSERT OR IGNORE" in sql:
            inserted.append(args)
            return {"success": True, "data": []}
        if "DELETE FROM chart_trades" in sql:
            deleted.append(args)
            return {"success": True, "data": []}
        raise AssertionError(sql)

    async def fetch_swap_events(pair_id: str, created_after=None) -> list:
        return [{
            "node": {
                "id": 101,
                "created": "2026-08-17T10:00:00Z",
                "data": {"amount0In": 1},
            }
        }]

    chart.exec_sql = exec_sql
    chart.fetch_swap_events_from_graphql = fetch_swap_events

    await chart.fetch_and_store_new_trades("17")

    assert [args[0] for args in inserted] == ["101"]
    assert deleted == [("17_2026-08-17T10:00:00Z",)]


async def test_failed_trade_insert_stops_before_later_events_advance_checkpoint():
    chart = build_chart()
    inserted = []
    notified = []

    async def exec_sql(sql: str, *args) -> dict:
        if "SELECT timestamp" in sql:
            return {"success": True, "data": []}
        if "MIN(timestamp)" in sql:
            return {"success": True, "data": [[None]]}
        if "SELECT id, data, timestamp" in sql:
            return {"success": True, "data": []}
        if "INSERT OR IGNORE" in sql:
            inserted.append(args[0])
            if args[0] == "101":
                return {"success": False, "data": "simulated insert failure"}
            return {"success": True, "data": []}
        raise AssertionError(sql)

    async def fetch_swap_events(pair_id: str, created_after=None) -> list:
        return [
            {
                "node": {
                    "id": 101,
                    "created": "2026-08-17T09:00:00Z",
                    "data": {"amount0In": 1},
                }
            },
            {
                "node": {
                    "id": 102,
                    "created": "2026-08-17T10:00:00Z",
                    "data": {"amount0In": 2},
                }
            },
        ]

    async def notify(error: Exception):
        notified.append(str(error))

    chart.exec_sql = exec_sql
    chart.fetch_swap_events_from_graphql = fetch_swap_events
    chart.notify = notify

    await chart.fetch_and_store_new_trades("17")

    assert inserted == ["101"]
    assert notified == ["Unable to store GraphQL event 101 for pair 17"]
