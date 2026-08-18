from plg.price.price import Price


def build_price() -> Price:
    price = object.__new__(Price)
    price._name = "price"
    return price


async def test_fetch_swap_events_supplies_required_variables_and_paginates():
    price = build_price()
    calls = []

    async def fetch_graphql(query: str, variables: dict) -> dict:
        calls.append((query, variables.copy()))
        if variables["after"] is None:
            return {
                "data": {
                    "allEvents": {
                        "edges": [{"node": {"id": "swap-1"}}],
                        "pageInfo": {"hasNextPage": True, "endCursor": "cursor-1"},
                    }
                }
            }
        return {
            "data": {
                "allEvents": {
                    "edges": [{"node": {"id": "swap-2"}}],
                    "pageInfo": {"hasNextPage": False, "endCursor": "cursor-2"},
                }
            }
        }

    price.fetch_graphql = fetch_graphql

    events = await price.fetch_swap_events("17")

    assert [edge["node"]["id"] for edge in events] == ["swap-1", "swap-2"]
    assert "greaterThanOrEqualTo: $createdAfter" in calls[0][0]
    assert [variables for _, variables in calls] == [
        {
            "pairId": "17",
            "createdAfter": "1970-01-01T00:00:00Z",
            "first": 500,
            "after": None,
        },
        {
            "pairId": "17",
            "createdAfter": "1970-01-01T00:00:00Z",
            "first": 500,
            "after": "cursor-1",
        },
    ]
