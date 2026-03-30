from main import TelegramBot
from plugin import TGBFPlugin


class FakeBot:
    def __init__(self):
        self.added = []
        self.removed = []

    def add_handler(self, handler, group):
        self.added.append((handler, group))

    def remove_handler(self, handler, group):
        self.removed.append((handler, group))


class FakeWeb:
    def __init__(self):
        self.removed = []

    def remove_endpoint(self, endpoint):
        self.removed.append(endpoint)


def build_plugin(tgb: TelegramBot, name: str = "demo") -> TGBFPlugin:
    plugin = object.__new__(TGBFPlugin)
    plugin._tgb = tgb
    plugin._name = name
    plugin._handlers = {}
    plugin._endpoints = {}
    plugin._cfg_global = None
    plugin._cfg = None
    plugin._manifest_cache = None
    return plugin


async def test_remove_handler_keeps_other_handlers_in_same_group():
    tgb = TelegramBot()
    tgb.bot = FakeBot()

    plugin = build_plugin(tgb)
    handler_one = object()
    handler_two = object()

    await plugin.add_handler(handler_one, group=7)
    await plugin.add_handler(handler_two, group=7)
    await plugin.remove_handler(handler_one)

    assert tgb.bot.removed == [(handler_one, 7)]
    assert plugin.handlers == {7: [handler_two]}


async def test_disable_plugin_removes_all_handlers_in_same_group():
    tgb = TelegramBot()
    tgb.bot = FakeBot()
    tgb.web = FakeWeb()

    plugin = build_plugin(tgb)
    handler_one = object()
    handler_two = object()

    await plugin.add_handler(handler_one, group=11)
    await plugin.add_handler(handler_two, group=11)

    tgb.plugins["demo"] = plugin
    tgb.plugin_manifests["demo"] = object()

    success, message = await tgb.disable_plugin("demo")

    assert success is True
    assert message == "Plugin 'demo' disabled"
    assert tgb.bot.removed == [(handler_one, 11), (handler_two, 11)]
    assert plugin.handlers == {}
    assert "demo" not in tgb.plugins
    assert "demo" not in tgb.plugin_manifests
