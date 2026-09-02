from types import SimpleNamespace

import pytest

import plugin as plugin_module
from plugin import TGBFPlugin


class FakeConfig:
    def get(self, key, *args):
        assert key == "admin_tg_id"
        return 123


class FakeTelegramBot:
    def __init__(self):
        self.messages = []

    async def send_message(self, chat_id, text):
        self.messages.append((chat_id, text))


class CancellingTelegramBot(FakeTelegramBot):
    async def send_message(self, chat_id, text):
        raise plugin_module.asyncio.CancelledError


class FailingFirstTelegramBot(FakeTelegramBot):
    def __init__(self):
        super().__init__()
        self.started = plugin_module.asyncio.Event()
        self.release = plugin_module.asyncio.Event()
        self.attempts = 0

    async def send_message(self, chat_id, text):
        self.attempts += 1
        if self.attempts == 1:
            self.started.set()
            await self.release.wait()
            raise RuntimeError("Telegram unavailable")
        await super().send_message(chat_id, text)


def build_plugin() -> tuple[TGBFPlugin, FakeTelegramBot]:
    sender = FakeTelegramBot()
    plugin = object.__new__(TGBFPlugin)
    plugin._name = "chart"
    plugin._cfg_global = FakeConfig()
    plugin._tgb = SimpleNamespace(
        bot=SimpleNamespace(updater=SimpleNamespace(bot=sender))
    )
    return plugin, sender


def timeout_error() -> TimeoutError:
    try:
        raise TimeoutError()
    except TimeoutError as error:
        return error


async def test_notify_adds_exception_origin_and_suppresses_repeated_alerts(monkeypatch):
    now = [100.0]
    monkeypatch.setattr(plugin_module, "monotonic", lambda: now[0], raising=False)
    plugin, sender = build_plugin()

    assert await plugin.notify(timeout_error()) is True
    assert await plugin.notify(timeout_error()) is True

    assert len(sender.messages) == 1
    first_message = sender.messages[0][1]
    assert "plugin=chart" in first_message
    assert "exception=TimeoutError" in first_message
    assert "test_notifications.py" in first_message
    assert "timeout_error" in first_message

    now[0] += 3601
    assert await plugin.notify(timeout_error()) is True

    assert len(sender.messages) == 2
    assert "suppressed 1 similar alert" in sender.messages[1][1]


async def test_notify_restores_cooldown_state_when_cancelled():
    plugin, _ = build_plugin()
    sender = CancellingTelegramBot()
    plugin._tgb.bot.updater.bot = sender

    with pytest.raises(plugin_module.asyncio.CancelledError):
        await plugin.notify(timeout_error())

    assert plugin._notification_state == {}


async def test_notify_bounds_unique_exception_fingerprints(monkeypatch):
    monkeypatch.setattr(plugin_module, "monotonic", lambda: 100.0, raising=False)
    plugin, _ = build_plugin()

    for number in range(300):
        await plugin.notify(RuntimeError(f"remote error {number}"))

    assert len(plugin._notification_state) <= 256


async def test_concurrent_duplicate_retries_after_first_send_fails():
    plugin, _ = build_plugin()
    sender = FailingFirstTelegramBot()
    plugin._tgb.bot.updater.bot = sender
    error = timeout_error()

    first = plugin_module.asyncio.create_task(plugin.notify(error))
    await sender.started.wait()
    second = plugin_module.asyncio.create_task(plugin.notify(error))
    await plugin_module.asyncio.sleep(0)

    assert second.done() is False

    sender.release.set()
    assert await first is False
    assert await second is True
    assert sender.attempts == 2
    assert len(sender.messages) == 1
