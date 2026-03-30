import os
import sys
import asyncio
import importlib

import constants as con

from pathlib import Path
from types import ModuleType
from loguru import logger
from dotenv import load_dotenv
from telegram.error import InvalidToken
from telegram.constants import ParseMode
from telegram.ext import Application, Defaults
from config import ConfigManager
from plugin import (
    TGBFPlugin,
    PluginLifecycleError,
    PluginDependencyError,
    PluginManifest,
)
from web import WebAppWrapper


class TelegramBot:
    def __init__(self):
        self.bot = None
        self.cfg = None
        self.web = None
        self.plugins = dict()
        self.plugin_manifests: dict[str, PluginManifest] = {}
        self._stopping = False

    async def cancel_pending_tasks(self):
        """Cancel all pending tasks except the current one"""
        current_task = asyncio.current_task()
        pending = [task for task in asyncio.all_tasks()
                   if task is not current_task and not task.done()]

        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.debug(f"Error canceling task {task.get_name()}: {e}")

    async def shutdown(self):
        """Graceful shutdown sequence"""
        if self._stopping:
            return

        self._stopping = True
        logger.info("Starting shutdown sequence")

        try:
            # First stop the bot application and updater
            if self.bot:
                logger.info("Stopping bot and updater...")
                try:
                    # Stop the bot application first
                    await self.bot.stop()
                    # Then stop the updater
                    if self.bot.updater:
                        await self.bot.updater.stop()
                    # Finally shutdown the application
                    await self.bot.shutdown()
                except Exception as e:
                    logger.debug(f"Bot stop error (can be ignored): {e}")

                await asyncio.sleep(0.5)  # Give it time to clean up

            # Then stop the webserver
            if self.web:
                logger.info("Stopping webserver...")
                await self.web.stop()
                await asyncio.sleep(0.5)  # Give it time to clean up

            # Clean up plugins
            if self.plugins:
                logger.info("Cleaning up plugins...")
                plugin_names = list(self.plugins.keys())
                for name in plugin_names:
                    try:
                        await self.disable_plugin(name)
                    except Exception as e:
                        logger.error(f"Error disabling plugin {name}: {e}")

            # Cancel any remaining tasks
            logger.info("Canceling remaining tasks...")
            await self.cancel_pending_tasks()

            logger.info("Shutdown complete")
            await asyncio.sleep(1)  # Give final logs time to be written

        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
        finally:
            logger.info("Shutdown coroutine finished")

    async def run(self, config: ConfigManager, token: str):
        """Main bot execution loop"""
        try:
            self.cfg = config

            # Init bot
            self.bot = (
                Application.builder()
                .defaults(Defaults(parse_mode=ParseMode.HTML))
                .token(token)
                .build()
            )

            # Init webserver
            self.web = WebAppWrapper(
                res_path=con.DIR_RES,
                port=self.cfg.get('webserver_port')
            )

            # Load all plugins
            await self.load_plugins()

            try:
                # Notify admin about bot start
                await self.bot.updater.bot.send_message(
                    chat_id=self.cfg.get('admin_tg_id'),
                    text=f'{con.ROBOT} Bot is up and running!'
                )
            except InvalidToken:
                logger.error('Invalid Telegram bot token')
                await self.shutdown()
                return

            try:
                async with self.bot:
                    logger.info("Initialize bot...")
                    await self.bot.initialize()
                    logger.info("Starting bot...")
                    await self.bot.start()
                    logger.info("Polling for updates...")
                    await self.bot.updater.start_polling(drop_pending_updates=True)
                    logger.info("Starting webserver...")
                    await self.web.run().serve()
            except Exception as e:
                logger.error(f"Error in bot execution: {e}")
                await self.shutdown()

        except Exception as e:
            logger.error(f"Critical error in main loop: {e}")
            await self.shutdown()

    async def load_plugins(self):
        """Load all plugins from the 'plg' folder"""
        try:
            for _, folders, _ in os.walk(con.DIR_PLG):
                for folder in sorted(folders):
                    if folder.startswith("_"):
                        continue
                    logger.info(f"Plugin '{folder}' loading...")
                    await self.enable_plugin(folder)
                break
        except Exception as e:
            logger.error(f"Error loading plugins: {e}")
            raise

    async def enable_plugin(self, name):
        """Load a single plugin"""
        # If already enabled, disable first
        await self.disable_plugin(name)

        try:
            module = await self._load_plugin_module(name)
            plugin_cls = self._resolve_plugin_class(module, name)
            plugin_instance: TGBFPlugin = await asyncio.to_thread(plugin_cls, self)

            manifest = plugin_instance.manifest
            missing_dependencies = [
                dependency for dependency in manifest.requires if dependency not in self.plugins
            ]
            if missing_dependencies:
                raise PluginDependencyError(
                    name, f"Missing dependencies: {', '.join(sorted(set(missing_dependencies)))}"
                )

            async with plugin_instance as plugin:
                self.plugins[name] = plugin
                self.plugin_manifests[name] = manifest
                msg = f"Plugin '{name}' enabled"
                logger.info(msg)
                return True, msg

        except PluginLifecycleError as exc:
            logger.error(exc)
            return False, exc.message
        except Exception as e:
            logger.exception(f"Plugin '{name}' cannot be enabled")
            return False, str(e)

    async def disable_plugin(self, name):
        """Remove a plugin and cleanup its resources"""
        if name in self.plugins:
            plugin = self.plugins[name]

            try:
                # Run plugin's own cleanup method
                await plugin.cleanup()

                # Remove plugin handlers
                handlers_by_group = list(plugin.handlers.items())
                for group, handlers in handlers_by_group:
                    for handler in list(handlers):
                        try:
                            self.bot.remove_handler(handler, group)
                        except Exception as e:
                            logger.debug(f"Error removing handler: {e}")
                plugin.handlers.clear()

                # Remove plugin endpoints
                endpoints = list(plugin.endpoints)
                for endpoint in endpoints:
                    try:
                        self.web.remove_endpoint(endpoint)
                    except Exception as e:
                        logger.debug(f"Error removing endpoint: {e}")
                plugin.endpoints.clear()

                # Remove all plugin references
                sys.modules.pop(f"{con.DIR_PLG}.{name}.{name}", None)
                sys.modules.pop(f"{con.DIR_PLG}.{name}", None)
                self.plugins.pop(name, None)

                msg = f"Plugin '{name}' disabled"
                logger.info(msg)
                self.plugin_manifests.pop(name, None)
                return True, msg

            except Exception as e:
                logger.error(f"Error disabling plugin {name}: {e}")
                return False, str(e)

    async def _load_plugin_module(self, name: str) -> ModuleType:
        module_path = f"{con.DIR_PLG}.{name}.{name}"

        def _load() -> ModuleType:
            if module_path in sys.modules:
                return importlib.reload(sys.modules[module_path])
            return importlib.import_module(module_path)

        return await asyncio.to_thread(_load)

    @staticmethod
    def _resolve_plugin_class(module: ModuleType, name: str) -> type[TGBFPlugin]:
        class_name = "".join(part.capitalize() for part in name.split("_"))
        if not hasattr(module, class_name):
            raise PluginLifecycleError(name, f"Plugin class '{class_name}' not found in module")

        plugin_cls = getattr(module, class_name)
        if not issubclass(plugin_cls, TGBFPlugin):
            raise PluginLifecycleError(name, f"'{class_name}' is not a TGBFPlugin subclass")
        return plugin_cls


if __name__ == "__main__":
    # Load data from .env file
    load_dotenv()

    # Read parameters from .env file
    log_level = os.getenv('LOG_LEVEL', 'INFO')
    log_into_file = os.getenv('LOG_INTO_FILE', 'true').lower() == 'true'

    # Remove standard logger
    logger.remove()

    # Add new loguru logger
    logger.add(
        sys.stderr,
        level=log_level
    )

    # Save log in file
    if log_into_file:
        logger.add(
            Path(Path('log') / Path('{time}.log')),
            format="{time} {level} {name} {message}",
            level=log_level,
            rotation="5 MB"
        )

    bot = TelegramBot()
    try:
        asyncio.run(bot.run(
            ConfigManager(con.DIR_CFG / con.FILE_CFG),
            os.getenv('TG_TOKEN'))
        )
    except KeyboardInterrupt:
        logger.info("Interrupted by user, shutting down...")
        try:
            asyncio.run(bot.shutdown())
        except RuntimeError:
            # Event loop may already be closed; best effort log
            logger.debug("Event loop already closed during shutdown")
