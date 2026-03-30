from __future__ import annotations

import os
import inspect
import asyncio
import aiohttp
import pickledb
import aiosqlite

import constants as c
import utils as utl

from dataclasses import dataclass, field
from pathlib import Path
from loguru import logger
from functools import wraps
from pickledb import PickleDB
from xian_py import XianAsync
from xian_py.wallet import Wallet
from telegram.constants import ChatAction
from telegram import Chat, Update, Message
from typing import TYPE_CHECKING, Any, BinaryIO, Callable, ClassVar, Dict, Iterable, Optional, Tuple
from telegram.ext import CallbackContext, BaseHandler, Job, CallbackQueryHandler
from datetime import datetime, timedelta
from config import ConfigManager, ConfigError

if TYPE_CHECKING:
    from main import TelegramBot


@dataclass(slots=True, frozen=True)
class PluginManifest:
    """Declarative metadata describing a plugin."""

    name: Optional[str] = None
    description: Optional[str] = None
    category: Optional[str] = None
    requires: Tuple[str, ...] = field(default_factory=tuple)
    version: Optional[str] = None
    exposed_routes: Tuple[str, ...] = field(default_factory=tuple)

    def materialize(self, plugin: "TGBFPlugin") -> "PluginManifest":
        """Fill in missing fields using runtime data from ``plugin``."""
        requires = self.requires or getattr(type(plugin), "requires", tuple())
        return PluginManifest(
            name=(self.name or plugin.name),
            description=self.description or plugin.description,
            category=self.category or plugin.category,
            requires=tuple(dict.fromkeys(dep.lower() for dep in requires if dep)),
            version=self.version,
            exposed_routes=tuple(dict.fromkeys(self.exposed_routes)),
        )


class PluginLifecycleError(RuntimeError):
    """Base error for plugin lifecycle failures."""

    def __init__(self, plugin: str, message: str):
        super().__init__(f"[{plugin}] {message}")
        self.plugin = plugin
        self.message = message


class PluginDependencyError(PluginLifecycleError):
    """Raised when plugin dependencies are not satisfied."""


class TGBFPlugin:
    log = logger
    MANIFEST: ClassVar[Optional[PluginManifest]] = None
    requires: ClassVar[Tuple[str, ...]] = tuple()

    def __init__(self, tgb: TelegramBot):
        # Parent that instantiated this plugin
        self._tgb = tgb

        # Set class name as name of this plugin
        self._name = type(self).__name__.lower()

        # All bot handlers for this plugin
        self._handlers: Dict[int, list[BaseHandler]] = dict()

        # All endpoints of this plugin
        self._endpoints: Dict[str, Callable] = dict()

        # Access to global config
        self._cfg_global = self._tgb.cfg

        # Access to plugin config
        try:
            self._cfg = ConfigManager(self.get_cfg_path() / self.get_cfg_name())
        except ConfigError as exc:
            raise PluginLifecycleError(self.name, f"Unable to load configuration: {exc}") from exc

        self._manifest_cache: Optional[PluginManifest] = None

    async def __aenter__(self):
        """ Executes init() method. Make sure to return 'self' if you override it """
        await self.init()

        # Create global db table for wallets
        if not await self.table_exists_global("wallets"):
            sql = await self.get_resource_global("create_wallets.sql")
            await self.exec_sql_global(sql)

        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """ This method gets executed after the plugin is loaded """
        pass

    async def init(self):
        method = inspect.currentframe().f_code.co_name
        raise NotImplementedError(f"Method '{method}' not implemented")

    async def cleanup(self):
        """ Overwrite this method if you want to clean something up
         before the plugin will be disabled """
        pass

    @property
    def tgb(self) -> TelegramBot:
        return self._tgb

    @property
    def name(self) -> str:
        """ Return the name of the current plugin """
        return self._name

    @property
    def handle(self) -> str:
        """ Return the command string that triggers the plugin """
        handle = self.cfg.get("handle")
        return handle.lower() if handle else self.name

    @property
    def category(self) -> str:
        """ Return the category of the plugin for the 'help' command """
        return self.cfg.get("category")

    @property
    def description(self) -> str:
        """ Return the description of the plugin """
        return self.cfg.get("description")

    @property
    def aliases(self) -> str:
        """ Return a list of aliases for this command """
        return self.cfg.get("aliases")

    @property
    def plugins(self) -> Dict:
        """ Return a dict with all plugins: key = plugin name, value = plugin """
        return self.tgb.plugins

    @property
    def jobs(self) -> Tuple[Job, ...]:
        """ Return a tuple with all currently active jobs """
        return self.tgb.bot.job_queue.jobs()

    @property
    def cfg_global(self) -> ConfigManager:
        """ Return the global configuration """
        return self._cfg_global

    @property
    def cfg(self) -> ConfigManager:
        """ Return the configuration for this plugin """
        return self._cfg

    @property
    def handlers(self) -> Dict[int, list[BaseHandler]]:
        """ Return bot handlers grouped by Telegram handler group """
        return self._handlers

    @property
    def endpoints(self) -> Dict[str, Callable]:
        """ Return a list of bot endpoints for this plugin """
        return self._endpoints

    @property
    def manifest(self) -> PluginManifest:
        """Return the resolved manifest for this plugin instance."""
        if self._manifest_cache is None:
            template = getattr(type(self), "MANIFEST", None)
            manifest = template.materialize(self) if isinstance(template, PluginManifest) else None

            if manifest is None:
                manifest = PluginManifest(
                    name=self.name,
                    description=self.description,
                    category=self.category,
                    requires=tuple(dict.fromkeys(dep.lower() for dep in getattr(type(self), "requires", tuple()) if dep)),
                )

            self._manifest_cache = manifest
        return self._manifest_cache

    async def add_handler(self, handler: BaseHandler, group: int = None):
        """ Will add bot handlers to this plugins list of handlers
         and also add them to the bot dispatcher """

        if group is None:
            if isinstance(handler, CallbackQueryHandler) and handler.pattern:
                group = 0
            else:
                group = abs(hash(self.name)) % 1000 + 1

        self.tgb.bot.add_handler(handler, group)
        self.handlers.setdefault(group, []).append(handler)

        self.log.info(f"Plugin '{self.name}': {type(handler).__name__} added to group {group}")

    async def remove_handler(self, handler: BaseHandler):
        """ Removed the given handler from the bot """

        for group, handlers in list(self.handlers.items()):
            if handler in handlers:
                self.tgb.bot.remove_handler(handler, group)
                handlers.remove(handler)
                if not handlers:
                    del self.handlers[group]
                break

        self.log.info(f"Plugin '{self.name}': {type(handler).__name__} removed")

    async def add_endpoint(self, name: str, action):
        """ Adds a webserver endpoint """

        self.tgb.web.add_endpoint(name, action)
        self.endpoints[name] = action

        self.log.info(f"Plugin '{self.name}': Endpoint '{name}' added")

    async def remove_endpoint(self, name: str):
        """ Remove an existing endpoint from webserver """

        self.tgb.web.remove_endpoint(name)
        del self.endpoints[name]

        self.log.info(f"Plugin '{self.name}': Endpoint '{name}' removed")

    # TODO: If not in private, remove after certain time
    async def get_info(self, replace: dict = None):
        """
        Return info about the command. Default resource '<plugin>.txt'
        will be loaded from the resource folder and if you provide a
        dict with '<placeholder>,<value>' entries then placeholders in
        the resource will be replaced with the corresponding <value>.

        The placeholders need to be wrapped in double curly brackets
        """

        usage = await self.get_resource(f"{self.name}.html")

        if usage:
            usage = usage.replace("{{handle}}", self.handle)

            if replace:
                for placeholder, value in replace.items():
                    usage = usage.replace(placeholder, str(value))

            return usage

        await self.notify(f'No usage info for plugin <b>{self.name}</b>')
        return f'{c.ERROR} Could not retrieve usage info'

    async def get_img(self, filename: str = None) -> BinaryIO | None:
        """ If 'filename' is supplied then the content of the file from
        the 'res' directory of the plugin will be returned. If it's
        empty then content of file <plugin_name>.png will be returned. """

        if not filename:
            filename = f'{self.name}.png'

        filepath = self.get_res_path() / filename

        if not filepath.is_file():
            await self.notify(f'File not found: {filepath}')
            return None

        return open(filepath, "rb")

    async def get_resource_global(self, filename):
        """ Return the content of the given file
        from the global resource directory """

        path = Path(Path.cwd() / c.DIR_RES / filename)
        return await self._get_resource_content(path)

    async def get_resource(self, filename, plugin=None):
        """ Return the content of the given file from
        the resource directory of the given plugin """

        path = os.path.join(self.get_res_path(plugin), filename)
        return await self._get_resource_content(path)

    async def _get_resource_content(self, path):
        """ Return the content of the file in the given path """

        try:
            with open(path, "r", encoding="utf8") as f:
                return f.read()
        except Exception as e:
            self.log.error(e)
            await self.notify(e)

    async def get_jobs(self, name=None) -> Tuple[Job, ...]:
        """ Return jobs with given name or all jobs if not name given """

        if name:
            # Get all jobs with given name
            return await self.tgb.bot.job_queue.get_jobs_by_name(name)
        else:
            # Return all jobs
            return await self.tgb.bot.job_queue.jobs()

    def run_repeating(self, callback, interval, first=0, last=None, data=None, name=None):
        """ Executes the provided callback function indefinitely.
        It will be executed every 'interval' (seconds) time. The
        created job will be returned by this method. If you want
        to stop the job, execute 'schedule_removal()' on it.

        The job will be added to the job queue and the default
        name of the job (if no 'name' provided) will be the name
        of the plugin plus some random data"""

        name = name if name else (self.name + "_" + utl.random_id())

        return self.tgb.bot.job_queue.run_repeating(
            callback,
            interval,
            first=first,
            last=last,
            data=data,
            name=name)

    def run_once(self, callback, when, data=None, name=None):
        """ Executes the provided callback function only one time.
        It will be executed at the provided 'when' time. The
        created job will be returned by this method. If you want
        to stop the job before it gets executed, execute
        'schedule_removal()' on it.

        The job will be added to the job queue and the default
        name of the job (if no 'name' provided) will be the name
        of the plugin """

        return self.tgb.bot.job_queue.run_once(
            callback,
            when,
            data=data,
            name=name if name else (self.name + "_" + utl.random_id()))

    def _get_kv(self, plugin="", db_name="") -> PickleDB:
        if db_name:
            if not db_name.lower().endswith(".kv"):
                db_name += ".kv"
        else:
            if plugin:
                db_name = plugin + ".kv"
            else:
                db_name = self.name + ".kv"

        plugin = plugin if plugin else self.name
        db_path = Path(self.get_dat_path(plugin=plugin) / db_name)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        return pickledb.load(db_path, True)

    def kv_set(self, key, value, plugin="", db_name=""):
        kv_db = self._get_kv(plugin, db_name)
        return kv_db.set(key, value)

    def kv_get(self, key, plugin="", db_name=""):
        kv_db = self._get_kv(plugin, db_name)
        return kv_db.get(key)

    def kv_all(self, plugin="", db_name=""):
        kv_db = self._get_kv(plugin, db_name)
        return kv_db.getall()

    def kv_del(self, key, plugin="", db_name="", is_prefix: bool = False):
        kv_db = self._get_kv(plugin, db_name)
        if is_prefix:
            for entry in [k for k in kv_db.getall() if k.startswith(key)]:
                kv_db.rem(entry)
            kv_db.dump()
        else:
            return kv_db.rem(key)

    async def fetch_graphql(
            self,
            query: str,
            variables: dict = None,
            endpoint: str = None,
            headers: dict = None,
            timeout: float = 30.0
    ) -> dict:
        """
        Execute a GraphQL query and return the results.

        Args:
            query: The GraphQL query string
            variables: Optional variables for the query
            endpoint: GraphQL endpoint URL (falls back to config if not provided)
            headers: Optional additional headers
            timeout: Request timeout in seconds

        Returns:
            Dict containing the GraphQL response

        Raises:
            Exception: When the query fails
        """
        variables = variables or {}
        endpoint = endpoint or self.cfg_global.get('xian', 'graph_ql')

        # Prepare default headers
        default_headers = {'Content-Type': 'application/json'}
        if headers:
            default_headers.update(headers)

        payload = {
            'query': query,
            'variables': variables
        }

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                        endpoint,
                        json=payload,
                        headers=default_headers,
                        timeout=aiohttp.ClientTimeout(total=timeout)
                ) as response:
                    result = await response.json()

                    # Check for HTTP error status
                    if response.status != 200:
                        raise Exception(f"GraphQL query failed with status code {response.status}")

                    # Check for GraphQL errors
                    if 'errors' in result:
                        raise Exception(f"GraphQL query returned errors")

                    return result

        except Exception as e:
            self.log.error(f"GraphQL error: {e}")
            raise

    async def exec_sql_global(self, sql, *args, db_name=""):
        """ Execute raw SQL statement on the global
        database and return the result

        param: sql = the SQL query
        param: *args = arguments for the SQL query
        param: db_name = name of the database file

        Following data will be returned
        If error happens:
        {"success": False, "data": None}

        If no data available:
        {"success": True, "data": None}

        If database disabled:
        {"success": False, "data": "Database disabled"} """

        if db_name:
            if not db_name.lower().endswith(".db"):
                db_name += ".db"
        else:
            db_name = c.FILE_DAT

        db_path = Path.cwd() / c.DIR_DAT / db_name
        return await self._exec_on_db(db_path, sql, *args)

    async def exec_sql(self, sql, *args, plugin="", db_name=""):
        """ Execute raw SQL statement on database for given
        plugin and return the result.

        param: sql = the SQL query
        param: *args = arguments for the SQL query
        param: plugin = name of plugin that DB belongs too
        param: db_name = name of DB in case it's not the
        default (the name of the plugin)

        Following data will be returned
        If error happens:
        {"success": False, "data": None}

        If no data available:
        {"success": True, "data": None}

        If database disabled:
        {"success": False, "data": "Database disabled"} """

        if db_name:
            if not db_name.lower().endswith(".db"):
                db_name += ".db"
        else:
            if plugin:
                db_name = plugin + ".db"
            else:
                db_name = self.name + ".db"

        plugin = plugin if plugin else self.name
        db_path = Path(self.get_dat_path(plugin=plugin) / db_name)

        return await self._exec_on_db(db_path, sql, *args)

    async def _exec_on_db(self, db_path: Path, sql: str, *args) -> Dict[str, Any]:
        """ Open database connection and execute SQL statement """

        res = {"data": None, "success": None}

        sql_str = ' '.join(sql.split())
        self.log.debug(f"Access DB '{db_path}' with SQL '{sql_str}' and values '{args}'")

        try:
            # Create directory if it doesn't exist
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)

            async with aiosqlite.connect(db_path, timeout=5) as con:
                cur = await con.cursor()
                await cur.execute(sql, args)
                await con.commit()

                res["data"] = await cur.fetchall()
                res["success"] = True
        except Exception as e:
            res["data"] = str(e)
            res["success"] = False
            self.log.error(e)
            await self.notify(e)

        return res

    async def table_exists_global(self, table_name, db_name="") -> bool:
        """ Return TRUE if given table exists in global database, otherwise FALSE """

        if db_name:
            if not db_name.lower().endswith(".db"):
                db_name += ".db"
        else:
            db_name = c.FILE_DAT

        db_path = Path(Path.cwd() / c.DIR_DAT / db_name)
        return await self._db_table_exists(db_path, table_name)

    async def table_exists(self, table_name, plugin=None, db_name=None) -> bool:
        """ Return TRUE if given table exists in given plugin, otherwise FALSE """

        if db_name:
            if not db_name.lower().endswith(".db"):
                db_name += ".db"
        else:
            if plugin:
                db_name = plugin + ".db"
            else:
                db_name = self.name + ".db"

        plugin = plugin if plugin else self.name
        db_path = Path(self.get_dat_path(plugin=plugin) / db_name)

        return await self._db_table_exists(db_path, table_name)

    async def _db_table_exists(self, db_path, table_name) -> bool:
        """ Open connection to database and check if given table exists """

        if not db_path.is_file():
            return False

        exists = False
        statement = await self.get_resource_global("table_exists.sql")

        try:
            async with aiosqlite.connect(db_path) as con:
                cur = await con.cursor()
                await cur.execute(statement, [table_name])
                result = await cur.fetchone()
                if result:
                    exists = True
        except Exception as e:
            self.log.error(e)
            await self.notify(e)

        return exists

    def get_res_path(self, plugin=None) -> Path:
        """ Return path of resource directory for given plugin """
        plugin = plugin if plugin else self.name
        return Path(c.DIR_PLG, plugin, c.DIR_RES)

    def get_cfg_path(self, plugin=None) -> Path:
        """ Return path of configuration directory for the given plugin """
        plugin = plugin if plugin else self.name
        return Path(c.DIR_PLG / plugin / c.DIR_CFG)

    def get_cfg_name(self, plugin=None) -> Path:
        """ Return name of configuration file for given plugin """
        plugin = plugin if plugin else self.name
        return Path(plugin).with_suffix(c.CFG_EXT)

    def get_dat_path(self, plugin=None) -> Path:
        """ Return path of data directory for given plugin """
        plugin = plugin if plugin else self.name
        return Path(c.DIR_PLG, plugin, c.DIR_DAT)

    def get_plg_path(self, plugin=None) -> Path:
        """ Return path of given plugin directory """
        plugin = plugin if plugin else self.name
        return Path(c.DIR_PLG, plugin)

    def get_plugin(self, plugin_name):
        """ Return the plugin with the given name """
        if plugin_name in self.plugins:
            return self.plugins[plugin_name]

    def is_enabled(self, plugin_name) -> bool:
        """ Return TRUE if the given plugin is enabled or FALSE otherwise """
        return plugin_name in self.plugins

    def is_private(self, message: Message) -> bool:
        """ Check if message was sent in a private chat or not """
        return message.chat.type == Chat.PRIVATE

    async def remove_msg_after(self, *messages: Message, after_secs):
        """ Remove a Telegram message after a given time """

        async def remove_msg_job(context: CallbackContext):
            param_lst = str(context.job.data).split("_")
            chat_id = param_lst[0]
            msg_id = int(param_lst[1])

            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
            except Exception as e:
                self.log.error(f"Not possible to remove message: {e}")

        for message in messages:
            self.run_once(
                remove_msg_job,
                datetime.utcnow() + timedelta(seconds=after_secs),
                data=f"{message.chat_id}_{message.message_id}")

    async def notify(self, msg: str | Exception) -> bool:
        """ Admin in global config will get a message with the given text.
         Primarily used for exceptions but can be used with other inputs too. """

        msg = repr(msg) if isinstance(msg, Exception) else msg

        admin = self.cfg_global.get('admin_tg_id')

        try:
            await self.tgb.bot.updater.bot.send_message(admin, f"{c.ALERT} {msg}")
        except Exception as e:
            error = f"Not possible to notify admin id '{admin}'"
            self.log.error(f"{error}: {e}")
            return False

        return True

    @classmethod
    def logging(cls):
        """ Decorator for logging out 'update' object from bot """

        def decorator(func):
            @wraps(func)
            async def _logging(self, update: Update, context: CallbackContext, **kwargs):
                TGBFPlugin.log.debug(f'update: {update}')

                if asyncio.iscoroutinefunction(func):
                    return await func(self, update, context, **kwargs)
                else:
                    return func(self, update, context, **kwargs)

            return _logging
        return decorator

    @classmethod
    def private(cls, hidden: bool = False, remove_after: int = 10):
        """ Decorator for methods that need to be run in a private chat with the bot """

        def decorator(func):
            @wraps(func)
            async def _private(self, update: Update, context: CallbackContext, **kwargs):
                if (await context.bot.get_chat(update.effective_chat.id)).type == Chat.PRIVATE:
                    if asyncio.iscoroutinefunction(func):
                        return await func(self, update, context, **kwargs)
                    else:
                        return func(self, update, context, **kwargs)

                if (not hidden) and update.message:
                    name = context.bot.username if context.bot.username else context.bot.name
                    msg = f"{c.ERROR} Use this command in a chat with the bot @{name}"
                    reply = await update.message.reply_text(msg)

                    if remove_after:
                        await self.remove_msg_after(update.message, reply, after_secs=remove_after)

            return _private
        return decorator

    @classmethod
    def public(cls, hidden: bool = False):
        """ Decorator for methods that need to be run in a public group """

        def decorator(func):
            @wraps(func)
            async def _public(self, update: Update, context: CallbackContext, **kwargs):
                if (await context.bot.get_chat(update.effective_chat.id)).type != Chat.PRIVATE:
                    if asyncio.iscoroutinefunction(func):
                        return await func(self, update, context, **kwargs)
                    else:
                        return func(self, update, context, **kwargs)

                if (not hidden) and update.message:
                    msg = f"{c.ERROR} Can only be used in a public chat"
                    await update.message.reply_text(msg)

            return _public
        return decorator

    @classmethod
    def owner(cls, hidden: bool = False):
        """
        Decorator that executes the method only if the user is a bot admin.

        The user ID that triggered the command has to be in 'admin_tg_id'
        of the global config file 'global.json' or in the ["admins"] list
        of the currently used plugin config file.
        """

        def decorator(func):
            @wraps(func)
            async def _owner(self, update: Update, context: CallbackContext, **kwargs):
                user_id = update.effective_user.id

                plg_admins = self.cfg.get("admins")
                plg_admins = plg_admins if isinstance(plg_admins, list) else []

                global_admin = self.cfg_global.get("admin_tg_id")

                if user_id in plg_admins or user_id == global_admin:
                    if asyncio.iscoroutinefunction(func):
                        return await func(self, update, context, **kwargs)
                    else:
                        return func(self, update, context, **kwargs)

                if (not hidden) and update.message:
                    msg = f"{c.ERROR} Can only be used by the owner"
                    await update.message.reply_text(msg)

            return _owner
        return decorator

    @classmethod
    def dependency(cls):
        """ Decorator that executes a method only if the mentioned
        plugins in the config file of the current plugin are enabled """

        def decorator(func):
            @wraps(func)
            async def _dependency(self, update: Update, context: CallbackContext, **kwargs):
                dependencies = self.cfg.get("dependency")
                dependencies = dependencies if isinstance(dependencies, list) else []

                for dependency in dependencies:
                    if dependency.lower() not in self.plugins:
                        msg = f"{c.ERROR} Plugin '{self.name}' is missing dependency '{dependency}'"
                        await update.message.reply_text(msg)
                        return

                if asyncio.iscoroutinefunction(func):
                    return await func(self, update, context, **kwargs)
                else:
                    return func(self, update, context, **kwargs)

            return _dependency
        return decorator

    @classmethod
    def send_typing(cls):
        """ Decorator for sending typing notification in the Telegram chat """

        def decorator(func):
            @wraps(func)
            async def _send_typing(self, update, context, **kwargs):
                # Make sure that edited messages will not trigger any functionality
                if not update.edited_message:
                    try:
                        await context.bot.send_chat_action(
                            chat_id=update.effective_chat.id,
                            action=ChatAction.TYPING)
                    except:
                        pass

                if asyncio.iscoroutinefunction(func):
                    return await func(self, update, context, **kwargs)
                else:
                    return func(self, update, context, **kwargs)

            return _send_typing
        return decorator

    @classmethod
    def blacklist(cls, hidden: bool = False, dm: bool = False):
        """ Decorator to check whether a command can be executed in the given
         chat or not. If the current chat ID is part of the 'blacklist' list
         in the plugins config file then the command will not be executed. """

        def decorator(func):
            @wraps(func)
            async def _blacklist(self, update: Update, context: CallbackContext, **kwargs):
                group_id = update.effective_chat.id
                thread_id = update.effective_message.message_thread_id

                blacklist = self.cfg.get("blacklist")
                blacklist = blacklist if blacklist else []

                try:
                    is_blacklisted = any(
                        entry["group"] == group_id and
                        (not "thread" in entry or entry["thread"] == thread_id)
                        for entry in blacklist
                    )

                    if not is_blacklisted or (dm and (await context.bot.get_chat(group_id)).type == Chat.PRIVATE):
                        if asyncio.iscoroutinefunction(func):
                            return await func(self, update, context, **kwargs)
                        return func(self, update, context, **kwargs)
                    elif not hidden:
                        name = context.bot.username or context.bot.name
                        msg = self.cfg.get("blacklist_msg").replace("{{name}}", name)
                        reply = await update.message.reply_text(msg, disable_web_page_preview=True)
                        await self.remove_msg_after(update.message, reply, after_secs=5)
                except:
                    pass

            return _blacklist
        return decorator

    @classmethod
    def whitelist(cls, hidden: bool = False, dm: bool = True):
        """ Decorator to check whether a command can be executed in the given
         chat or not. If the current chat ID is part of the 'whitelist' list
         in the plugins config file then the command will be executed. """

        def decorator(func):
            @wraps(func)
            async def _whitelist(self, update: Update, context: CallbackContext, **kwargs):
                group_id = update.effective_chat.id
                thread_id = update.effective_message.message_thread_id

                whitelist = self.cfg.get("whitelist")
                whitelist = whitelist if whitelist else []

                try:
                    is_whitelisted = any(
                        entry["group"] == group_id and
                        (not "thread" in entry or entry["thread"] == thread_id)
                        for entry in whitelist
                    )

                    if not is_whitelisted and dm:
                        if (await context.bot.get_chat(group_id)).type == Chat.PRIVATE:
                            is_whitelisted = True

                    if is_whitelisted:
                        if asyncio.iscoroutinefunction(func):
                            return await func(self, update, context, **kwargs)
                        return func(self, update, context, **kwargs)
                    elif not hidden:
                        name = context.bot.username or context.bot.name
                        msg = self.cfg.get("whitelist_msg").replace("{{name}}", name)
                        reply = await update.message.reply_text(msg, disable_web_page_preview=True)
                        await self.remove_msg_after(update.message, reply, after_secs=5)
                except:
                    pass

            return _whitelist
        return decorator

    async def get_wallet(self, user_id, db_name="global.db") -> Wallet:
        """ Return address and private key for given user_id.
        If no wallet exists then it will be created. """

        # Check if user already has a wallet
        sql = await self.get_resource_global("select_wallet.sql")
        res = await self.exec_sql_global(sql, user_id, db_name=db_name)

        # User already has a wallet
        if res["data"]:
            return Wallet(res["data"][0][2])

        # Create new wallet
        wallet = Wallet()

        # Save wallet to database
        await self.exec_sql_global(
            await self.get_resource_global("insert_wallet.sql"),
            user_id,
            wallet.public_key,
            wallet.private_key,
            db_name=db_name)

        self.log.info(f'Address {wallet.public_key} created for user ID {user_id}')
        return wallet

    async def get_xian(self, node: str = None, chain_id: str = None, wallet: Wallet = None) -> XianAsync:
        """ Return a Xian Network node instance """

        if node is None:
            node = self.cfg_global.get('xian', 'node')
        if chain_id is None:
            chain_id = self.cfg_global.get('xian', 'chain_id')

        xian = XianAsync(node, chain_id, wallet)

        if chain_id is None:
            global_node = self.cfg_global.get('xian', 'node')
            if global_node == node:
                self.cfg_global.set(xian.chain_id, 'xian', 'chain_id')

        return xian
