# iangrind/forwardworker/forwardworker-1ff680b8c32922eb74e103a193e108a8d299c7bc/bot.py

import asyncio
import logging 
import logging.config
import aiohttp  # <-- ADDED IMPORT
from config import Config, temp
from database import db
from aiohttp import web
from plugins import web_server
from pyrogram import Client, __version__, idle
from pyrogram.raw.all import layer 
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait 

# --- ADD THESE IMPORTS ---
from plugins.forward_engine import resilient_start_clone, robust_access_check
from plugins.utils import edit_progress
# -------------------------

logging.config.fileConfig('logging.conf')
logging.getLogger().setLevel(logging.INFO)
logging.getLogger("pyrogram").setLevel(logging.ERROR)

PORT = Config.PORT
# --- UPDATED PING INTERVAL ---
PING_INTERVAL = 300  # 5 minutes (in seconds)

class Bot(Client): 
    def __init__(self):
        super().__init__(
            Config.BOT_SESSION,
            api_hash=Config.API_HASH,
            api_id=Config.API_ID,
            plugins={
                "root": "plugins"
            },
            bot_token=Config.BOT_TOKEN
        )
        self.log = logging

    # --- ADDED THIS ENTIRE METHOD ---
    async def self_ping_task(self):
        """A background task to ping the app's own URL to keep it alive."""
        if not Config.APP_URL:
            self.log.warning("APP_URL not set. Self-ping task will not run.")
            return

        self.log.info(f"Self-ping task started. Pinging {Config.APP_URL} every {PING_INTERVAL}s.")
        # Wait 30s for the web server to be ready on first boot
        await asyncio.sleep(30) 

        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(Config.APP_URL) as response:
                        if response.status == 200:
                            self.log.info(f"Self-ping to {Config.APP_URL} successful (Status: {response.status}).")
                        else:
                            self.log.warning(f"Self-ping to {Config.APP_URL} returned non-200 status: {response.status}")
            except aiohttp.ClientError as e:
                self.log.error(f"Self-ping to {Config.APP_URL} failed: {e}")
            except Exception as e:
                self.log.error(f"An unexpected error occurred in self-ping task: {e}", exc_info=True)

            await asyncio.sleep(PING_INTERVAL)
    # ----------------------------------

    async def start(self):
        try:
            await super().start()
        except FloodWait as e:
            self.log.warning(f"FloodWait on start: waiting for {e.value} seconds.")
            await asyncio.sleep(e.value)
            await super().start()
            
        me = await self.get_me()
        logging.info(f"{me.first_name} with Pyrogram v{__version__} (Layer {layer}) started on @{me.username}.")
        
        temp.BANNED_USERS = await db.get_banned()

        # --- ADD AUTO-RESTART LOGIC HERE ---
        await self.resume_running_tasks()
        # ------------------------------------

        # Start the web server
        app = web.AppRunner(await web_server())
        await app.setup()
        bind_address = "0.0.0.0"
        await web.TCPSite(app, bind_address, PORT).start()
        
        # --- ADDED THIS LINE TO LAUNCH THE TASK ---
        asyncio.create_task(self.self_ping_task())
        # ------------------------------------------
        
        await idle()

    async def stop(self, *args):
        await super().stop()
        logging.info("Bot has stopped.")

    async def resume_running_tasks(self):
        """
        Finds all 'running' or 'paused' sub-tasks in the DB and 
        creates asyncio tasks to resume them.
        """
        self.log.info("Checking for incomplete sub-tasks to restart...")
        try:
            # We must import the worker function here, inside the method
            from plugins.forward_engine import run_partition_worker 
        
            # Find all sub-tasks that are not completed
            sub_tasks_to_restart = await db.sub_tasks.find(
                {'status': {'$ne': 'completed'}}
            ).to_list(None)
            
            self.log.info(f"Found {len(sub_tasks_to_restart)} sub-tasks to resume.")
            
            for sub_task_doc in sub_tasks_to_restart:
                self.log.info(f"Resuming sub-task {sub_task_doc['_id']} for operator {sub_task_doc['operator_config']['id']}")
                asyncio.create_task(run_partition_worker(sub_task_doc))
                
        except Exception as e:
            self.log.error(f"Failed to query sub-tasks for restart: {e}", exc_info=True)
