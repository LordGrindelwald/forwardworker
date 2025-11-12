# iangrind/forwardworker/forwardworker-1ff680b8c32922eb74e103a193e108a8d299c7bc/bot.py

import asyncio
import logging 
import logging.config
from config import Config, temp
from database import db
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
