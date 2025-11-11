# iangrind/forwardworker/forwardworker-1ff680b8c32922eb74e103a193e108a8d299c7bc/bot.py

import asyncio
import logging 
import logging.config
from config import Config, temp
from database import db
from aiohttp import web
from plugins import web_server
from pyrogram import Client, __version__, idle
from pyrogram.raw.all import layer 
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait 

# --- ADD THESE IMPORTS ---
from plugins.forward_engine import WorkerManager, resilient_start_clone, robust_access_check, cancel_checker
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

        # Start the web server
        app = web.AppRunner(await web_server())
        await app.setup()
        bind_address = "0.0.0.0"
        await web.TCPSite(app, bind_address, PORT).start()
        
        await idle()

    async def stop(self, *args):
        await super().stop()
        logging.info("Bot has stopped.")

    async def resume_task(self, task_doc):
        """
        The core logic to resume a single task.
        """
        user_id = task_doc['user_id']
        task_id = task_doc['_id']
        self.log.info(f"Resuming task {task_id} for user {user_id}")
        
        # Default message handle. We send a NEW message.
        m = None
        valid_operators = []

        try:
            # Send a new message to the user
            m = await self.send_message(
                user_id,
                f"🔄 **Task Resumed**\n\nBot restarted. Resuming task `{task_id}` from its last known progress. Please wait..."
            )
            
            operator_configs = await db.get_bots(user_id)
            if not operator_configs:
                raise ValueError("No operators found for this user.")
            
            results = await asyncio.gather(*[resilient_start_clone(c) for c in operator_configs])
            all_operator_clients = [client for client, error in results if client]
            
            failed_operator_details = []
            for client in all_operator_clients:
                source_ok, source_err = await robust_access_check(client, task_doc['from_chat_id'])
                target_ok, target_err = await robust_access_check(client, task_doc['to_chat_id'])
                if source_ok and target_ok:
                    valid_operators.append(client)
                else:
                    err_detail = source_err if not source_ok else target_err
                    failed_operator_details.append(f"`{client.me.first_name}` ({err_detail})")
            
            if not valid_operators:
                raise ValueError("No operators could access the required chats.")

            if failed_operator_details:
                await self.send_message(user_id, f"⚠️ **Warning:** The following operators failed access checks and will be skipped for resumed task `{task_id}`:\n- " + "\n- ".join(failed_operator_details))
            
            # Store the new message for updates
            temp.ACTIVE_TASKS.setdefault(user_id, {})[task_id] = {"process": m, "start_time": task_doc['start_time']}
            temp.lock[user_id] = True
            
            # Start the manager and helper tasks
            reporter_task = asyncio.create_task(edit_progress(m, task_id))
            manager = WorkerManager(valid_operators, task_doc, task_doc['configs'])
            cancel_task = asyncio.create_task(cancel_checker(task_id, manager))
            
            await manager.start()
            
        except Exception as e:
            self.log.error(f"Failed to resume task {task_id}: {e}", exc_info=True)
            await db.tasks.update_one({'_id': task_id}, {'$set': {'status': 'failed', 'error': str(e)}})
            if user_id:
                try:
                    await self.send_message(user_id, f"**TASK FAILED**\n\nFailed to resume task `{task_id}`. \n**Reason:** `{e}`")
                except Exception as e2:
                    self.log.error(f"Failed to send resume-fail message to user {user_id}: {e2}")
        finally:
            if 'reporter_task' in locals(): reporter_task.cancel()
            if 'cancel_task' in locals(): cancel_task.cancel()
            
            if m: # Update the new message one last time
                await edit_progress(m, task_id, done=True)
            
            # Stop all clients started for this task
            await asyncio.gather(*[client.stop() for client in valid_operators if client.is_connected], return_exceptions=True)
            
            if user_id in temp.ACTIVE_TASKS:
                temp.ACTIVE_TASKS.pop(user_id, None)
            temp.CANCEL.pop(task_id, None)
            temp.lock.pop(user_id, None)
            self.log.info(f"Cleanup for resumed task {task_id} complete.")

    async def resume_running_tasks(self):
        """
        Finds all 'running' tasks in the DB and creates asyncio tasks to resume them.
        """
        self.log.info("Checking for incomplete tasks to restart...")
        try:
            tasks_to_restart = await db.tasks.find({'status': 'running'}).to_list(None)
            self.log.info(f"Found {len(tasks_to_restart)} tasks to resume.")
            
            for task_doc in tasks_to_restart:
                asyncio.create_task(self.resume_task(task_doc))
                
        except Exception as e:
            self.log.error(f"Failed to query tasks for restart: {e}", exc_info=True)