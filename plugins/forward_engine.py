import os
import sys
import asyncio
import random
import logging
import re
import string
import time
from uuid import uuid4
from collections import deque
from database import db
from config import Config, temp
from translation import Translation
from .utils import (start_range_selection, update_range_message,
                    edit_progress, get_size, progress_message_content, get_status_alert_text)
from .parser import parse_buttons
from .test import CLIENT, start_clone_bot
from pyrogram import Client, filters, enums
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message, CallbackQuery
from pyrogram.errors import FloodWait, MessageNotModified

SYD = ["https://files.catbox.moe/3lwlbm.png"]
logger = logging.getLogger(__name__)
BATCH_SIZE = 100 # This is now the "claim size"
OPERATOR_START_TIMEOUT = 30

def generate_short_id(length=8):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

def should_skip(message, configs):
    f_config = configs.get('filters', {})
    if not message: return True
    if message.empty or message.service: return True

    if message.text and not message.media and not f_config.get('text', True): return True
    if message.photo and not f_config.get('photo', True): return True
    if message.video and not f_config.get('video', True): return True
    if message.audio and not f_config.get('audio', True): return True
    if message.voice and not f_config.get('voice', True): return True
    if message.document and not f_config.get('document', True): return True
    if message.sticker and not f_config.get('sticker', True): return True
    if message.animation and not f_config.get('animation', True): return True
    if message.poll and not f_config.get('poll', True): return True
    return False

def get_custom_caption(msg, caption_template):
    if not caption_template or not msg:
        return msg.caption.html if msg and msg.caption else ""

    original_caption = msg.caption.html if msg and msg.caption else ""

    if msg.media:
        media = getattr(msg, msg.media.value, None)
        if media:
            file_name = getattr(media, 'file_name', '')
            file_size = getattr(media, 'file_size', 0)
            return caption_template.format(filename=file_name, size=get_size(file_size), caption=original_caption)

    return caption_template.format(filename="", size="", caption=original_caption)

class WorkerManager:
    def __init__(self, operator_clients, task_doc, configs):
        self.clients = deque(operator_clients)
        self.task_id = task_doc['_id']
        self.task_data = task_doc  # This is our initial cache
        self.configs = configs
        self.delay = configs.get('forward_delay', 0.5)
        self.cooldown_workers = {}
        self.is_cancelled = False
        self.task_is_done = False # Flag to stop new work
        # This lock ensures only one worker is claiming from the DB at a time
        self.db_lock = asyncio.Lock() 

    async def claim_next_batch(self):
        """
        Atomically claims the next batch from the database.
        This is the *only* function that touches the 'fetched' count for claiming.
        Returns (message_ids_batch, is_done)
        """
        async with self.db_lock:
            if self.task_is_done:
                return None, True

            # Get the *current* state from the DB
            task_doc = await db.tasks.find_one(
                {'_id': self.task_id, 'status': 'running'}
            )

            if not task_doc:
                self.task_is_done = True
                return None, True

            current_fetched = task_doc['fetched']
            total_messages = task_doc['total_messages']
            start_id = task_doc['start_id']
            end_id = task_doc['end_id']

            if current_fetched >= total_messages:
                self.task_is_done = True
                return None, True

            # Calculate the next batch
            messages_in_batch = min(BATCH_SIZE, total_messages - current_fetched)
            new_fetched_count = current_fetched + messages_in_batch
            
            # Atomically update the fetched count
            await db.tasks.update_one(
                {'_id': self.task_id},
                {'$set': {'fetched': new_fetched_count}}
            )

            # Calculate the message IDs for this claimed batch
            batch_start_msg_id = start_id + current_fetched
            batch_end_msg_id = start_id + new_fetched_count - 1
            
            message_ids_batch = list(range(batch_start_msg_id, batch_end_msg_id + 1))
            
            logger.info(f"Task {self.task_id}: Claimed batch {batch_start_msg_id}-{batch_end_msg_id}")
            return message_ids_batch, False

    async def start(self):
        """
        Starts worker tasks that pull from the atomic claim function.
        """
        logger.info(f"WorkerManager started for task {self.task_id} with {len(self.clients)} workers.")
        
        # Create a worker task for each available client
        worker_tasks = []
        for client in self.clients:
            worker_tasks.append(
                asyncio.create_task(self.worker_loop(client))
            )
        
        # Wait for all worker tasks to complete
        await asyncio.gather(*worker_tasks)

        logger.info(f"WorkerManager for task {self.task_id} stopping.")
        
        if self.is_cancelled:
            await db.tasks.update_one({'_id': self.task_id}, {'$set': {'status': 'cancelled'}})
        else:
             # Final check
             final_doc = await db.tasks.find_one({'_id': self.task_id})
             if final_doc and final_doc['fetched'] >= final_doc['total_messages']:
                await db.tasks.update_one({'_id': self.task_id}, {'$set': {'status': 'completed'}})

    async def worker_loop(self, client):
        """
        A single worker's processing loop.
        It will continuously claim and process batches until the task is done.
        """
        while not self.is_cancelled and not self.task_is_done:
            # Check if this worker is on cooldown
            if client in self.cooldown_workers:
                if time.time() < self.cooldown_workers[client]:
                    await asyncio.sleep(1) # Wait if on cooldown
                    continue
                else:
                    logger.info(f"Worker {client.me.first_name} cooldown finished.")
                    del self.cooldown_workers[client] # Cooldown over

            # Claim the next available batch
            message_ids_batch, is_done = await self.claim_next_batch()

            if is_done:
                break # No more work

            if message_ids_batch:
                try:
                    worker_ok = await self.process_batch(client, message_ids_batch)
                    if not worker_ok: # Hit FloodWait
                        self.cooldown_workers[client] = time.time() + 10 # 10s default, process_batch sets longer
                except Exception as e:
                    logger.error(f"Worker {client.me.first_name} had unexpected failure: {e}. Re-queuing batch (duplicates possible).", exc_info=True)
                    # This is the trade-off: to prevent skips, we must risk duplicates.
                    # We must "un-claim" the batch by decrementing the fetched count.
                    await db.tasks.update_one({'_id': self.task_id}, {'$inc': {'fetched': -len(message_ids_batch)}})
                    self.cooldown_workers[client] = time.time() + 10 # 10s cooldown
            else:
                # No batch, wait a moment
                await asyncio.sleep(0.5)

    async def process_batch(self, client, message_ids):
        """
        Processes a batch. Returns True if worker is healthy, False if FloodWaited.
        This function NO LONGER updates 'fetched', but DOES update 'total_files' and 'failed'.
        """
        try:
            messages = await client.get_messages(self.task_data['from_chat_id'], message_ids)
        except Exception as e:
            logger.error(f"Failed to get_messages for batch {message_ids[0]}-{message_ids[-1]}. Error: {e}. Re-queuing.")
            # We "un-claim" this batch so another worker can try it.
            await db.tasks.update_one(
                {'_id': self.task_id},
                {'$inc': {'fetched': -len(message_ids)}}
            )
            return False # Worker is not healthy

        batch_forwarded = 0
        batch_failed = 0
        re_queue_remaining = False
        
        for i, message in enumerate(messages):
            if self.is_cancelled:
                # If cancelled, we must un-claim the remaining messages
                remaining_count = len(messages[i:])
                if remaining_count > 0:
                    await db.tasks.update_one({'_id': self.task_id}, {'$inc': {'fetched': -remaining_count}})
                re_queue_remaining = True
                break

            if not message:
                batch_failed += 1
                continue

            if should_skip(message, self.configs):
                continue
                
            try:
                if self.configs.get('forward_tag', False):
                    await client.forward_messages(chat_id=self.task_data['to_chat_id'], from_chat_id=self.task_data['from_chat_id'], message_ids=[message.id])
                else:
                    await client.copy_message(
                        chat_id=self.task_data['to_chat_id'], from_chat_id=self.task_data['from_chat_id'], message_id=message.id,
                        caption=get_custom_caption(message, self.configs.get('caption')),
                        reply_markup=parse_buttons(self.configs.get('button'))
                    )
                batch_forwarded += 1
                if self.delay > 0: await asyncio.sleep(self.delay)
            except FloodWait as e:
                logger.warning(f"Worker {client.me.first_name} hit FloodWait. Re-queuing remainder.")
                cooldown_duration = e.value + 5
                self.cooldown_workers[client] = time.time() + cooldown_duration
                
                # Un-claim remaining messages
                remaining_count = len(messages[i:])
                if remaining_count > 0:
                    await db.tasks.update_one({'_id': self.task_id}, {'$inc': {'fetched': -remaining_count}})
                re_queue_remaining = True
                break # Stop processing this batch
            except Exception as e:
                logger.error(f"Failed to process message {message.id}. Error: {e}")
                batch_failed += 1

        # Batch completed (or interrupted), update DB
        if batch_forwarded > 0 or batch_failed > 0:
            await db.tasks.update_one(
                {'_id': self.task_id},
                {'$inc': {'total_files': batch_forwarded, 'failed': batch_failed}}
            )
            
        return not re_queue_remaining # Return True if healthy, False if FloodWait/Cancelled

    def cancel(self):
        self.is_cancelled = True

async def resilient_start_clone(config):
    try:
        client = await asyncio.wait_for(start_clone_bot(CLIENT.client(config), config), timeout=OPERATOR_START_TIMEOUT)
        return client, None
    except asyncio.TimeoutError:
        return None, f"Timed out after {OPERATOR_START_TIMEOUT}s"
    except Exception as e:
        return None, str(e)

async def robust_access_check(client, chat_id):
    """
    A bot-safe check to ensure a client can access a chat.
    """
    try:
        # Just checking get_chat is safe for bots and userbots.
        # This works with INT IDs (-100123) and STRING usernames ("publicchannel")
        chat = await client.get_chat(chat_id)
        return True, None
    except Exception as e:
        logger.error(f"Robust access check failed for chat {chat_id}: {e}", exc_info=True)
        return False, type(e).__name__


@Client.on_callback_query(filters.regex(r'^start_public_'))
async def pub_(bot, cb: CallbackQuery):
    user_id = cb.from_user.id
    if temp.lock.get(user_id):
        return await cb.answer("Another task is already in progress.", show_alert=True)
    frwd_id = cb.data.split("_")[2] # This is the temp session_id
    session = temp.FORWARD_SESSIONS.get(frwd_id)
    if not session:
        return await cb.message.edit("This task has expired or is invalid.")
    await cb.answer()
    m = await cb.message.edit("`Initializing...`")
    
    all_operator_clients = []
    task_id = generate_short_id() # This is our new persistent task_id
    valid_operators = [] # Define here for finally block
    
    try:
        user_configs = await db.get_configs(user_id)
        operator_configs = await db.get_bots(user_id)
        if not operator_configs: raise ValueError("No Operator Bots/Userbots found in your settings.")
        
        await m.edit(f"`Step 1/4: Starting {len(operator_configs)} operator(s)...`")
        results = await asyncio.gather(*[resilient_start_clone(c) for c in operator_configs])
        
        all_operator_clients = [client for client, error in results if client]
        
        await m.edit(f"`Step 2/4: Warming up sessions and verifying access...`")
        
        failed_operator_details = []

        for client in all_operator_clients:
            # Use the bot-safe robust_access_check
            source_ok, source_err = await robust_access_check(client, session['from_chat_id'])
            target_ok, target_err = await robust_access_check(client, session['to_chat_id'])

            if source_ok and target_ok:
                valid_operators.append(client)
            else:
                err_detail = source_err if not source_ok else target_err
                logger.warning(f"Operator {client.me.first_name} failed access check: {err_detail}")
                failed_operator_details.append(f"`{client.me.first_name}` ({err_detail})")
        
        if failed_operator_details:
            details_str = "\n- ".join(failed_operator_details)
            await bot.send_message(user_id, f"⚠️ **Warning:** The following operators could not access the required chats and will be skipped:\n- {details_str}")

        if not valid_operators:
            raise ValueError("No operators could access both the source and target chats. Please check their permissions and memberships.")

        await m.edit("`Step 3/4: Creating persistent task in database...`")
        
        start_id, end_id = min(session['start_id'], session['end_id']), max(session['start_id'], session['end_id'])
        
        task_doc = {
            '_id': task_id,
            'user_id': user_id,
            'from_chat_id': session['from_chat_id'],
            'to_chat_id': session['to_chat_id'],
            'start_id': start_id,
            'end_id': end_id,
            'total_messages': (end_id - start_id) + 1,
            'fetched': 0, # This is now the main counter
            'total_files': 0,
            'failed': 0,
            'status': 'running', # 'running', 'paused', 'completed', 'cancelled', 'failed'
            'start_time': time.time(),
            'configs': user_configs,
            'error': None
        }
        
        try:
            await db.tasks.insert_one(task_doc)
        except Exception as e:
            logger.error(f"Failed to create task in DB: {e}", exc_info=True)
            raise ValueError(f"Could not create task in database: {e}")

        temp.ACTIVE_TASKS.setdefault(user_id, {})[task_id] = {"process": m, "start_time": task_doc['start_time']}
        temp.lock[user_id] = True
        
        await m.edit(f"`Step 4/4: Deploying {len(valid_operators)} valid worker(s)...`")
        
        text, buttons = progress_message_content(task_doc)
        await m.edit(text, reply_markup=buttons)

        reporter_task = asyncio.create_task(edit_progress(m, task_id))
        
        manager = WorkerManager(valid_operators, task_doc, user_configs)
        
        cancel_task = asyncio.create_task(cancel_checker(task_id, manager))
        await manager.start()

    except Exception as e:
        logger.error(f"Task {task_id} failed: {e}", exc_info=True)
        await m.edit(f"**TASK FAILED**\n\n**Reason:** `{e}`")
        await db.tasks.update_one({'_id': task_id}, {'$set': {'status': 'failed', 'error': str(e)}})
    finally:
        if 'reporter_task' in locals(): reporter_task.cancel()
        if 'cancel_task' in locals(): cancel_task.cancel()
        
        await edit_progress(m, task_id, done=True)
        
        logger.info(f"Cleaning up resources for task {task_id}...")
        
        clients_to_stop = valid_operators
        
        await asyncio.gather(*[client.stop() for client in clients_to_stop if client.is_connected], return_exceptions=True)
        
        temp.FORWARD_SESSIONS.pop(frwd_id, None)
        if user_id in temp.ACTIVE_TASKS:
            temp.ACTIVE_TASKS.pop(user_id, None)
        temp.CANCEL.pop(task_id, None)
        temp.lock.pop(user_id, None)
        logger.info(f"Cleanup complete for task {task_id}.")

async def cancel_checker(task_id, manager):
    while not manager.is_cancelled:
        if temp.CANCEL.get(task_id):
            manager.cancel()
            break
        await asyncio.sleep(1)

@Client.on_callback_query(filters.regex(r'^fwrdstatus_'))
async def status_popup_cb(bot, cb):
    task_id = cb.data.split("_")[-1]
    
    task_doc = await db.tasks.find_one({'_id': task_id})
    if not task_doc:
        return await cb.answer("This task has expired or is invalid.", show_alert=True)

    alert_text = get_status_alert_text(task_doc)
    await cb.answer(alert_text, show_alert=True)

@Client.on_callback_query(filters.regex(r'^cancel_task_'))
async def cancel_task_cb(bot, cb):
    task_id = cb.data.split("_")[-1]
    temp.CANCEL[task_id] = True
    await cb.answer("Cancelling task... Please wait.", show_alert=True)
    try: 
        await cb.message.edit_reply_markup(None)
        # The manager will update the DB status
    except MessageNotModified: pass
    except Exception as e:
        logger.warning(f"Error during task cancel CB: {e}")


@Client.on_message(filters.private & filters.command(['start']))
async def start(client, message):
    user = message.from_user
    try:
        if not await db.is_user_exist(user.id):
            await db.add_user(user.id, user.first_name)
    except Exception as e:
        logger.error(f"Error in user registration: {e}", exc_info=True)
    await message.reply_photo(
        photo=random.choice(SYD),
        caption=Translation.START_TXT.format(user.mention),
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton('Help', callback_data='help'),
            InlineKeyboardButton('About', callback_data='about')
        ]])
    )

@Client.on_message(filters.private & filters.command(['restart', "r"]) & filters.user(Config.OWNER_ID))
async def restart(client, message):
    msg = await message.reply_text("<i>Restarting...</i>")
    await asyncio.sleep(2)
    os.execl(sys.executable, sys.executable, *sys.argv)

# --- THIS IS THE CORRECTED, ROBUST FUNCTION ---
# Inspired by mistaldrin/fwd/fwd-DawnUltra/plugins/public.py
def parse_message_input(message):
    """Parses a forwarded message or a message link."""
    if not message or (not message.text and not message.forward_date):
        return None, None, "Invalid input. A message link or forwarded message is required."

    if message.text and not message.forward_date:
        # Regex from mistraldrin repo, adapted for lordgrindelwald capture groups
        regex = re.compile(r"(https://)?t\.me/(c/)?(\d+|[a-zA-Z_0-9]+)/(\d+)")
        match = regex.match(message.text.replace("?single", ""))
        if not match: 
            return None, None, 'Invalid Link.'
        
        chat_id_str, msg_id = match.group(3), int(match.group(4))
        
        # --- THIS IS THE FIX ---
        # Correctly casts private channel IDs to INT
        chat_id = int(("-100" + chat_id_str)) if chat_id_str.isnumeric() else chat_id_str
        # --- END FIX ---
        
        return chat_id, msg_id, None
    elif message.forward_from_chat:
        # More robust check from mistraldrin repo
        msg_id, chat_id = message.forward_from_message_id, message.forward_from_chat.username or message.forward_from_chat.id
        return chat_id, msg_id, None
    else:
        return None, None, "Invalid input. Please forward from a channel or provide a valid message link."
# --- END CORRECTED FUNCTION ---

@Client.on_message(filters.private & filters.command(["fwd", "forward"]))
async def forward_command_handler(bot, message):
    user_id = message.from_user.id
    if temp.lock.get(user_id): return await message.reply("A task is in progress. Use /tasks to manage it.")
    if not await db.get_bots(user_id): return await message.reply("No bots found. Add one in `/settings`.")
    if not await db.get_user_channels(user_id): return await message.reply("No target channels found. Add one in `/settings`.")
    temp.USER_STATES[user_id] = {'command_message': message, 'is_settings': False, 'state': 'awaiting_target'}
    buttons = [[InlineKeyboardButton(c['title'], callback_data=f"fwd_target_{c['chat_id']}")] for c in await db.get_user_channels(user_id)]
    buttons.append([InlineKeyboardButton("« Cancel", callback_data="close_btn")])
    await message.reply("<b>Step 1: Select Target Channel</b>", reply_markup=InlineKeyboardMarkup(buttons))

@Client.on_callback_query(filters.regex(r'^fwd_target_'))
async def cb_select_target(bot, query):
    user_id = query.from_user.id
    state = temp.USER_STATES.get(user_id)
    if not state or state.get("state") != 'awaiting_target': return await query.answer("Session expired or invalid state.", show_alert=True)
    state['to_chat_id'] = int(query.data.split('_')[-1])
    prompt = await query.message.edit_text(Translation.FROM_MSG)
    state.update({'prompt_message_id': prompt.id, 'state': 'awaiting_source'})

@Client.on_callback_query(filters.regex(r"^(range_|noop)"))
async def range_menu_handler(bot: Client, query: CallbackQuery):
    user_id = query.from_user.id
    if query.data == "noop": return await query.answer()
    try:
        parts = query.data.split('_')
        action, session_id = parts[1], parts[-1]
        session = temp.RANGE_SESSIONS.get(session_id)
        if not session or session.get('user_id') != user_id: return await query.answer("Session expired.", show_alert=True)
        message_to_edit = await bot.get_messages(user_id, session['range_message_id'])
        if action == "confirm":
            await message_to_edit.delete()
            await show_final_confirmation(bot, query, session_id)
        elif action == "cancel":
            temp.RANGE_SESSIONS.pop(session_id, None)
            await message_to_edit.delete()
            await bot.send_message(user_id, "Cancelled.")
        elif action == "edit":
            part = "start" if parts[2] == "start" else "end"
            prompt = await message_to_edit.edit_text(f"Send the new **{part}** message ID.")
            temp.USER_STATES[user_id] = {"state": "awaiting_range_edit", "session_id": session_id, "part_to_edit": part, "prompt_message_id": prompt.id}
        elif action == "swap":
            session['start_id'], session['end_id'] = session['end_id'], session['start_id']
            await update_range_message(bot, session_id)
            await query.answer("Swapped.")
    except Exception as e:
        logger.error(f"Range menu error: {e}")
        await query.answer(f"Error: {e}", show_alert=True)

async def show_final_confirmation(bot, query, session_id):
    user_id = query.from_user.id
    session = temp.RANGE_SESSIONS.get(session_id)
    if not session: return await bot.send_message(user_id, "Session expired.")
    operators = await db.get_bots(user_id)
    to_title = (await db.get_channel_details(user_id, session['to_chat_id']))['title']
    forward_id = generate_short_id()
    temp.FORWARD_SESSIONS[forward_id] = temp.RANGE_SESSIONS.pop(session_id)
    await bot.send_message(user_id, f"<b>Final Check</b>\n\n"
        f"● <b>Source:</b> <code>{session['from_title']}</code>\n"
        f"● <b>Target:</b> <code>{to_title}</code>\n"
        f"● <b>Range:</b> `{min(session['start_id'], session['end_id'])}` to `{max(session['start_id'], session['end_id'])}`\n"
        f"● <b>Operators:</b> `{len(operators)}` will be used.\n\n"
        f"<i>Ensure operators are in both channels!</i>",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton('✓ Start Forwarding', callback_data=f"start_public_{forward_id}")],
            [InlineKeyboardButton('« Cancel', callback_data="close_btn")]
        ]))

@Client.on_message(filters.private & filters.incoming & ~filters.command([
    "start", "restart", "r", "fwd", "forward", "settings", "forwardelay", "fd", "tasks", "diagnose",
    "ping", "p", "stats", "status", "s", "donate", "d", "addsudo", "rmsudo", "ban", "unban", "broadcast", "b"
]))
async def universal_message_handler(bot: Client, message: Message):
    user_id = message.from_user.id
    state = temp.USER_STATES.get(user_id)

    if not state: 
        return 

    if message.text and message.text.lower() == "/cancel":
        prompt_id = state.get("prompt_message_id")
        if prompt_id:
            try:
                await bot.delete_messages(user_id, prompt_id)
                await message.delete()
            except: pass
        temp.USER_STATES.pop(user_id, None)
        await bot.send_message(user_id, "Cancelled.")
        return
    
    prompt_id = state.get("prompt_message_id")
    if prompt_id:
        try: await bot.delete_messages(user_id, prompt_id)
        except: pass

    state_type = state.get("state")

    if state_type == 'awaiting_source':
        # Use the new, corrected parse_message_input function
        from_chat, end_id, error = parse_message_input(message)
        
        if error:
            prompt = await message.reply(f"**Error:** {error}\n\n{Translation.FROM_MSG}")
            state['prompt_message_id'] = prompt.id
            return 
        
        temp.USER_STATES.pop(user_id, None)
        await message.delete()

        to_chat_id = state['to_chat_id']
        bots = await db.get_bots(user_id)
        if not bots:
            return await bot.send_message(user_id, "Error: No bots found. Cannot get chat title.")
            
        from_title = "Private Chat"
        try:
            async with CLIENT.client(bots[0]) as temp_client:
                await temp_client.start()
                from_title = (await temp_client.get_chat(from_chat)).title
        except Exception as e:
            logger.warning(f"Could not get chat title for {from_chat} using first operator: {e}")
            try:
                 from_title = (await bot.get_chat(from_chat)).title
            except Exception as e2:
                 logger.error(f"Could not get chat title for {from_chat} using main bot: {e2}")

        await start_range_selection(bot, state['command_message'], from_chat, from_title, to_chat_id, 1, end_id)
        return

    elif state_type == 'awaiting_range_edit':
        temp.USER_STATES.pop(user_id, None)
        session_id = state["session_id"]
        session = temp.RANGE_SESSIONS.get(session_id)
        if not session: return await message.reply_text("Session expired.")
        try:
            new_id = int(message.text)
            session[f'{state["part_to_edit"]}_id'] = new_id
            await message.delete()
            await update_range_message(bot, session_id)
        except ValueError: await message.reply_text("Not a valid ID.")
        return
        
    elif state_type == 'diag_awaiting_source':
        if not message.forward_from_chat:
            await bot.send_message(user_id, "Invalid input. Please forward a message from the source chat.")
            prompt = await bot.send_message(user_id, "Please forward a message from the source chat.")
            state['prompt_message_id'] = prompt.id
            return 
        
        return

    if not state.get("is_settings"):
        return
    
    temp.USER_STATES.pop(user_id, None)
    sent_message = await message.reply_text("`Processing...`")

    try:
        if state_type == "awaiting_bot_token":
            if await CLIENT.add_bot(message): await list_bots(sent_message, user_id, as_new=True)
        elif state_type == "awaiting_user_session":
            if await CLIENT.add_session(message): await list_bots(sent_message, user_id, as_new=True)
        elif state_type == "awaiting_bots_bulk":
            if await CLIENT.add_bots_bulk(message): await list_bots(sent_message, user_id, as_new=True)
        elif state_type == "awaiting_users_bulk":
            if await CLIENT.add_sessions_bulk(message): await list_bots(sent_message, user_id, as_new=True)
        elif state_type == "awaiting_channel_forward":
            if message.forward_from_chat:
                await db.add_channel(user_id, message.forward_from_chat.id, message.forward_from_chat.title, message.forward_from_chat.username)
                await message.reply("✅ Channel added.")
                await list_channels(sent_message, user_id, as_new=True)
            else:
                await message.reply("Not a valid forwarded message.")
        elif state_type == "awaiting_caption":
            await db.update_configs(user_id, 'caption', message.text)
            await message.reply("Caption updated successfully.")
        elif state_type == "awaiting_button":
            if parse_buttons(message.text):
                await db.update_configs(user_id, 'button', message.text)
                await message.reply("Button layout updated successfully.")
            else:
                await message.reply("Invalid button format.")
        
        await sent_message.delete()
        
    except Exception as e:
        logger.error(f"Error in settings universal handler ({state_type}): {e}", exc_info=True)
        await sent_message.edit(f"An error occurred: {e}")


@Client.on_callback_query(filters.regex(r'^close_btn$'))
async def close_callback(bot, query):
    await query.message.delete()
    temp.USER_STATES.pop(query.from_user.id, None)

@Client.on_callback_query(filters.regex(r'^back'))
async def back_to_start(bot, query):
    await query.message.edit_caption(
       caption=Translation.START_TXT.format(query.from_user.first_name),
       reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('Help', callback_data='help'), InlineKeyboardButton('About', callback_data='about')]])
    )

@Client.on_callback_query(filters.regex(r'^help'))
async def helpcb(bot, query):
    await query.message.edit_caption(
        caption=Translation.HELP_TXT,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('Settings', callback_data='settings#main'), InlineKeyboardButton('« Back', callback_data='back')]])
    )

@Client.on_callback_query(filters.regex(r'^about'))
async def about(bot, query):
    await query.message.edit_caption(caption=Translation.ABOUT_TXT.format(bot.me.mention), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('« Back', callback_data='back')]]))