import time
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup
from config import temp
from database import db
from .utils import get_readable_time, progress_message_content
from translation import Translation


@Client.on_message(filters.private & filters.command(['tasks']))
async def tasks_command(client, message):
    user_id = message.from_user.id
    
    # We check the DB for any running tasks for this user
    active_tasks_docs = await db.tasks.find({
        'user_id': user_id, 
        'status': 'running'
    }).to_list(length=10)
    
    if not active_tasks_docs:
        return await message.reply_text("You have no active tasks.")
        
    for task_doc in active_tasks_docs:
        task_id = task_doc['_id']
        
        # Check if we have a live message to edit
        live_task_info = temp.ACTIVE_TASKS.get(user_id, {}).get(task_id)
        
        text, buttons = progress_message_content(task_doc, done=False)
        
        reply_text = f"**Active Task:** `{task_id}`\n\n{text}"
        
        # If it's in ACTIVE_TASKS, it's live. If not, it's a resumed task
        # that was probably started before a restart.
        if not live_task_info:
            reply_text = f"**(Resumed) {reply_text}"
        
        await message.reply_text(
            reply_text,
            reply_markup=buttons
        )

@Client.on_message(filters.private & filters.command(["forwardelay", "fd"]))
async def forward_delay(client: Client, message: Message):
    """Handler for the /forwardelay command."""
    user_id = message.from_user.id
    
    # Use the helper function to update configs
    from .settings import update_configs, get_configs

    user_configs = await get_configs(user_id)
    delay = user_configs.get('forward_delay', 0.5)

    if len(message.command) < 2:
        return await message.reply_text(
            Translation.FORWARDELAY_TXT.format(current_delay=delay)
        )
    
    try:
        new_delay = float(message.command[1])
        if new_delay < 0:
            return await message.reply_text("Delay must be a positive number (e.g., 0.5, 1, 2).")
        
        await update_configs(user_id, 'forward_delay', new_delay)
        
        await message.reply_text(f"✅ Forward delay updated to **{new_delay}** seconds.")

    except ValueError:
        await message.reply_text("Invalid input. Please provide a number for the delay.")
    except Exception as e:
        await message.reply_text(f"An error occurred: {e}")