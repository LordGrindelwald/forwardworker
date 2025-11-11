import re
import random
import time
import math
import logging
import asyncio
from uuid import uuid4
from database import db
from config import temp
from translation import Translation
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message
from pyrogram.errors import MessageNotModified

SYD = ["https://files.catbox.moe/3lwlbm.png"]
logger = logging.getLogger(__name__)

def get_readable_time(seconds: int) -> str:
    if seconds == 0: return "0s"
    result = ""
    (days, remainder) = divmod(seconds, 86400)
    if days > 0: result += f"{int(days)}d "
    (hours, remainder) = divmod(remainder, 3600)
    if hours > 0: result += f"{int(hours)}h "
    (minutes, seconds) = divmod(remainder, 60)
    if minutes > 0: result += f"{int(minutes)}m "
    if seconds > 0: result += f"{int(seconds)}s"
    return result.strip()

def get_size(size):
    if not size: return ""
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while size >= 1024 and i < len(units) - 1:
        size /= 1024
        i += 1
    return f"{size:.2f} {units[i]}"

async def start_range_selection(bot, message: Message, from_chat_id, from_title, to_chat_id, start_id, end_id):
    session_id = str(uuid4())
    range_msg = await bot.send_message(
        chat_id=message.chat.id, text="`Calculating...`"
    )
    temp.RANGE_SESSIONS[session_id] = {
        'user_id': message.chat.id,
        'from_chat_id': from_chat_id, 'from_title': from_title,
        'to_chat_id': to_chat_id, 'start_id': start_id, 'end_id': end_id,
        'original_message_id': message.id,
        'range_message_id': range_msg.id
    }
    await update_range_message(bot, session_id)

async def update_range_message(bot, session_id):
    session = temp.RANGE_SESSIONS.get(session_id)
    if not session: return

    try:
        message_to_edit = await bot.get_messages(session['user_id'], session['range_message_id'])
    except Exception:
        logger.warning(f"Could not find message to edit for range session {session_id}")
        return

    text = Translation.RANGE_SELECTION_TXT.format(
        start=min(session['start_id'], session['end_id']),
        end=max(session['start_id'], session['end_id'])
    )

    buttons = [
        [InlineKeyboardButton(f"Range: {min(session['start_id'], session['end_id'])} ➔ {max(session['start_id'], session['end_id'])}", callback_data="noop")],
        [InlineKeyboardButton("✎ Edit Start", callback_data=f"range_edit_start_{session_id}"),
         InlineKeyboardButton("✎ Edit End", callback_data=f"range_edit_end_{session_id}")],
        [InlineKeyboardButton("⇄ Swap", callback_data=f"range_swap_{session_id}")],
        [InlineKeyboardButton("✓ Confirm", callback_data=f"range_confirm_{session_id}")],
        [InlineKeyboardButton("« Cancel", callback_data=f"range_cancel_{session_id}")]
    ]

    try:
        await message_to_edit.edit_text(text=text, reply_markup=InlineKeyboardMarkup(buttons))
    except Exception as e:
        logger.error(f"Error in update_range_message: {e}", exc_info=True)

async def edit_progress(message, task_id, done=False):
    """
    Fetches task data from DB and edits the progress message.
    """
    if not message:
        logger.warning(f"edit_progress called with invalid message for task {task_id}")
        return
        
    try:
        while not temp.CANCEL.get(task_id) and not done:
            task_doc = await db.tasks.find_one({'_id': task_id})
            if not task_doc:
                logger.warning(f"Task {task_id} not found in DB for progress update.")
                break
            
            # Check if task was externally completed or cancelled
            if task_doc.get('status') in ['completed', 'cancelled']:
                done = True
                break
            
            text, buttons = progress_message_content(task_doc)
            try:
                await message.edit_text(text, reply_markup=buttons)
            except MessageNotModified:
                pass
            await asyncio.sleep(5)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.warning(f"Progress update failed for task {task_id}: {e}")
    finally:
        task_doc = await db.tasks.find_one({'_id': task_id})
        if task_doc:
            # Determine 'done' status from the final doc status
            final_done = done or task_doc.get('status') in ['completed', 'cancelled', 'failed']
            text, buttons = progress_message_content(task_doc, done=final_done)
            try:
                await message.edit_text(text, reply_markup=buttons)
            except Exception: 
                pass

def progress_message_content(task_doc, done=False):
    """
    Generates progress message content from the task document.
    """
    task_id = task_doc['_id']
    start_time = task_doc['start_time']
    total = task_doc['total_messages']
    fetched = task_doc['fetched']
    forwarded = task_doc['total_files']
    failed = task_doc['failed']
    status = task_doc['status']
    elapsed_time = time.time() - start_time
    if elapsed_time == 0: elapsed_time = 1

    if done or status in ['completed', 'cancelled', 'failed']:
        if status == 'completed':
            status_text = "Completed"
        elif status == 'cancelled':
            status_text = "Cancelled"
        elif status == 'failed':
            status_text = f"Failed"
        else:
            status_text = "Completed" if not temp.CANCEL.get(task_id) else "Cancelled"

        text = (
            f"✅ **Task {status_text}!**\n\n"
            f"**Total Forwarded:** `{forwarded}`\n"
            f"**Total Failed:** `{failed}`\n"
            f"**Time Taken:** `{get_readable_time(int(elapsed_time))}`"
        )
        if status == 'failed':
             text += f"\n**Error:** `{task_doc.get('error', 'Unknown')}`"
             
        buttons = None
    else:
        speed = fetched / elapsed_time
        percentage = (fetched * 100) / total if total > 0 else 0
        percentage = min(100.00, percentage)

        eta = get_readable_time(int(((total - fetched) / speed) if speed > 0 and fetched < total else 0))
        progress_bar = "▰" * math.floor(percentage / 10) + "▱" * (10 - math.floor(percentage / 10))

        status_text = "Running..."

        text = Translation.TEXT.format(
            status=status_text,
            fetched=fetched, total=total,
            forwarded=forwarded,
            skipped=max(0, fetched - forwarded - failed), # Ensure skipped is not negative
            failed=failed,
            progress_bar=progress_bar,
            percentage=f"{percentage:.2f}",
            eta=eta
        )

        buttons = InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 Status", callback_data=f"fwrdstatus_{task_id}")],
            [InlineKeyboardButton("✖️ Cancel Task ✖️", callback_data=f"cancel_task_{task_id}")]
        ])

    return text, buttons

def get_status_alert_text(task_doc):
    """Generates the text for the real-time status pop-up alert."""
    start_time = task_doc['start_time']
    total = task_doc['total_messages']
    fetched = task_doc['fetched']
    forwarded = task_doc['total_files']
    failed = task_doc['failed']
    status = task_doc['status']
    elapsed_time = time.time() - start_time
    if elapsed_time == 0: elapsed_time = 1

    speed = fetched / elapsed_time
    percentage = (fetched * 100) / total if total > 0 else 0
    percentage = min(100.00, percentage)

    eta = get_readable_time(int(((total - fetched) / speed) if speed > 0 and fetched < total else 0))

    return Translation.STATUS_ALERT.format(
        fetched=fetched, total=total,
        percentage=f"{percentage:.2f}",
        forwarded=forwarded, failed=failed,
        skipped=max(0, fetched - forwarded - failed),
        status=status.capitalize(),
        eta=eta
    )