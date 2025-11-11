import asyncio 
import time, datetime 
import logging
from database import db 
from config import Config
from pyrogram import Client, filters 
from pyrogram.errors import InputUserDeactivated, FloodWait, UserIsBlocked

logger = logging.getLogger(__name__)

@Client.on_message(filters.command(["broadcast", "b"]) & filters.user(Config.OWNER_ID) & filters.reply)
async def broadcast (bot, message):
    # Use find() which returns a cursor
    users = db.col.find({}) 
    b_msg = message.reply_to_message
    sts = await message.reply_text(
        text='Broadcasting Your Messages...'
    )
    start_time = time.time()
    
    # Get total users count correctly
    total_users = await db.col.count_documents({})
    
    done = 0
    blocked = 0
    deleted = 0
    failed = 0 
    success = 0
    
    async for user in users:
        pti, sh = await broadcast_messages(int(user['id']), b_msg)
        if pti:
            success += 1
            await asyncio.sleep(0.05) # 50ms sleep
        elif pti == False:
            if sh == "Blocked":
                blocked+=1
            elif sh == "Deleted":
                deleted += 1
            elif sh == "Error":
                failed += 1
        done += 1
        if not done % 20:
            await sts.edit(f"<b><u>Broadcast In Progress :</u></b>\n\nTotal Users {total_users}\nCompleted: {done} / {total_users}\nSuccess: {success}\nBlocked: {blocked}\nDeleted: {deleted}")    
    
    time_taken = datetime.timedelta(seconds=int(time.time()-start_time))
    await sts.edit(f"<b><u>Broadcast Completed :</u></b>\n\nCompleted in {time_taken} seconds.\n\nTotal Users {total_users}\nCompleted: {done} / {total_users}\nSuccess: {success}\nBlocked: {blocked}\nDeleted: {deleted}")

async def broadcast_messages(user_id, message):
    try:
        await message.copy(chat_id=user_id)
        return True, "Success"
    except FloodWait as e:
        logger.warning(f"FloodWait for {e.value}s during broadcast to {user_id}")
        await asyncio.sleep(e.value)
        return await broadcast_messages(user_id, message) # Retry
    except InputUserDeactivated:
        # We don't have db.delete_user, but we can ban them
        await db.ban_user(int(user_id), "Account Deleted")
        logger.info(f"{user_id} - Banned, since deleted account.")
        return False, "Deleted"
    except UserIsBlocked:
        logger.info(f"{user_id} - Blocked the bot.")
        return False, "Blocked"
    except Exception as e:
        logger.error(f"Broadcast failed for {user_id}: {e}")
        return False, "Error"
