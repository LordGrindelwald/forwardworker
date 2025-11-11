import asyncio
import logging
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from config import temp
from database import db
from .test import CLIENT, start_clone_bot

logger = logging.getLogger(__name__)

async def run_single_bot_diagnosis(operator_config, source_chat_id, target_chat_id):
    """
    Starts a client for a single operator, tests its permissions, and returns a detailed report string.
    """
    bot_name = operator_config.get('name', 'Unknown Bot')
    bot_id = operator_config.get('id', 'N/A')
    report_lines = [f"**▶️ Bot: `{bot_name}` (ID: `{bot_id}`)**"]
    operator_client = None

    # Start the client
    try:
        operator_client = await start_clone_bot(CLIENT.client(operator_config), operator_config)
    except Exception as e:
        report_lines.append(f"   ❌ **Connection:** FAILED TO START")
        report_lines.append(f"      **Error:** `{type(e).__name__}` - Check token/session string.")
        return "\n".join(report_lines)

    # 1. Test fetching from the source chat
    try:
        # --- THIS IS THE FIX ---
        # Removed the get_chat_history call. get_chat is safe for bots.
        await operator_client.get_chat(source_chat_id)
        # --- END FIX ---
        report_lines.append("   ✅ **Source Access (Fetch):** OK")
    except Exception as e:
        report_lines.append(f"   ❌ **Source Access (Fetch):** FAILED")
        report_lines.append(f"      **Error:** `{e.__class__.__name__}` - {e}")

    # 2. Test posting to the target chat
    try:
        test_message = await operator_client.send_message(target_chat_id, "🔬 `Permission diagnosis in progress...`")
        await asyncio.sleep(1) # Give Telegram a moment to process
        await test_message.delete()
        report_lines.append("   ✅ **Target Access (Post):** OK")
    except Exception as e:
        report_lines.append(f"   ❌ **Target Access (Post):** FAILED")
        report_lines.append(f"      **Error:** `{e.__class__.__name__}` - {e}")

    # Stop the client
    if operator_client and operator_client.is_connected:
        await operator_client.stop()
    return "\n".join(report_lines)

async def start_diagnosis_process(bot, user_id, prompt_message, source_chat_id, target_chat_id):
    """
    Orchestrates the diagnosis for all of a user's bots.
    """
    await prompt_message.edit("`🔬 Diagnosing all operators... This may take a few moments.`")
    
    operator_configs = await db.get_bots(user_id)
    if not operator_configs:
        return await prompt_message.edit("You have no operator bots configured in `/settings`.")

    # Run diagnosis for all bots concurrently
    tasks = [run_single_bot_diagnosis(cfg, source_chat_id, target_chat_id) for cfg in operator_configs]
    results = await asyncio.gather(*tasks)

    # Format the final report
    final_report = "**🔬 Diagnosis Report**\n\n" + "\n\n".join(results)
    final_report += "\n\n**--- Diagnosis Complete ---**"

    await prompt_message.edit(final_report)

@Client.on_message(filters.private & filters.command("diagnose"))
async def diagnose_command(client, message):
    """
    Starts the diagnosis flow.
    """
    user_id = message.from_user.id
    if temp.lock.get(user_id):
        return await message.reply("A task is already in progress. Please wait until it's finished to run a diagnosis.")
    
    prompt = await message.reply_text(
        "**Step 1 of 2: Set Source Chat**\n\n"
        "Please forward any message from the **source chat** you want to test."
    )
    temp.USER_STATES[user_id] = {'state': 'diag_awaiting_source', 'prompt_message_id': prompt.id}

@Client.on_message(filters.private & filters.forwarded)
async def handle_diag_source_message(bot: Client, message: Message):
    """
    Handles the forwarded message to set the source chat.
    """
    user_id = message.from_user.id
    state = temp.USER_STATES.get(user_id)

    # Check if we are actually waiting for this user's input for diagnosis
    if not state or state.get("state") != 'diag_awaiting_source':
        return

    source_chat_id = message.forward_from_chat.id
    state['source_chat_id'] = source_chat_id
    
    # Clean up the previous prompt message
    try:
        await bot.delete_messages(user_id, state['prompt_message_id'])
    except Exception:
        pass

    # Ask for the target channel from the user's saved list
    channels = await db.get_user_channels(user_id)
    if not channels:
        temp.USER_STATES.pop(user_id, None)
        return await message.reply("No target channels found in `/settings`. Please add one first to run a diagnosis.")

    buttons = [[InlineKeyboardButton(c['title'], callback_data=f"diag_target_{c['chat_id']}")] for c in channels]
    buttons.append([InlineKeyboardButton("« Cancel", callback_data="close_btn")])
    
    prompt = await message.reply(
        "**Step 2 of 2: Select Target Channel**\n\n"
        "Now, please select the **target chat** you want to test posting to.",
        reply_markup=InlineKeyboardMarkup(buttons)
    )
    state.update({'state': 'diag_awaiting_target', 'prompt_message_id': prompt.id})

@Client.on_callback_query(filters.regex(r'^diag_target_'))
async def cb_select_diag_target(bot, query: CallbackQuery):
    """
    Handles the target channel button selection and starts the main diagnosis.
    """
    user_id = query.from_user.id
    state = temp.USER_STATES.get(user_id)
    if not state or state.get("state") != 'diag_awaiting_target':
        return await query.answer("This diagnosis session has expired. Please start over with /diagnose.", show_alert=True)

    target_chat_id = int(query.data.split('_')[-1])
    source_chat_id = state['source_chat_id']
    prompt_message = await bot.get_messages(user_id, state['prompt_message_id'])

    temp.USER_STATES.pop(user_id, None) # Clear the user's state
    await query.answer("Starting diagnosis...")

    # Trigger the main diagnosis process
    await start_diagnosis_process(bot, user_id, prompt_message, source_chat_id, target_chat_id)