import os

class Config:
    API_ID = os.environ.get("API_ID", "")
    API_HASH = os.environ.get("API_HASH", "")
    BOT_TOKEN = os.environ.get("BOT_TOKEN", "") 
    BOT_SESSION = os.environ.get("BOT_SESSION", "forward-bot")  
    DB_URL = os.environ.get("DB_URL", "")
    PORT = os.environ.get("PORT", "8080")
    DB_NAME = os.environ.get("DB_NAME", "cluster0")
    OWNER_ID = [int(id) for id in os.environ.get("OWNER_ID", '').split()]
    # --- ADDED THIS LINE ---
    APP_URL = os.environ.get("APP_URL")


class temp(object): 
    lock = {}
    CANCEL = {}
    forwardings = 0
    BANNED_USERS = []
    IS_FRWD_CHAT = []
    RANGE_SESSIONS = {}
    FORWARD_SESSIONS = {}
    USER_STATES = {}
    ACTIVE_TASKS = {}
    FORWARD_BOT_ID = {}
    UNEQUIFY_USERBOT_ID = {}
    # NEW: A persistent pool for running operator clients
    OPERATOR_CLIENTS = {}
