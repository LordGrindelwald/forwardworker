import motor.motor_asyncio
from config import Config

db = None

def initialize_database():
    global db
    if db is None:
        db = Database(Config.DB_URL, Config.DB_NAME)

class Database:
    def __init__(self, uri, database_name):
        self._client = motor.motor_asyncio.AsyncIOMotorClient(uri)
        self.db = self._client[database_name]
        self.bot = self.db.bots
        self.col = self.db.user
        self.chl = self.db.channels
        self.tasks = self.db.tasks  # <-- ADDED FOR PERSISTENCE

    async def add_user(self, id, name):
        await self.col.insert_one({'id': id, 'name': name, 'ban_status': {'is_banned': False, 'ban_reason': ""}})

    async def is_user_exist(self, id):
        return bool(await self.col.find_one({'id': int(id)}))

    async def get_ban_status(self, id):
        user = await self.col.find_one({'id': int(id)})
        return user.get('ban_status', {'is_banned': False, 'ban_reason': ''}) if user else {'is_banned': False, 'ban_reason': ''}
        
    async def ban_user(self, user_id, ban_reason="No Reason"):
        ban_status = dict(is_banned=True, ban_reason=ban_reason)
        await self.col.update_one({'id': user_id}, {'$set': {'ban_status': ban_status}})

    async def remove_ban(self, id):
        ban_status = dict(is_banned=False, ban_reason='')
        await self.col.update_one({'id': id}, {'$set': {'ban_status': ban_status}})

    async def get_banned(self):
        users = self.col.find({'ban_status.is_banned': True})
        return [user['id'] async for user in users]

    async def get_configs(self, id):
        # Default configuration for a new user
        default = {
            'caption': None, 
            'duplicate': True, 
            'forward_tag': False, 
            'file_size': 0, 
            'size_limit': None,
            'extension': None, 
            'keywords': None, 
            'protect': None, 
            'button': None, 
            'db_uri': None, 
            'forward_delay': 0.5,
            'filters': {
                'poll': True, 'text': True, 'audio': True, 'voice': True, 
                'video': True, 'photo': True, 'document': True, 
                'animation': True, 'sticker': True
            }
        }
        user = await self.col.find_one({'id': int(id)})
        # If user exists, merge their saved configs with the default
        if user and 'configs' in user:
            user_configs = user['configs']
            # Ensure nested 'filters' dictionary is also merged
            default_filters = default['filters']
            user_filters = user_configs.get('filters', {})
            merged_filters = {**default_filters, **user_filters}
            
            # Merge top-level configs and the merged filters
            final_configs = {**default, **user_configs}
            final_configs['filters'] = merged_filters
            return final_configs
        
        # Return default if user or configs do not exist
        return default
        
    async def update_configs(self, id, configs):
        await self.col.update_one({'id': int(id)}, {'$set': {'configs': configs}}, upsert=True)

    async def add_bot(self, datas):
       await self.bot.insert_one(datas)
       
    async def is_bot_exist(self, user_id, bot_id):
        return bool(await self.bot.find_one({'user_id': user_id, 'id': bot_id}))

    async def remove_bot(self, user_id, bot_id):
       await self.bot.delete_one({'user_id': int(user_id), 'id': int(bot_id)})

    async def get_bot(self, user_id, bot_id):
       return await self.bot.find_one({'user_id': user_id, 'id': bot_id})

    async def get_bots(self, user_id):
        return [b async for b in self.bot.find({'user_id': user_id})]

    async def add_channel(self, user_id, chat_id, title, username):
       if await self.in_channel(user_id, chat_id): return False
       return await self.chl.insert_one({"user_id": user_id, "chat_id": chat_id, "title": title, "username": username})

    async def remove_channel(self, user_id, chat_id):
       if not await self.in_channel(user_id, chat_id): return False
       return await self.chl.delete_many({"user_id": int(user_id), "chat_id": int(chat_id)})
    
    async def in_channel(self, user_id, chat_id):
       return bool(await self.chl.find_one({"user_id": int(user_id), "chat_id": int(chat_id)}))

    async def get_channel_details(self, user_id, chat_id):
       return await self.chl.find_one({"user_id": int(user_id), "chat_id": int(chat_id)})

    async def get_user_channels(self, user_id):
       return [c async for c in self.chl.find({"user_id": int(user_id)})]