import datetime
import logging
import motor.motor_asyncio
from pymongo.errors import ConfigurationError
from urllib.parse import urlparse
from info import (
    DATABASE_NAME, DATABASE_NAME2, DATABASE_URI, DATABASE_URI2, MULTIPLE_DB, MAINTENANCE, PM_SEARCH,
    BUTTON_MODE, P_TTI_SHOW_OFF, PROTECT_CONTENT, IMDB, SPELL_CHECK_REPLY, MELCOW_NEW_USERS, 
    AUTO_DELETE, AUTO_FFILTER, MAX_BTN, IMDB_TEMPLATE, LOG_VR_CHANNEL, TUTORIAL, TUTORIAL_2,
    TUTORIAL_3, SHORTENER_API, SHORTENER_API2, SHORTENER_API3, SHORTENER_WEBSITE, SHORTENER_WEBSITE2,
    SHORTENER_WEBSITE3, IS_VERIFY, TWO_VERIFY_GAP, THREE_VERIFY_GAP, CUSTOM_FILE_CAPTION, AUTH_CHANNELS,
    MOVIE_UPDATE_NOTIFICATION, TZ
)


logger = logging.getLogger(__name__)


class MergedList(list):
    def __aiter__(self):
        async def iterator():
            for item in self:
                yield item
        return iterator()


class Database:
    def __init__(self, uri, database_name, secondary=None):
        self._client = motor.motor_asyncio.AsyncIOMotorClient(uri)
        self.db = self._client[database_name]
        self.secondary = secondary
        self._prem_cache = {}
        # Collections
        self.col = self.db.users
        self.grp = self.db.groups
        self.users = self.db.uersz #Premium users
        self.req = self.db.requests
        self.botcol = self.db.bot_settings
        self.misc = self.db.misc
        self.verify_id = self.db.verify_id 
        self.codes = self.db.codes
        self.filename_col = self.db.filename
        self.movie_updates = self.db.movie_updates
        self.connection = self.db.connections

    async def _find_one(self, collection_name, query, projection=None):
        collection_aliases = {"users": "uersz"}
        primary_collection_name = collection_aliases.get(collection_name, collection_name)
        collection = getattr(self, collection_name, None)
        if collection is None or collection_name == "users":
            collection = self.db[primary_collection_name]
        document = await collection.find_one(query, projection)
        if document:
            return document
        if self.secondary:
            secondary_collection_name = collection_aliases.get(collection_name, collection_name)
            return await self.secondary.db[secondary_collection_name].find_one(query, projection)
        return None

    async def _merged_documents(self, collection_name, query=None):
        query = query or {}
        documents = await self.db[collection_name].find(query).to_list(length=None)
        merged = MergedList(documents)
        seen = {document.get("id", document.get("_id")) for document in documents}
        if self.secondary:
            secondary_documents = await self.secondary.db[collection_name].find(query).to_list(length=None)
            for document in secondary_documents:
                identity = document.get("id", document.get("_id"))
                if identity not in seen:
                    seen.add(identity)
                    merged.append(document)
        return merged

    async def add_name(self, filename):
        if await self.movie_updates.find_one({'_id': filename}):
            return False
        await self.movie_updates.insert_one({'_id': filename})
        return True

    async def delete_all_msg(self):
        await self.movie_updates.delete_many({})
        logger.info("All filenames notification have been deleted.")
        return True
 
     
    async def add_join_req(self, user_id: int, channel_id: int): #update
        await self.req.update_one(
            {'user_id': user_id},
            {
                '$addToSet': {'channels': channel_id},
                '$setOnInsert': {'created_at': datetime.datetime.utcnow()}
            },
            upsert=True
        )
        
    async def has_joined_channel(self, user_id: int, channel_id: int):
        doc = await self.req.find_one({'user_id': user_id})
        return doc and 'channels' in doc and channel_id in doc['channels']

    async def del_join_req(self):
        await self.req.drop()

    def new_user(self, id, name):
        return dict(
            id = id,
            name = name,
            ban_status=dict(
                is_banned=False,
                ban_reason="",
            ),
        )

    def new_group(self, id, title):
        return dict(
            id = id,
            title = title,
            chat_status=dict(
                is_disabled=False,
                reason="",
            ),
        )
    
    async def add_user(self, id, name):
        user = self.new_user(id, name)
        await self.col.insert_one(user)
    
    async def is_user_exist(self, id):
        return bool(await self._find_one("col", {'id': int(id)}))
    
    async def total_users_count(self):
        users = await self._merged_documents("users")
        return len(users)
    
    async def remove_ban(self, id):
        ban_status = dict(
            is_banned=False,
            ban_reason=''
        )
        await self.col.update_one({'id': id}, {'$set': {'ban_status': ban_status}})
    
    async def ban_user(self, user_id, ban_reason="No Reason"):
        ban_status = dict(
            is_banned=True,
            ban_reason=ban_reason
        )
        await self.col.update_one({'id': user_id}, {'$set': {'ban_status': ban_status}})

    async def get_ban_status(self, id):
        default = dict(
            is_banned=False,
            ban_reason=''
        )
        user = await self._find_one("col", {'id': int(id)})
        if not user:
            return default
        return user.get('ban_status', default)

    async def get_all_users(self):
        return await self._merged_documents("users")
    
    async def delete_user(self, user_id):
        await self.col.delete_many({'id': int(user_id)})
        
    async def delete_chat(self, id):
        await self.grp.delete_many({'id': int(id)})    

    async def get_banned(self):
        all_users = await self._merged_documents("users", {'ban_status.is_banned': True})
        all_chats = await self._merged_documents("groups", {'chat_status.is_disabled': True})
        return [user['id'] for user in all_users], [chat['id'] for chat in all_chats]
    
    async def add_chat(self, chat, title):
        chat = self.new_group(chat, title)
        await self.grp.insert_one(chat)
    
    async def get_chat(self, chat):
        chat = await self._find_one("grp", {'id': int(chat)})
        return False if not chat else chat.get('chat_status')
    
    async def re_enable_chat(self, id):
        chat_status=dict(
            is_disabled=False,
            reason="",
            )
        await self.grp.update_one({'id': int(id)}, {'$set': {'chat_status': chat_status}})
        
    async def update_settings(self, id, settings):
        await self.grp.update_one({'id': int(id)}, {'$set': {'settings': settings}}, upsert=True)
                                  
    async def get_settings(self, id):
        default = {
            'button': BUTTON_MODE,
            'botpm': P_TTI_SHOW_OFF,
            'file_secure': PROTECT_CONTENT,
            'imdb': IMDB,
            'spell_check': SPELL_CHECK_REPLY,
            'welcome': MELCOW_NEW_USERS,
            'auto_delete': AUTO_DELETE,
            'auto_ffilter': AUTO_FFILTER,
            'max_btn': MAX_BTN,
            'template': IMDB_TEMPLATE,
            'log': LOG_VR_CHANNEL,
            'tutorial': TUTORIAL,
            'tutorial_2': TUTORIAL_2,
            'tutorial_3': TUTORIAL_3,
            'shortner': SHORTENER_WEBSITE,
            'api': SHORTENER_API,
            'shortner_two': SHORTENER_WEBSITE2,
            'api_two': SHORTENER_API2,
            'shortner_three': SHORTENER_WEBSITE3,
            'api_three': SHORTENER_API3,
            'is_verify': IS_VERIFY,
            'verify_time': TWO_VERIFY_GAP,
            'third_verify_time': THREE_VERIFY_GAP,
            'caption': CUSTOM_FILE_CAPTION,
            'fsub': AUTH_CHANNELS,
        }
        chat = await self._find_one("grp", {'id': int(id)})
        if chat and 'settings' in chat:
            saved = chat['settings']
            res = default.copy()
            res.update(saved)
            try:
                res['log'] = int(res['log'])
            except (TypeError, ValueError):
                res['log'] = LOG_VR_CHANNEL
            if res['log'] == -100:
                res['log'] = LOG_VR_CHANNEL
            if not saved.get('custom_shortner'):
                res['shortner'] = SHORTENER_WEBSITE
                res['api'] = SHORTENER_API
            if not saved.get('custom_shortner_two'):
                res['shortner_two'] = SHORTENER_WEBSITE2
                res['api_two'] = SHORTENER_API2
            if not saved.get('custom_shortner_three'):
                res['shortner_three'] = SHORTENER_WEBSITE3
                res['api_three'] = SHORTENER_API3
            return res
        else:
            return default.copy()

    async def dreamx_reset_settings(self):
        try:
            result = await self.grp.update_many(
                {'settings': {'$exists': True}}, 
                {'$unset': {'settings': ""}}    
            )
            return result.modified_count
        except Exception as e:
            logger.error("Failed to reset group settings: %s", e)
            raise  

    async def disable_chat(self, chat, reason="No Reason"):
        chat_status=dict(
            is_disabled=True,
            reason=reason,
            )
        await self.grp.update_one({'id': int(chat)}, {'$set': {'chat_status': chat_status}})

    async def total_chat_count(self):
        chats = await self._merged_documents("groups")
        return len(chats)
    
    async def get_all_chats(self):
        return await self._merged_documents("groups")

    async def get_user(self, user_id):
        return await self._find_one("users", {"id": user_id})
    async def update_user(self, user_data):
        self._prem_cache.pop(user_data.get("id"), None)
        await self.users.update_one({"id": user_data["id"]}, {"$set": user_data}, upsert=True)
  
    async def get_notcopy_user(self, user_id):
        user_id = int(user_id)
        user = await self._find_one("misc", {"user_id": user_id})
        if not user:
            res = {
                "user_id": user_id,
                "last_verified": datetime.datetime(2020, 5, 17, 0, 0, 0, tzinfo=TZ),
                "second_time_verified": datetime.datetime(2019, 5, 17, 0, 0, 0, tzinfo=TZ),
            }
            await self.misc.insert_one(res)
            return res
        return user

    async def update_notcopy_user(self, user_id, value: dict):
        user_id = int(user_id)
        existing = await self._find_one("misc", {"user_id": user_id})
        document = {"user_id": user_id}
        if existing:
            document.update(existing)
        document.update(value)
        return await self.misc.replace_one({"user_id": user_id}, document, upsert=True)

    async def is_user_verified(self, user_id):
        user = await self.get_notcopy_user(user_id)
        try:
            pastDate = user["last_verified"]
        except Exception:
            user = await self.get_notcopy_user(user_id)
            pastDate = user["last_verified"]
        pastDate = pastDate.astimezone(TZ)
        current_time = datetime.datetime.now(tz=TZ)
        seconds_since_midnight = (current_time - datetime.datetime(current_time.year, current_time.month, current_time.day, 0, 0, 0, tzinfo=TZ)).total_seconds()
        time_diff = current_time - pastDate
        total_seconds = time_diff.total_seconds()
        return total_seconds <= seconds_since_midnight

    async def user_verified(self, user_id):
        user = await self.get_notcopy_user(user_id)
        try:
            pastDate = user["second_time_verified"]
        except Exception:
            user = await self.get_notcopy_user(user_id)
            pastDate = user["second_time_verified"]
        pastDate = pastDate.astimezone(TZ)
        current_time = datetime.datetime.now(tz=TZ)
        seconds_since_midnight = (current_time - datetime.datetime(current_time.year, current_time.month, current_time.day, 0, 0, 0, tzinfo=TZ)).total_seconds()
        time_diff = current_time - pastDate
        total_seconds = time_diff.total_seconds()
        return total_seconds <= seconds_since_midnight

    async def use_second_shortener(self, user_id, time):
        user = await self.get_notcopy_user(user_id)
        if not user.get("second_time_verified"):
            await self.update_notcopy_user(user_id, {"second_time_verified":datetime.datetime(2019, 5, 17, 0, 0, 0, tzinfo=TZ)})
            user = await self.get_notcopy_user(user_id)
        if await self.is_user_verified(user_id):
            try:
                pastDate = user["last_verified"]
            except Exception:
                user = await self.get_notcopy_user(user_id)
                pastDate = user["last_verified"]
            pastDate = pastDate.astimezone(TZ)
            current_time = datetime.datetime.now(tz=TZ)
            time_difference = current_time - pastDate
            if time_difference > datetime.timedelta(seconds=time):
                pastDate = user["last_verified"].astimezone(TZ)
                second_time = user["second_time_verified"].astimezone(TZ)
                return second_time < pastDate
        return False

    async def use_third_shortener(self, user_id, time):
        user = await self.get_notcopy_user(user_id)
        if not user.get("third_time_verified"):
            await self.update_notcopy_user(user_id, {"third_time_verified":datetime.datetime(2018, 5, 17, 0, 0, 0, tzinfo=TZ)})
            user = await self.get_notcopy_user(user_id)
        if await self.user_verified(user_id):
            try:
                pastDate = user["second_time_verified"]
            except Exception:
                user = await self.get_notcopy_user(user_id)
                pastDate = user["second_time_verified"]
            pastDate = pastDate.astimezone(TZ)
            current_time = datetime.datetime.now(tz=TZ)
            time_difference = current_time - pastDate
            if time_difference > datetime.timedelta(seconds=time):
                pastDate = user["second_time_verified"].astimezone(TZ)
                second_time = user["third_time_verified"].astimezone(TZ)
                return second_time < pastDate
        return False
   
    async def create_verify_id(self, user_id: int, hash):
        res = {"user_id": user_id, "hash":hash, "verified":False}
        return await self.verify_id.insert_one(res)

    async def get_verify_id_info(self, user_id: int, hash):
        return await self._find_one("verify_id", {"user_id": user_id, "hash": hash})

    async def update_verify_id_info(self, user_id, hash, value: dict):
        existing = await self._find_one("verify_id", {"user_id": user_id, "hash": hash}) or {
            "user_id": user_id,
            "hash": hash,
        }
        existing.update(value)
        return await self.verify_id.replace_one(
            {"user_id": user_id, "hash": hash}, existing, upsert=True
        )
        
    async def has_premium_access(self, user_id):
        now_ts = datetime.datetime.now().timestamp()
        cached = self._prem_cache.get(user_id)
        if cached and (now_ts - cached[1]) < 60:
            return cached[0]

        user_data = await self.get_user(user_id)
        has_access = False
        if user_data:
            expiry_time = user_data.get("expiry_time")
            if isinstance(expiry_time, datetime.datetime):
                now = datetime.datetime.now(expiry_time.tzinfo) if expiry_time.tzinfo else datetime.datetime.now()
                if now <= expiry_time:
                    has_access = True
                else:
                    await self.users.update_one({"id": user_id}, {"$set": {"expiry_time": None}})
            elif expiry_time is not None:
                await self.users.update_one({"id": user_id}, {"$set": {"expiry_time": None}})

        if len(self._prem_cache) > 300:
            self._prem_cache.clear()
        self._prem_cache[user_id] = (has_access, now_ts)
        return has_access
        
    

    async def update_one(self, filter_query, update_data):
        try:
            result = await self.users.update_one(filter_query, update_data)
            return result.matched_count == 1
        except Exception as e:
            logger.error("Error updating document: %s", e)
            return False

    async def get_expired(self, current_time):
        primary = await self.users.find({"expiry_time": {"$lt": current_time}}).to_list(length=None)
        expired_users = MergedList(primary)
        seen = {user.get("id") for user in primary}
        if self.secondary:
            secondary = await self.secondary.db.uersz.find(
                {"expiry_time": {"$lt": current_time}}
            ).to_list(length=None)
            for user in secondary:
                if user.get("id") not in seen:
                    seen.add(user.get("id"))
                    expired_users.append(user)
        return expired_users

    async def remove_premium_access(self, user_id):
        return await self.update_one(
            {"id": user_id}, {"$set": {"expiry_time": None}}
        )

    async def check_trial_status(self, user_id):
        user_data = await self.get_user(user_id)
        if user_data:
            return user_data.get("has_free_trial", False)
        return False

    async def give_free_trial(self, user_id):
        user_id = user_id
        seconds = 5*60         
        expiry_time = datetime.datetime.now() + datetime.timedelta(seconds=seconds)
        user_data = {"id": user_id, "expiry_time": expiry_time, "has_free_trial": True}
        await self.users.update_one({"id": user_id}, {"$set": user_data}, upsert=True)

    async def reset_free_trial(self, user_id=None):
        if user_id is None:
            update_data = {"$set": {"has_free_trial": False}}
            result = await self.users.update_many({}, update_data)  
            return result.modified_count
        else:
            update_data = {"$set": {"has_free_trial": False}}
            result = await self.users.update_one({"id": user_id}, update_data)
            return 1 if result.modified_count > 0 else 0  
        
    async def all_premium_users(self):
        query = {"expiry_time": {"$gt": datetime.datetime.now()}}
        primary = await self.users.find(query).to_list(length=None)
        users = MergedList(primary)
        seen = {user.get("id") for user in primary}
        if self.secondary:
            secondary = await self.secondary.db.uersz.find(query).to_list(length=None)
            for user in secondary:
                if user.get("id") not in seen:
                    seen.add(user.get("id"))
                    users.append(user)
        return len(users)
        
    async def get_all_premium_user_data(self):
        query = {"expiry_time": {"$gt": datetime.datetime.now()}}
        return await self._merged_premium_documents(query)

    async def _merged_premium_documents(self, query):
        primary = await self.users.find(query).to_list(length=None)
        users = MergedList(primary)
        seen = {user.get("id") for user in primary}
        if self.secondary:
            secondary = await self.secondary.db.uersz.find(query).to_list(length=None)
            for user in secondary:
                if user.get("id") not in seen:
                    seen.add(user.get("id"))
                    users.append(user)
        return users
    
    async def get_bot_setting(self, bot_id, setting_key, default_value):
        bot = await self.botcol.find_one({'id': int(bot_id)}, {setting_key: 1, '_id': 0})
        return bot[setting_key] if bot and setting_key in bot else default_value
        
    async def update_bot_setting(self, bot_id, setting_key, value):
        await self.botcol.update_one(
            {'id': int(bot_id)}, 
            {'$set': {setting_key: value}}, 
            upsert=True
        )

    async def connect_group(self, group_id, user_id):
        user= await self.connection.find_one({'_id': user_id})
        if user:
            if group_id not in user["group_ids"]:
                await self.connection.update_one({'_id': user_id}, {"$push": {"group_ids": group_id}})
        else:
            await self.connection.insert_one({'_id': user_id, 'group_ids': [group_id]})

    async def get_connected_grps(self, user_id):
        user = await self.connection.find_one({'_id': user_id})
        if user:
            return user["group_ids"]
        else:
            return []
        
    async def remove_group_connection(self, group_id, user_id):
        await self.connection.update_one(
            {'_id': user_id},
            {'$pull': {'group_ids': group_id}}
        )

    async def pm_search_status(self, bot_id):
        return await self.get_bot_setting(bot_id, 'PM_SEARCH', PM_SEARCH)

    async def update_pm_search_status(self, bot_id, enable):
        await self.update_bot_setting(bot_id, 'PM_SEARCH', enable)

    async def movie_update_status(self, bot_id):
        return await self.get_bot_setting(bot_id, 'MOVIE_UPDATE_NOTIFICATION', MOVIE_UPDATE_NOTIFICATION)

    async def update_movie_update_status(self, bot_id, enable):
        await self.update_bot_setting(bot_id, 'MOVIE_UPDATE_NOTIFICATION', enable)

    async def maintenance_status(self, bot_id):
        return await self.get_bot_setting(bot_id, 'MAINTENANCE', MAINTENANCE)

    async def update_maintenance_status(self, bot_id, enable):
        await self.update_bot_setting(bot_id, 'MAINTENANCE', enable)
     
if DATABASE_URI2:
    uri_database_name = urlparse(DATABASE_URI2).path.lstrip("/").split("?", 1)[0]
    secondary_db_name = DATABASE_NAME2 or uri_database_name
    if not secondary_db_name:
        raise ConfigurationError(
            "Secondary MongoDB database name is missing. Set DATABASE_NAME2 or include the database in DATABASE_URI2."
        )
else:
    secondary_db_name = DATABASE_NAME

logger.info(
    "MongoDB user routing: primary=%s, secondary=%s",
    DATABASE_NAME,
    secondary_db_name,
)

if DATABASE_URI2:
    db2 = Database(DATABASE_URI2, secondary_db_name)
    db = Database(DATABASE_URI, DATABASE_NAME, secondary=db2)
else:
    db2 = db = Database(DATABASE_URI, DATABASE_NAME)


