from aria2p import API as Aria2API, Client as Aria2Client
import asyncio
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os
import logging
import math
from pyrogram import Client, filters, enums
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.errors import FloodWait, ButtonUrlInvalid
from pymongo import MongoClient, ASCENDING
import time
import uuid
import urllib.parse
from urllib.parse import urlparse
import requests
from flask import Flask
from typing import Optional, List, Tuple
from threading import Thread, Event
import signal
import sys


# Constants
VALID_DOMAINS = [
    'terabox.com', 'nephobox.com', '4funbox.com', 'mirrobox.com', 
    'momerybox.com', 'teraboxapp.com', '1024tera.com', 
    'terabox.app', 'gibibox.com', 'goaibox.com', 'terasharelink.com', 
    'teraboxlink.com', 'terafileshare.com'
]
DEFAULT_SPLIT_SIZE = 2 * 1024**3  # 2GB
VIP_SPLIT_SIZE = 4 * 1024**3  # 4GB
UPDATE_INTERVAL = 15
TOKEN_EXPIRY_HOURS = 12

# Configuration
class Config:
    def __init__(self):
        load_dotenv('config.env', override=True)
        
        self.API_ID = self._get_env('TELEGRAM_API', required=True)
        self.API_HASH = self._get_env('TELEGRAM_HASH', required=True)
        self.BOT_TOKEN = self._get_env('BOT_TOKEN', required=True)
        self.DUMP_CHAT_ID = int(self._get_env('DUMP_CHAT_ID', required=True))
        self.FSUB_ID = int(self._get_env('FSUB_ID', required=True))
        self.DATABASE_URL = self._get_env('DATABASE_URL', required=True)
        self.SHORTENER_API = self._get_env('SHORTENER_API')
        self.USER_SESSION_STRING = self._get_env('USER_SESSION_STRING')
        
        # Aria2 Configuration
        self.aria2 = Aria2API(
            Aria2Client(host="http://localhost", port=6800, secret=""))
        self._configure_aria2()
        
        # MongoDB Indexes
        self._create_indexes()

    def _get_env(self, key: str, required: bool = False) -> Optional[str]:
        value = os.environ.get(key, '')
        if required and not value:
            logging.error(f"{key} variable is missing! Exiting now")
            sys.exit(1)
        return value or None

    def _configure_aria2(self):
        options = {
            "max-tries": "50",
            "retry-wait": "3",
            "continue": "true",
            "allow-overwrite": "true",
            "min-split-size": "4M",
            "split": "10"
        }
        self.aria2.set_global_options(options)

    def _create_indexes(self):
        client = MongoClient(self.DATABASE_URL)
        db = client["terabox"]
        db["user_requests"].create_index([("user_id", ASCENDING)])
        db["user_requests"].create_index([("token_expiry", ASCENDING)])

# Initialize config
config = Config()

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s - %(name)s - %(levelname)s] %(message)s - %(filename)s:%(lineno)d",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('terabox.log')
    ]
)
logger = logging.getLogger(__name__)

# Pyrogram Clients
bot = Client("jetbot", api_id=config.API_ID, api_hash=config.API_HASH, bot_token=config.BOT_TOKEN)
user_client = None
if config.USER_SESSION_STRING:
    user_client = Client("jetu", api_id=config.API_ID, api_hash=config.API_HASH, 
                       session_string=config.USER_SESSION_STRING)

# Database
client = MongoClient(config.DATABASE_URL)
db = client["terabox"]
collection = db["user_requests"]

# Flask
flask_app = Flask(__name__)
shutdown_event = Event()

# Utilities
def format_size(size: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TB"

def generate_progress_bar(percent: float, length: int = 10) -> str:
    filled = '★' * int(percent / 100 * length)
    empty = '☆' * (length - len(filled))
    return f"[{filled}{empty}]"

async def check_membership(client: Client, user_id: int) -> bool:
    try:
        member = await client.get_chat_member(config.FSUB_ID, user_id)
        return member.status in [
            enums.ChatMemberStatus.MEMBER,
            enums.ChatMemberStatus.ADMINISTRATOR,
            enums.ChatMemberStatus.OWNER
        ]
    except Exception as e:
        logger.error(f"Membership check failed: {e}")
        return False

def generate_uuid(user_id: int) -> str:
    token = str(uuid.uuid4())
    collection.update_one(
        {"user_id": user_id},
        {"$set": {"token": token, "token_status": "inactive", "token_expiry": None}},
        upsert=True
    )
    return token

def activate_token(user_id: int, token: str) -> bool:
    result = collection.update_one(
        {"user_id": user_id, "token": token},
        {"$set": {"token_status": "active", 
                 "token_expiry": datetime.now() + timedelta(hours=TOKEN_EXPIRY_HOURS)}}
    )
    return result.modified_count > 0

def has_valid_token(user_id: int) -> bool:
    user_data = collection.find_one({"user_id": user_id})
    if user_data and user_data.get("token_status") == "active":
        return datetime.now() < user_data.get("token_expiry", datetime.min)
    return False

def is_valid_url(url: str) -> bool:
    parsed = urlparse(url)
    return any(parsed.netloc.endswith(domain) for domain in VALID_DOMAINS)

def shorten_url(url: str) -> Optional[str]:
    if not config.SHORTENER_API:
        return url
    try:
        response = requests.get(
            "https://linkcents.com/api",
            params={"api": config.SHORTENER_API, "url": url}
        )
        return response.json().get("shortenedUrl", url)
    except Exception as e:
        logger.error(f"URL shortening failed: {e}")
        return url

async def safe_edit(message: Message, text: str):
    try:
        await message.edit_text(text)
    except FloodWait as e:
        await asyncio.sleep(e.value)
        await safe_edit(message, text)
    except Exception as e:
        logger.error(f"Message edit failed: {e}")

async def generate_thumbnail(video_path: str) -> Optional[str]:
    thumbnail_path = f"{video_path}.jpg"
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffmpeg", "-ss", "00:00:01", "-i", video_path,
            "-vframes", "1", "-vf", "scale=320:-1",
            "-y", thumbnail_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        await proc.wait()
        return thumbnail_path if os.path.exists(thumbnail_path) else None
    except Exception as e:
        logger.error(f"Thumbnail generation failed: {e}")
        return None

async def get_video_metadata(file_path: str) -> Tuple[int, int, int]:
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error", "-select_streams", "v:0",
            "-show_entries", "stream=duration,width,height",
            "-of", "csv=p=0", file_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        duration, width, height = map(float, stdout.decode().strip().split(','))
        return int(duration), int(width), int(height)
    except Exception as e:
        logger.error(f"Metadata extraction failed: {e}")
        return 0, 1280, 720

# Bot Handlers
@bot.on_message(filters.command("start"))
async def start_handler(client: Client, message: Message):
    user_id = message.from_user.id
    buttons = [
        [InlineKeyboardButton("ᴊᴏɪɴ", url="https://t.me/tellymirror"),
         InlineKeyboardButton("ᴅᴇᴠᴇʟᴏᴘᴇʀ", url="https://t.me/tellyhubownerbot")],
        [InlineKeyboardButton("ʀᴇᴘᴏ 🌐", url="https://github.com/Hrishi2861/Terabox-Downloader-Bot")]
    ]
    
    if len(message.command) > 1 and len(message.command[1]) == 36:
        token = message.command[1]
        if activate_token(user_id, token):
            caption = "🌟 Your token has been activated! You can now use the bot."
        else:
            caption = "❌ Invalid token. Generate a new one using /start"
    else:
        if not has_valid_token(user_id):
            token = generate_uuid(user_id)
            long_url = f"https://redirect.jet-mirror.in/{client.me.username}/{token}"
            short_url = shorten_url(long_url) or long_url
            buttons.insert(0, [InlineKeyboardButton("🔑 Generate Token", url=short_url)])
            caption = "🔑 Generate your access token (Valid for 12 hours)"
        else:
            caption = "✅ You already have an active token!"
    
    try:
        await message.reply_photo(
            photo="https://beritakarya.id/wp-content/uploads/2024/06/terabox.jpg",
            caption=caption,
            reply_markup=InlineKeyboardMarkup(buttons)
        )
    except ButtonUrlInvalid:
        await message.reply_text(
            "⚠️ Invalid button configuration. Please contact admin.",
            reply_markup=InlineKeyboardMarkup(buttons[:2])
        )

@bot.on_message(filters.text)
async def handle_message(client: Client, message: Message):
    user_id = message.from_user.id
    
    if not await check_membership(client, user_id):
        return await message.reply_text(
            "🔒 Please join our channel to use this bot:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Join Channel", url="https://t.me/tellycloudbots")]
            ]))
    
    url = next((word for word in message.text.split() if is_valid_url(word)), None)
    if not url:
        return await message.reply_text("❌ Invalid Terabox link. Please provide a valid URL.")
    
    try:
        await process_download(client, message, url, user_id)
    except Exception as e:
        logger.error(f"Download failed: {e}")
        await message.reply_text("⚠️ Failed to process your request. Please try again later.")

async def process_download(client: Client, message: Message, url: str, user_id: int):
    encoded_url = urllib.parse.quote(url)
    final_url = f"https://teraboxbotredirect.tellycloudapi.workers.dev/?url={encoded_url}"
    
    download = config.aria2.add_uris([final_url])
    status_message = await message.reply_text("🚀Wait Starting download...")
    
    try:
        await track_download_progress(download, status_message, user_id)
        await handle_upload(download, client, message, status_message)
    finally:
        await cleanup(download, status_message, message)

async def track_download_progress(download, status_message, user_id):
    start_time = datetime.now()
    while not download.is_complete:
        await asyncio.sleep(UPDATE_INTERVAL)
        download.update()
        
        elapsed = datetime.now() - start_time
        progress_text = (
            f"📥 Downloading: {download.name}\n"
            f"{generate_progress_bar(download.progress)}\n"
            f"📦 {format_size(download.completed_length)}/{format_size(download.total_length)}\n"
            f"⚡ {format_size(download.download_speed)}/s\n"
            f"⏳ Elapsed: {elapsed.seconds // 60}m {elapsed.seconds % 60}s"
        )
        await safe_edit(status_message, progress_text)

async def handle_upload(download, client, message, status_message):
    file_path = download.files[0].path
    file_size = os.path.getsize(file_path)
    split_size = VIP_SPLIT_SIZE if user_client else DEFAULT_SPLIT_SIZE
    
    if file_size > split_size:
        await split_and_upload(file_path, message, status_message)
    else:
        await direct_upload(file_path, message, status_message)

async def split_and_upload(file_path, message, status_message):
    try:
        split_files = await split_video(file_path, status_message)
        for part in split_files:
            await upload_file(part, message, status_message)
    finally:
        for part in split_files:
            safe_remove(part)

async def direct_upload(file_path, message, status_message):
    try:
        await upload_file(file_path, message, status_message)
    finally:
        safe_remove(file_path)

async def upload_file(file_path: str, message: Message, status_message: Message):
    caption = f"✨ {os.path.basename(file_path)}\n👤 User: {message.from_user.mention}"
    client = user_client or bot
    thumbnail = None

    try:
        thumbnail = await generate_thumbnail(file_path)
        duration, width, height = await get_video_metadata(file_path)
        
        msg = await client.send_video(
            chat_id=config.DUMP_CHAT_ID,
            video=file_path,
            caption=caption,
            duration=duration,
            width=width,
            height=height,
            thumb=thumbnail,
            supports_streaming=True,
            progress=lambda current, total: asyncio.create_task(
                upload_progress(current, total, status_message)
            )
        )
  # Fixed closing parenthesis here
        
        await bot.copy_message(message.chat.id, config.DUMP_CHAT_ID, msg.id)
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        await status_message.edit_text("⚠️ Failed to upload file. Please try again.")
    finally:
        if thumbnail and os.path.exists(thumbnail):
            os.remove(thumbnail)

async def upload_progress(current: int, total: int, status_message: Message):
    progress = (current / total) * 100
    try:
        await status_message.edit_text(
            f"📤 Upload Progress\n"
            f"{generate_progress_bar(progress)}\n"
            f"📊 {format_size(current)}/{format_size(total)}"
        )
    except Exception as e:
        logger.error(f"Progress update failed: {e}")

async def split_video(input_path: str, status_message: Message) -> List[str]:
    try:
        proc = await asyncio.create_subprocess_exec(
            "ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", input_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        total_duration = float(stdout.decode().strip())
        
        file_size = os.path.getsize(input_path)
        parts = math.ceil(file_size / DEFAULT_SPLIT_SIZE)
        duration_per_part = total_duration / parts
        split_files = []
        
        for i in range(parts):
            output_path = f"{os.path.splitext(input_path)[0]}.part{i+1:03d}{os.path.splitext(input_path)[1]}"
            cmd = [
                "ffmpeg", "-y", "-ss", str(i * duration_per_part),
                "-i", input_path, "-t", str(duration_per_part),
                "-c", "copy", output_path
            ]
            proc = await asyncio.create_subprocess_exec(*cmd)
            await proc.wait()
            split_files.append(output_path)
        
        return split_files
    except Exception as e:
        logger.error(f"Video splitting failed: {e}")
        raise

def safe_remove(path: str):
    try:
        os.remove(path)
    except Exception as e:
        logger.error(f"File removal failed: {e}")

async def cleanup(download, status_message, original_message):
    try:
        await status_message.delete()
        await original_message.delete()
        if os.path.exists(download.files[0].path):
            os.remove(download.files[0].path)
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")

# Signal Handling
def signal_handler(sig, frame):
    logger.info("Shutting down gracefully...")
    shutdown_event.set()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

if __name__ == "__main__":
    # Use Heroku-provided PORT, fallback to 5000 locally
    port = int(os.environ.get("PORT", 5000))

    # Start Flask server on the correct port
    Thread(target=lambda: flask_app.run(host='0.0.0.0', port=port)).start()

    # Start Telegram Clients
    if user_client:
        user_client.start()
    bot.run()
