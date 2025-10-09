import os
from telethon import TelegramClient
from telethon.sessions import StringSession
import logging
from dotenv import load_dotenv


# Load environment variables from .env file
load_dotenv()


logger = logging.getLogger(__name__)


class TelegramService:
    def __init__(self):
        # Load from environment variables
        self.api_id = int(os.getenv('TELEGRAM_API_ID'))
        self.api_hash = os.getenv('TELEGRAM_API_HASH')
        self.phone = os.getenv('TELEGRAM_PHONE')
        
        # Debug print (remove in production)
        print(f"Loading API ID: {self.api_id}")
        print(f"Loading API Hash: {self.api_hash[:10]}...")
        print(f"Loading Phone: {self.phone}")
        
        # Use sessions directory in current working directory (Railway-compatible)
        session_path = os.path.join(os.getcwd(), "sessions", "session_name")
        
        # Create sessions directory if it doesn't exist
        os.makedirs(os.path.dirname(session_path), exist_ok=True)
        
        print(f"Session file path: {session_path}")
        
        # Create client with session file in sessions directory
        self.client = TelegramClient(session_path, self.api_id, self.api_hash)
        self.connected = False
    
    async def start(self):
        """Start the Telegram client and authenticate"""
        try:
            print("Starting Telegram client...")
            await self.client.start(phone=self.phone)
            self.connected = True
            
            # Get user info
            me = await self.client.get_me()
            print(f"Connected as: {me.first_name} {me.last_name} (@{me.username})")
            logger.info(f"Connected as: {me.first_name} {me.last_name} (@{me.username})")
            
            # Import and set up event handlers
            from .handlers import setup_handlers
            setup_handlers(self.client)
            
            # Keep running
            await self.client.run_until_disconnected()
            
        except Exception as e:
            print(f"Failed to start Telegram client: {e}")
            logger.error(f"Failed to start Telegram client: {e}")
            self.connected = False
    
    async def stop(self):
        """Stop the Telegram client"""
        if self.client.is_connected():
            await self.client.disconnect()
        self.connected = False
    
    def is_connected(self):
        return self.connected and self.client.is_connected()
    
    async def get_me(self):
        """Get current user info"""
        if self.client.is_connected():
            return await self.client.get_me()
        return None
