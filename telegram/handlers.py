from telethon import events
import os
import logging
from .downloader import VideoDownloader

# Set up detailed logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
downloader = VideoDownloader()

def setup_handlers(client):
    """Set up all event handlers for the Telegram client"""
    
    @client.on(events.NewMessage())
    async def handle_new_message(event):
        """Handle incoming messages and download videos"""
        
        print(f"\n=== NEW MESSAGE RECEIVED ===")
        
        # Get sender information
        try:
            sender = await event.get_sender()
            chat = await event.get_chat()
        except Exception as e:
            print(f"Error getting sender/chat info: {e}")
            sender = None
            chat = None
        
        # Print sender details
        print(f"Sender ID: {event.sender_id}")
        if sender:
            print(f"Sender Name: {getattr(sender, 'first_name', 'Unknown')} {getattr(sender, 'last_name', '')}")
            print(f"Sender Username: @{getattr(sender, 'username', 'None')}")
            print(f"Sender Phone: {getattr(sender, 'phone', 'Not available')}")
            print(f"Sender Type: {type(sender).__name__}")
        
        # Print chat details
        print(f"Chat ID: {event.chat_id}")
        if chat:
            print(f"Chat Title: {getattr(chat, 'title', getattr(chat, 'first_name', 'Private Chat'))}")
            print(f"Chat Type: {type(chat).__name__}")
            print(f"Chat Username: @{getattr(chat, 'username', 'None')}")
        
        # Message details
        print(f"Message ID: {event.message.id}")
        print(f"Message Date: {event.message.date}")
        print(f"Message Text: {event.raw_text}")
        print(f"Has Media: {event.message.media is not None}")
        
        # Check if message has media
        if not event.message.media:
            print("No media found in message")
            return
        
        print(f"Media Type: {type(event.message.media)}")
        
        # Check if it's a document (video file)
        if hasattr(event.message.media, 'document'):
            document = event.message.media.document
            
            print(f"Document found!")
            print(f"MIME Type: {document.mime_type}")
            print(f"Size: {document.size} bytes ({document.size/1024/1024:.2f} MB)")
            print(f"Document ID: {document.id}")
            
            # Get filename if available
            filename = "unknown"
            for attr in document.attributes:
                if hasattr(attr, 'file_name') and attr.file_name:
                    filename = attr.file_name
                    break
            print(f"Filename: {filename}")
            
            # Prepare download info with sender details
            download_info = {
                'sender_id': event.sender_id,
                'sender_name': f"{getattr(sender, 'first_name', '') if sender else ''} {getattr(sender, 'last_name', '') if sender else ''}".strip(),
                'sender_username': getattr(sender, 'username', None) if sender else None,
                'sender_phone': getattr(sender, 'phone', None) if sender else None,
                'chat_id': event.chat_id,
                'chat_title': getattr(chat, 'title', getattr(chat, 'first_name', 'Private Chat')) if chat else 'Unknown Chat',
                'message_id': event.message.id,
                'message_date': event.message.date,
                'file_size': document.size,
                'mime_type': document.mime_type,
                'original_filename': filename
            }
            
            # Check if it's a video OR just download everything for testing
            if document.mime_type and ('video' in document.mime_type or 'application' in document.mime_type):
                print("Starting download...")
                result = await downloader.download_video(event.message, document, download_info)
                if result:
                    print(f"Download successful: {result}")
                else:
                    print("Download failed!")
            else:
                print(f"Skipping file - not a video. MIME: {document.mime_type}")
        
        # Also handle video messages (round video messages)
        elif hasattr(event.message.media, 'video'):
            print("Video message found!")
            
            download_info = {
                'sender_id': event.sender_id,
                'sender_name': f"{getattr(sender, 'first_name', '') if sender else ''} {getattr(sender, 'last_name', '') if sender else ''}".strip(),
                'sender_username': getattr(sender, 'username', None) if sender else None,
                'sender_phone': getattr(sender, 'phone', None) if sender else None,
                'chat_id': event.chat_id,
                'chat_title': getattr(chat, 'title', getattr(chat, 'first_name', 'Private Chat')) if chat else 'Unknown Chat',
                'message_id': event.message.id,
                'message_date': event.message.date,
            }
            
            result = await downloader.download_video(event.message, None, download_info)
        
        else:
            print(f"Unknown media type: {type(event.message.media)}")
        
        print("=== MESSAGE PROCESSING COMPLETE ===\n")
    
    print("Event handlers set up successfully")
    