from telethon import events
import os
import logging
import asyncio
from datetime import datetime
from .downloader import VideoDownloader


# Set up detailed logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
downloader = VideoDownloader()


# Track active downloads
active_downloads = {}  # Changed to dict to track details


def log_box(title, details=None, emoji="📦"):
    """Create beautiful box logs"""
    width = 70
    print("\n" + "=" * width)
    print(f"{emoji} {title.center(width - 4)}")
    print("=" * width)
    if details:
        for key, value in details.items():
            print(f"  {key}: {value}")
        print("=" * width)


def log_step(step_num, total_steps, description, emoji="➡️"):
    """Log individual steps with numbers"""
    print(f"\n{emoji} STEP {step_num}/{total_steps}: {description}")


def log_success(message, emoji="✅"):
    """Log success messages"""
    print(f"{emoji} {message}")


def log_error(message, emoji="❌"):
    """Log error messages"""
    print(f"{emoji} {message}")


def log_info(message, emoji="ℹ️"):
    """Log info messages"""
    print(f"{emoji} {message}")


def log_progress(current, total, prefix="Progress"):
    """Log progress with visual bar"""
    percent = (current / total) * 100
    bar_length = 40
    filled = int(bar_length * current / total)
    bar = "█" * filled + "░" * (bar_length - filled)
    print(f"  📊 {prefix}: [{bar}] {percent:.1f}% ({current}/{total})")


def setup_handlers(client):
    """Set up all event handlers for the Telegram client"""
    
    @client.on(events.NewMessage())
    async def handle_new_message(event):
        """Handle incoming messages and download videos - NON-BLOCKING"""
        
        log_box("NEW MESSAGE RECEIVED", emoji="📨")
        
        # Get sender information
        try:
            sender = await event.get_sender()
            chat = await event.get_chat()
        except Exception as e:
            log_error(f"Failed to get sender/chat info: {e}")
            sender = None
            chat = None
        
        # Print sender details in organized format
        sender_details = {
            "Sender ID": event.sender_id,
            "Sender Name": f"{getattr(sender, 'first_name', 'Unknown')} {getattr(sender, 'last_name', '')}" if sender else "Unknown",
            "Username": f"@{getattr(sender, 'username', 'None')}" if sender else "None",
            "Phone": getattr(sender, 'phone', 'Not available') if sender else "Not available",
            "Sender Type": type(sender).__name__ if sender else "Unknown",
            "Chat Type": type(chat).__name__ if chat else "Unknown",
            "Message ID": event.message.id,
            "Timestamp": event.message.date.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        for key, value in sender_details.items():
            log_info(f"{key}: {value}")
        
        # ============================================================
        # RESTRICTION: Only accept messages from personal users
        # ============================================================
        
        from telethon.tl.types import User, Channel
        
        log_step(1, 3, "Validating message source", "🔍")
        
        if not sender or not isinstance(sender, User):
            log_error("Message not from a personal user")
            log_info(f"Sender type: {type(sender).__name__ if sender else 'None'}")
            log_info("Only personal user messages are accepted")
            log_box("MESSAGE REJECTED", emoji="🚫")
            return
        
        if isinstance(chat, Channel):
            log_error("Message from channel/group")
            log_info(f"Chat type: {type(chat).__name__}")
            log_info(f"Chat title: {getattr(chat, 'title', 'Unknown')}")
            log_info("Only private chats accepted")
            log_box("MESSAGE REJECTED", emoji="🚫")
            return
        
        if not event.is_private:
            log_error("Not a private chat")
            log_info("Only direct messages accepted")
            log_box("MESSAGE REJECTED", emoji="🚫")
            return
        
        log_success(f"Message from personal user: {sender.first_name}")
        
        # ============================================================
        # Process media
        # ============================================================
        
        log_step(2, 3, "Checking for video content", "🎬")
        
        # Capture video caption/description
        video_caption = event.raw_text if event.raw_text else None
        if video_caption:
            log_info(f"Caption provided: \"{video_caption[:50]}{'...' if len(video_caption) > 50 else ''}\"", "📝")
        else:
            log_info("No caption provided", "📝")
        
        # Check if message has media
        if not event.message.media:
            log_info("No media found in message")
            log_box("MESSAGE IGNORED - NO MEDIA", emoji="⏭️")
            return
        
        log_info(f"Media type detected: {type(event.message.media).__name__}")
        
        # Check if it's a document (video file)
        if hasattr(event.message.media, 'document'):
            document = event.message.media.document
            
            file_size_mb = document.size / 1024 / 1024
            
            video_info = {
                "MIME Type": document.mime_type,
                "File Size": f"{file_size_mb:.2f} MB ({document.size:,} bytes)",
                "Document ID": document.id
            }
            
            # Get filename if available
            filename = "unknown"
            for attr in document.attributes:
                if hasattr(attr, 'file_name') and attr.file_name:
                    filename = attr.file_name
                    break
            video_info["Filename"] = filename
            
            log_box("VIDEO DETECTED", video_info, "🎥")
            
            # Prepare download info
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
                'original_filename': filename,
                'description': video_caption
            }
            
            # Check if it's a video
            if document.mime_type and ('video' in document.mime_type or 'application' in document.mime_type):
                
                log_step(3, 3, "Starting background processing", "🚀")
                
                # Generate task ID for tracking
                task_id = f"{event.sender_id}_{event.message.id}"
                
                # Create async task
                task = asyncio.create_task(
                    process_video_async(task_id, event.message, document, download_info, file_size_mb)
                )
                
                # Track active downloads
                active_downloads[task_id] = {
                    'user': sender.first_name,
                    'size_mb': file_size_mb,
                    'started_at': datetime.utcnow(),
                    'task': task
                }
                task.add_done_callback(lambda t: active_downloads.pop(task_id, None))
                
                log_success(f"Video processing task created: {task_id}")
                log_info(f"Background task spawned - Handler is now free", "🆓")
                log_info(f"Active downloads: {len(active_downloads)}", "📊")
                
                # Show active downloads
                if len(active_downloads) > 1:
                    print("\n  📋 Current Queue:")
                    for tid, info in active_downloads.items():
                        elapsed = (datetime.utcnow() - info['started_at']).total_seconds()
                        print(f"    • {info['user']}: {info['size_mb']:.1f} MB (Running {elapsed:.0f}s)")
                
                log_box("HANDLER READY FOR NEXT MESSAGE", emoji="✅")
                
            else:
                log_error(f"File is not a video: {document.mime_type}")
                log_box("MESSAGE IGNORED - NOT VIDEO", emoji="⏭️")
        
        # Also handle video messages
        elif hasattr(event.message.media, 'video'):
            log_box("VIDEO MESSAGE DETECTED", emoji="🎥")
            
            download_info = {
                'sender_id': event.sender_id,
                'sender_name': f"{getattr(sender, 'first_name', '') if sender else ''} {getattr(sender, 'last_name', '') if sender else ''}".strip(),
                'sender_username': getattr(sender, 'username', None) if sender else None,
                'sender_phone': getattr(sender, 'phone', None) if sender else None,
                'chat_id': event.chat_id,
                'chat_title': getattr(chat, 'title', getattr(chat, 'first_name', 'Private Chat')) if chat else 'Unknown Chat',
                'message_id': event.message.id,
                'message_date': event.message.date,
                'description': video_caption
            }
            
            task_id = f"{event.sender_id}_{event.message.id}"
            
            task = asyncio.create_task(
                process_video_async(task_id, event.message, None, download_info, 0)
            )
            
            active_downloads[task_id] = {
                'user': sender.first_name,
                'size_mb': 0,
                'started_at': datetime.utcnow(),
                'task': task
            }
            task.add_done_callback(lambda t: active_downloads.pop(task_id, None))
            
            log_success(f"Video message task created: {task_id}")
            log_info(f"Active downloads: {len(active_downloads)}", "📊")
            log_box("HANDLER READY FOR NEXT MESSAGE", emoji="✅")
        
        else:
            log_error(f"Unknown media type: {type(event.message.media).__name__}")
            log_box("MESSAGE IGNORED - UNKNOWN MEDIA", emoji="⏭️")
    
    log_success("Event handlers initialized successfully")


async def process_video_async(task_id, message, document, download_info, file_size_mb):
    """Process video in background with beautiful logging"""
    try:
        log_box(f"BACKGROUND TASK STARTED: {task_id}", {
            "User": download_info['sender_name'],
            "File Size": f"{file_size_mb:.2f} MB",
            "Task ID": task_id,
            "Active Tasks": len(active_downloads)
        }, "🎬")
        
        start_time = datetime.utcnow()
        
        result = await downloader.download_video(message, document, download_info)
        
        duration = (datetime.utcnow() - start_time).total_seconds()
        
        if result:
            log_box(f"TASK COMPLETED: {task_id}", {
                "Status": "SUCCESS",
                "Submission ID": result.get('submission_id', 'unknown')[:20] + "...",
                "Duration": f"{duration:.1f} seconds",
                "User": download_info['sender_name'],
                "Remaining Tasks": len(active_downloads) - 1
            }, "✅")
        else:
            log_box(f"TASK FAILED: {task_id}", {
                "Status": "FAILED",
                "Duration": f"{duration:.1f} seconds",
                "User": download_info['sender_name']
            }, "❌")
            
    except Exception as e:
        log_box(f"TASK CRASHED: {task_id}", {
            "Error": str(e),
            "User": download_info['sender_name']
        }, "💥")
        logger.error(f"Background processing error: {e}", exc_info=True)
