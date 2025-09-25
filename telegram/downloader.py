import json
import boto3
import os
import logging
from datetime import datetime
import uuid
import tempfile
import requests
from .database import DatabaseManager

logger = logging.getLogger(__name__)

class VideoDownloader:
    def __init__(self):
        # Database manager
        self.db = DatabaseManager()
        
        # AWS clients (with region configured)
        self.aws_region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        self.s3_bucket = os.getenv('S3_BUCKET_NAME')
        self.telegram_bot_token = os.getenv('TELEGRAM_BOT_TOKEN')  # For user notifications
        
        # Initialize AWS clients with region
        if self.s3_bucket:
            try:
                self.s3_client = boto3.client('s3', region_name=self.aws_region)
                self.lambda_client = boto3.client('lambda', region_name=self.aws_region)
                print(f"✅ AWS clients initialized for region: {self.aws_region}")
                
                # Test S3 connection
                self.s3_client.head_bucket(Bucket=self.s3_bucket)
                print(f"✅ S3 bucket accessible: {self.s3_bucket}")
                
            except Exception as e:
                self.s3_client = None
                self.lambda_client = None
                print(f"❌ AWS initialization failed: {e}")
        else:
            self.s3_client = None
            self.lambda_client = None
            print("❌ S3_BUCKET_NAME not set - cannot run in production")
        
        # Create temp directory for downloads (no persistent storage)
        self.temp_dir = tempfile.mkdtemp(prefix="telegram_videos_")
        print(f"📁 Temp directory created: {self.temp_dir}")
        
        # Log directory (minimal logging only)
        self.log_dir = "/tmp/logs"
        os.makedirs(self.log_dir, exist_ok=True)
        self.log_file = os.path.join(self.log_dir, f"downloads_{datetime.now().strftime('%Y%m%d')}.log")
    
    async def download_video(self, message, document=None, download_info=None):
        """Process video - PRODUCTION VERSION with comprehensive error handling"""
        submission_id = None
        volunteer_id = None
        temp_file_path = None
        
        try:
            # Extract data
            volunteer_id = str(download_info.get('sender_id'))
            telegram_file_id = str(document.id) if document else f"msg_{message.id}"
            file_size_mb = document.size/1024/1024 if document else 0
            
            print(f"\n🔍 PROCESSING VIDEO")
            print(f"👤 User ID: {volunteer_id}")
            print(f"📄 File ID: {telegram_file_id}")
            print(f"📊 File Size: {file_size_mb:.2f} MB")
            
            # Send immediate acknowledgment to user
            await self.send_user_notification(
                volunteer_id, 
                f"📥 **Video Received!**\n\nYour {file_size_mb:.2f} MB video is being processed.\nPlease wait...",
                "info"
            )
            
            # Step 1: Pre-flight checks
            if not self.s3_client or not self.s3_bucket:
                error_msg = "❌ AWS S3 not configured - cannot process videos"
                print(error_msg)
                await self.send_user_notification(
                    volunteer_id,
                    "❌ **System Error**\n\nVideo processing service is temporarily unavailable. Please try again later.",
                    "error"
                )
                return {"status": "configuration_error", "message": error_msg}
            
            # Step 2: Check if user is registered
            try:
                volunteer = await self.db.check_volunteer_exists(volunteer_id)
            except Exception as e:
                error_msg = f"❌ Database connection failed: {e}"
                print(error_msg)
                await self.send_user_notification(
                    volunteer_id,
                    "❌ **Database Error**\n\nCannot verify your registration. Please try again later.",
                    "error"
                )
                return {"status": "database_error", "message": error_msg}
            
            if not volunteer:
                print(f"❌ User {volunteer_id} NOT REGISTERED - Auto-registering...")
                try:
                    first_name = download_info.get('sender_name', 'Unknown User')
                    success = await self.db.complete_user_registration(
                        volunteer_id, 
                        download_info.get('sender_phone', '+0000000000'),
                        first_name.split(' ')[0],
                        ' '.join(first_name.split(' ')[1:]) if ' ' in first_name else None,
                        download_info.get('sender_username')
                    )
                    
                    if success:
                        print(f"✅ Auto-registered user: {first_name}")
                        volunteer = await self.db.check_volunteer_exists(volunteer_id)
                        await self.send_user_notification(
                            volunteer_id,
                            f"✅ **Welcome {first_name}!**\n\nYou've been registered successfully. Processing your video...",
                            "success"
                        )
                    else:
                        await self.send_user_notification(
                            volunteer_id,
                            "❌ **Registration Failed**\n\nCannot create your account. Please contact support.",
                            "error"
                        )
                        return {"status": "registration_failed"}
                        
                except Exception as e:
                    error_msg = f"❌ Auto-registration failed: {e}"
                    print(error_msg)
                    await self.send_user_notification(
                        volunteer_id,
                        "❌ **Registration Error**\n\nCannot create your account. Please try again later.",
                        "error"
                    )
                    return {"status": "registration_error", "message": error_msg}
            
            print(f"✅ REGISTERED USER: {volunteer['first_name']}")
            
            # Step 3: Create submission record
            try:
                submission_id = await self.db.create_video_submission(volunteer_id, telegram_file_id)
                if not submission_id:
                    raise Exception("Failed to generate submission ID")
                print(f"💾 CREATED SUBMISSION: {submission_id}")
            except Exception as e:
                error_msg = f"❌ Failed to create submission record: {e}"
                print(error_msg)
                await self.send_user_notification(
                    volunteer_id,
                    "❌ **Database Error**\n\nCannot create submission record. Please try again.",
                    "error"
                )
                return {"status": "database_error", "message": error_msg}
            
            # Step 4: Download video to temporary file
            try:
                temp_file_path = await self.download_video_temp(message, document, submission_id)
                if not temp_file_path:
                    raise Exception("Download returned None")
                print(f"✅ TEMP DOWNLOAD: {temp_file_path}")
            except Exception as e:
                error_msg = f"❌ Video download failed: {e}"
                print(error_msg)
                await self.update_submission_status(submission_id, 'FAILED', error_msg)
                await self.send_user_notification(
                    volunteer_id,
                    "❌ **Download Failed**\n\nCannot download your video from Telegram. Please try sending it again.",
                    "error"
                )
                return {"status": "download_failed", "message": error_msg}
            
            # Step 5: Upload to S3
            try:
                s3_key = await self.upload_to_s3(temp_file_path, submission_id, volunteer_id, download_info)
                if not s3_key:
                    raise Exception("S3 upload returned None")
                print(f"☁️ S3 UPLOAD: {s3_key}")
            except Exception as e:
                error_msg = f"❌ S3 upload failed: {e}"
                print(error_msg)
                await self.update_submission_status(submission_id, 'FAILED', error_msg)
                await self.send_user_notification(
                    volunteer_id,
                    "❌ **Upload Failed**\n\nCannot upload your video to cloud storage. Please try again.",
                    "error"
                )
                return {"status": "s3_upload_failed", "message": error_msg}
            
            # Step 6: Trigger Lambda 2
            try:
                await self.trigger_lambda_2(submission_id, volunteer_id, s3_key, download_info)
                print(f"🚀 LAMBDA 2 TRIGGERED")
                
                # Success notification
                await self.send_user_notification(
                    volunteer_id,
                    f"✅ **Processing Started!**\n\nYour video has been uploaded successfully and is now being processed.\n\n📊 **Details:**\n• Size: {file_size_mb:.2f} MB\n• Submission ID: `{submission_id[:8]}...`\n\nYou'll receive another message when processing is complete!",
                    "success"
                )
                
            except Exception as e:
                error_msg = f"❌ Lambda trigger failed: {e}"
                print(error_msg)
                await self.update_submission_status(submission_id, 'FAILED', error_msg)
                await self.send_user_notification(
                    volunteer_id,
                    "❌ **Processing Failed**\n\nVideo uploaded but cannot start processing. Our team has been notified.",
                    "error"
                )
                return {"status": "lambda_trigger_failed", "message": error_msg}
            
            return {
                "status": "success",
                "submission_id": submission_id,
                "volunteer_id": volunteer_id,
                "s3_key": s3_key,
                "file_size_mb": file_size_mb,
                "message": "Video uploaded to S3 and Lambda 2 triggered successfully"
            }
            
        except Exception as e:
            error_msg = f"❌ Unexpected error in download_video: {e}"
            logger.error(error_msg, exc_info=True)
            print(error_msg)
            
            # Update submission status if we have one
            if submission_id:
                try:
                    await self.update_submission_status(submission_id, 'FAILED', error_msg)
                except:
                    pass
            
            # Notify user
            if volunteer_id:
                await self.send_user_notification(
                    volunteer_id,
                    "❌ **Unexpected Error**\n\nSomething went wrong while processing your video. Please try again or contact support.",
                    "error"
                )
            
            return {"status": "unexpected_error", "message": error_msg}
            
        finally:
            # Always clean up temp file
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                    print(f"🧹 Cleaned up temp file: {temp_file_path}")
                except Exception as e:
                    print(f"⚠️ Failed to cleanup temp file: {e}")
    
    async def download_video_temp(self, message, document, submission_id: str):
        """Download video to temporary file (no persistent storage)"""
        try:
            # Generate temporary filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"video_{submission_id[:8]}_{timestamp}.mp4"
            
            if document:
                for attr in document.attributes:
                    if hasattr(attr, 'file_name') and attr.file_name:
                        extension = attr.file_name.split('.')[-1] if '.' in attr.file_name else 'mp4'
                        filename = f"video_{submission_id[:8]}_{timestamp}.{extension}"
                        break
            
            temp_file_path = os.path.join(self.temp_dir, filename)
            print(f"📥 DOWNLOADING TO TEMP: {filename}")
            
            downloaded_file = await message.download_media(
                file=temp_file_path,
                progress_callback=self.progress_callback
            )
            
            if downloaded_file and os.path.exists(downloaded_file):
                actual_size = os.path.getsize(downloaded_file)
                print(f"✅ DOWNLOAD COMPLETE: {actual_size/1024/1024:.2f} MB")
                return downloaded_file
            else:
                raise Exception("Downloaded file does not exist")
            
        except Exception as e:
            logger.error(f"❌ Temp download failed: {e}")
            raise
    
    async def upload_to_s3(self, temp_file_path: str, submission_id: str, volunteer_id: str, download_info: dict):
        """Upload to S3 with retry logic"""
        try:
            # Prepare S3 key (same format as Lambda 1)
            file_extension = temp_file_path.split('.')[-1]
            s3_key = f"temp_videos/{submission_id}.{file_extension}"
            
            print(f"☁️ Uploading to S3: {s3_key}")
            
            # Get file size for progress
            file_size = os.path.getsize(temp_file_path)
            print(f"📊 File size: {file_size/1024/1024:.2f} MB")
            
            # Upload to S3 with metadata
            with open(temp_file_path, 'rb') as f:
                self.s3_client.upload_fileobj(
                    f,
                    self.s3_bucket,
                    s3_key,
                    ExtraArgs={
                        'ContentType': 'video/mp4',
                        'Metadata': {
                            'submission_id': submission_id,
                            'volunteer_id': volunteer_id,
                            'uploaded_at': datetime.utcnow().isoformat(),
                            'source': 'mtproto_fastapi_ec2',
                            'original_size': str(file_size),
                            'sender_name': download_info.get('sender_name', ''),
                            'sender_phone': download_info.get('sender_phone', '')
                        }
                    }
                )
            
            # Verify upload
            try:
                self.s3_client.head_object(Bucket=self.s3_bucket, Key=s3_key)
                print(f"✅ S3 upload verified: {s3_key}")
            except:
                raise Exception("S3 upload verification failed")
            
            return s3_key
            
        except Exception as e:
            logger.error(f"❌ S3 upload failed: {e}")
            raise
    
    async def trigger_lambda_2(self, submission_id: str, volunteer_id: str, s3_key: str, download_info: dict):
        """Trigger Lambda 2 Video Processor (same payload as Lambda 1)"""
        try:
            function_name = os.getenv('VIDEO_PROCESSOR_FUNCTION_NAME')
            if not function_name:
                raise Exception("VIDEO_PROCESSOR_FUNCTION_NAME not set")
            
            # Same payload structure as Lambda 1
            payload = {
                'submission_id': submission_id,
                'volunteer_id': volunteer_id,
                's3_key': s3_key,
                'video_title': f"MTProto Submission from {download_info.get('sender_name', volunteer_id)}"
            }
            
            # Trigger Lambda 2 asynchronously
            response = self.lambda_client.invoke(
                FunctionName=function_name,
                InvocationType='Event',  # Async
                Payload=json.dumps(payload)
            )
            
            # Check response
            if response['StatusCode'] != 202:
                raise Exception(f"Lambda invoke failed with status: {response['StatusCode']}")
            
            print(f"✅ Lambda 2 triggered successfully: {function_name}")
            
        except Exception as e:
            logger.error(f"❌ Failed to trigger Lambda 2: {e}")
            raise
    
    async def send_user_notification(self, chat_id: str, message: str, message_type: str = "info"):
        """Send notification to user via Telegram Bot API"""
        if not self.telegram_bot_token:
            print(f"⚠️ No bot token - cannot send notification to {chat_id}")
            return
        
        try:
            send_url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"
            
            # Add emoji based on type
            emoji_map = {
                "info": "ℹ️",
                "success": "✅", 
                "error": "❌",
                "warning": "⚠️"
            }
            
            formatted_message = f"{emoji_map.get(message_type, 'ℹ️')} {message}"
            
            payload = {
                "chat_id": chat_id,
                "text": formatted_message,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True
            }
            
            response = requests.post(send_url, json=payload, timeout=10)
            
            if response.status_code == 200:
                print(f"📤 Notification sent to {chat_id}: {message_type}")
            else:
                print(f"❌ Failed to send notification: {response.status_code}")
                
        except Exception as e:
            print(f"❌ Notification error: {e}")
    
    async def update_submission_status(self, submission_id: str, status: str, reason: str = None):
        """Update submission status in database"""
        try:
            await self.db.update_submission_status(submission_id, status, reason)
            print(f"💾 Updated submission {submission_id[:8]}... status: {status}")
        except Exception as e:
            logger.error(f"Failed to update submission status: {e}")
    
    async def progress_callback(self, current, total):
        """Progress callback for downloads"""
        if total > 0:
            percent = (current / total) * 100
            if int(percent) % 20 == 0 and percent > 0:  # Show every 20%
                mb_current = current / 1024 / 1024
                mb_total = total / 1024 / 1024
                print(f"📊 Progress: {percent:.1f}% ({mb_current:.1f}/{mb_total:.1f} MB)")
    
    def __del__(self):
        """Cleanup temp directory on destruction"""
        try:
            if hasattr(self, 'temp_dir') and os.path.exists(self.temp_dir):
                import shutil
                shutil.rmtree(self.temp_dir)
                print(f"🧹 Cleaned up temp directory: {self.temp_dir}")
        except:
            pass
