import json
import boto3
import os
import logging
from datetime import datetime
import uuid
import tempfile
import requests
import asyncio
from .database import DatabaseManager

# Set up comprehensive logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/tmp/telegram_mtproto.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class VideoDownloader:
    def __init__(self):
        logger.info("🚀 Initializing VideoDownloader for production")
        
        # Database manager
        try:
            self.db = DatabaseManager()
            logger.info("✅ Database manager initialized")
        except Exception as e:
            logger.error(f"❌ Database manager failed to initialize: {e}")
            raise
        
        # AWS configuration
        self.aws_region = os.getenv('AWS_DEFAULT_REGION', 'ap-south-1')
        self.s3_bucket = os.getenv('S3_BUCKET_NAME')
        self.telegram_bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        
        logger.info(f"📍 AWS Region: {self.aws_region}")
        logger.info(f"🪣 S3 Bucket: {self.s3_bucket}")
        logger.info(f"🤖 Bot Token: {'Set' if self.telegram_bot_token else 'Not Set'}")
        
        # Initialize AWS clients
        self.s3_client = None
        self.lambda_client = None
        
        if self.s3_bucket:
            try:
                self.s3_client = boto3.client('s3', region_name=self.aws_region)
                self.lambda_client = boto3.client('lambda', region_name=self.aws_region)
                
                # Test S3 connection
                self.s3_client.head_bucket(Bucket=self.s3_bucket)
                
                logger.info(f"✅ AWS clients initialized successfully")
                logger.info(f"✅ S3 bucket '{self.s3_bucket}' is accessible")
                
            except Exception as e:
                self.s3_client = None
                self.lambda_client = None
                logger.error(f"❌ AWS initialization failed: {e}")
                logger.error(f"❌ Ensure S3 bucket exists and IAM role has permissions")
        else:
            logger.error("❌ S3_BUCKET_NAME not set - production mode requires S3")
    
    async def download_video(self, message, document=None, download_info=None):
        """Process video - PRODUCTION VERSION with direct S3 upload and comprehensive logging"""
        
        # Initialize tracking variables
        submission_id = None
        volunteer_id = None
        start_time = datetime.utcnow()
        
        # Log the incoming request
        logger.info("=" * 60)
        logger.info("🎬 NEW VIDEO PROCESSING REQUEST")
        logger.info("=" * 60)
        
        try:
            # Extract and log basic information
            volunteer_id = str(download_info.get('sender_id'))
            telegram_file_id = str(document.id) if document else f"msg_{message.id}"
            file_size_mb = document.size/1024/1024 if document else 0
            
            logger.info(f"👤 User ID: {volunteer_id}")
            logger.info(f"📄 Telegram File ID: {telegram_file_id}")
            logger.info(f"📊 File Size: {file_size_mb:.2f} MB")
            logger.info(f"🕐 Start Time: {start_time.isoformat()}")
            
            # Log sender information
            logger.info(f"📱 Sender Name: {download_info.get('sender_name', 'Unknown')}")
            logger.info(f"📞 Sender Phone: {download_info.get('sender_phone', 'Unknown')}")
            logger.info(f"💬 Chat ID: {download_info.get('chat_id', 'Unknown')}")
            
            print(f"\n🔍 PROCESSING VIDEO")
            print(f"👤 User ID: {volunteer_id}")
            print(f"📊 File Size: {file_size_mb:.2f} MB")
            
            # Send immediate acknowledgment
            await self.send_user_notification(
                volunteer_id, 
                f"📥 **Video Received!**\n\nYour {file_size_mb:.2f} MB video is being processed.\nPlease wait...",
                "info"
            )
            
            # STEP 1: Pre-flight checks
            logger.info("🔍 STEP 1: Pre-flight checks")
            
            if not self.s3_client or not self.s3_bucket:
                error_msg = "AWS S3 not configured - cannot process videos in production"
                logger.error(f"❌ {error_msg}")
                logger.error("❌ Check S3_BUCKET_NAME environment variable")
                logger.error("❌ Check IAM role permissions")
                
                await self.send_user_notification(
                    volunteer_id,
                    "❌ **System Error**\n\nVideo processing service is temporarily unavailable. Please try again later.",
                    "error"
                )
                
                return {
                    "status": "configuration_error", 
                    "message": error_msg,
                    "volunteer_id": volunteer_id,
                    "duration_seconds": (datetime.utcnow() - start_time).total_seconds()
                }
            
            logger.info("✅ AWS S3 configuration validated")
            
            # STEP 2: User registration check
            logger.info("🔍 STEP 2: User registration validation")
            
            try:
                volunteer = await self.db.check_volunteer_exists(volunteer_id)
                logger.info(f"📝 Database query for user {volunteer_id}: {'Found' if volunteer else 'Not found'}")
            except Exception as e:
                error_msg = f"Database connection failed: {e}"
                logger.error(f"❌ {error_msg}")
                
                await self.send_user_notification(
                    volunteer_id,
                    "❌ **Database Error**\n\nCannot verify your registration. Please try again later.",
                    "error"
                )
                
                return {
                    "status": "database_error", 
                    "message": error_msg,
                    "volunteer_id": volunteer_id,
                    "duration_seconds": (datetime.utcnow() - start_time).total_seconds()
                }
            
            if not volunteer:
                logger.info(f"🆕 User {volunteer_id} not registered - auto-registering")
                
                try:
                    first_name = download_info.get('sender_name', 'Unknown User')
                    registration_success = await self.db.complete_user_registration(
                        volunteer_id, 
                        download_info.get('sender_phone', '+0000000000'),
                        first_name.split(' ')[0],
                        ' '.join(first_name.split(' ')[1:]) if ' ' in first_name else None,
                        download_info.get('sender_username')
                    )
                    
                    if registration_success:
                        logger.info(f"✅ Auto-registration successful for {first_name}")
                        volunteer = await self.db.check_volunteer_exists(volunteer_id)
                        
                        await self.send_user_notification(
                            volunteer_id,
                            f"✅ **Welcome {first_name}!**\n\nYou've been registered successfully. Processing your video...",
                            "success"
                        )
                    else:
                        logger.error(f"❌ Auto-registration failed for {volunteer_id}")
                        await self.send_user_notification(
                            volunteer_id,
                            "❌ **Registration Failed**\n\nCannot create your account. Please contact support.",
                            "error"
                        )
                        return {
                            "status": "registration_failed",
                            "volunteer_id": volunteer_id,
                            "duration_seconds": (datetime.utcnow() - start_time).total_seconds()
                        }
                        
                except Exception as e:
                    error_msg = f"Auto-registration failed: {e}"
                    logger.error(f"❌ {error_msg}")
                    
                    await self.send_user_notification(
                        volunteer_id,
                        "❌ **Registration Error**\n\nCannot create your account. Please try again later.",
                        "error"
                    )
                    
                    return {
                        "status": "registration_error", 
                        "message": error_msg,
                        "volunteer_id": volunteer_id,
                        "duration_seconds": (datetime.utcnow() - start_time).total_seconds()
                    }
            
            logger.info(f"✅ User validated: {volunteer['first_name']}")
            print(f"✅ REGISTERED USER: {volunteer['first_name']}")
            
            # STEP 3: Create submission record
            logger.info("🔍 STEP 3: Creating submission record")
            
            try:
                submission_id = await self.db.create_video_submission(volunteer_id, telegram_file_id)
                if not submission_id:
                    raise Exception("Failed to generate submission ID")
                
                logger.info(f"💾 Submission created: {submission_id}")
                print(f"💾 CREATED SUBMISSION: {submission_id}")
                
            except Exception as e:
                error_msg = f"Failed to create submission record: {e}"
                logger.error(f"❌ {error_msg}")
                
                await self.send_user_notification(
                    volunteer_id,
                    "❌ **Database Error**\n\nCannot create submission record. Please try again.",
                    "error"
                )
                
                return {
                    "status": "database_error", 
                    "message": error_msg,
                    "volunteer_id": volunteer_id,
                    "duration_seconds": (datetime.utcnow() - start_time).total_seconds()
                }
            
            # STEP 4: Direct download and S3 upload (no local temp files)
            logger.info("🔍 STEP 4: Direct MTProto to S3 upload")
            
            try:
                s3_key = await self.direct_mtproto_to_s3_upload(
                    message, document, submission_id, volunteer_id, download_info
                )
                
                if not s3_key:
                    raise Exception("Direct S3 upload returned None")
                
                logger.info(f"☁️ Direct S3 upload successful: {s3_key}")
                print(f"☁️ S3 UPLOAD: {s3_key}")
                
            except Exception as e:
                error_msg = f"Direct S3 upload failed: {e}"
                logger.error(f"❌ {error_msg}")
                
                # Update submission status
                try:
                    await self.update_submission_status(submission_id, 'FAILED', error_msg)
                except:
                    logger.error("❌ Failed to update submission status after upload failure")
                
                await self.send_user_notification(
                    volunteer_id,
                    "❌ **Upload Failed**\n\nCannot upload your video to cloud storage. Please try again.",
                    "error"
                )
                
                return {
                    "status": "s3_upload_failed", 
                    "message": error_msg,
                    "submission_id": submission_id,
                    "volunteer_id": volunteer_id,
                    "duration_seconds": (datetime.utcnow() - start_time).total_seconds()
                }
            
            # STEP 5: Trigger Lambda 2
            logger.info("🔍 STEP 5: Triggering Lambda 2 (Video Processor)")
            
            try:
                await self.trigger_lambda_2(submission_id, volunteer_id, s3_key, download_info)
                logger.info(f"🚀 Lambda 2 triggered successfully")
                print(f"🚀 LAMBDA 2 TRIGGERED")
                
                # Success notification
                processing_time = (datetime.utcnow() - start_time).total_seconds()
                await self.send_user_notification(
                    volunteer_id,
                    f"✅ **Processing Started!**\n\nYour video has been uploaded successfully and is now being processed.\n\n📊 **Details:**\n• Size: {file_size_mb:.2f} MB\n• Processing Time: {processing_time:.1f}s\n• Submission ID: `{submission_id[:8]}...`\n\nYou'll receive another message when processing is complete!",
                    "success"
                )
                
            except Exception as e:
                error_msg = f"Lambda trigger failed: {e}"
                logger.error(f"❌ {error_msg}")
                
                # Update submission status
                try:
                    await self.update_submission_status(submission_id, 'FAILED', error_msg)
                except:
                    logger.error("❌ Failed to update submission status after Lambda failure")
                
                await self.send_user_notification(
                    volunteer_id,
                    "❌ **Processing Failed**\n\nVideo uploaded but cannot start processing. Our team has been notified.",
                    "error"
                )
                
                return {
                    "status": "lambda_trigger_failed", 
                    "message": error_msg,
                    "submission_id": submission_id,
                    "volunteer_id": volunteer_id,
                    "s3_key": s3_key,
                    "duration_seconds": (datetime.utcnow() - start_time).total_seconds()
                }
            
            # SUCCESS - Log final results
            total_duration = (datetime.utcnow() - start_time).total_seconds()
            
            logger.info("=" * 60)
            logger.info("✅ VIDEO PROCESSING COMPLETED SUCCESSFULLY")
            logger.info("=" * 60)
            logger.info(f"📊 Total Duration: {total_duration:.2f} seconds")
            logger.info(f"📄 Submission ID: {submission_id}")
            logger.info(f"☁️ S3 Key: {s3_key}")
            logger.info(f"🚀 Lambda 2 Status: Triggered")
            logger.info("=" * 60)
            
            return {
                "status": "success",
                "submission_id": submission_id,
                "volunteer_id": volunteer_id,
                "volunteer_name": volunteer['first_name'],
                "s3_key": s3_key,
                "file_size_mb": file_size_mb,
                "duration_seconds": total_duration,
                "message": "Video uploaded to S3 and Lambda 2 triggered successfully"
            }
            
        except Exception as e:
            error_msg = f"Unexpected error in download_video: {e}"
            logger.error("=" * 60)
            logger.error("❌ UNEXPECTED ERROR IN VIDEO PROCESSING")
            logger.error("=" * 60)
            logger.error(f"❌ Error: {error_msg}", exc_info=True)
            logger.error(f"👤 User ID: {volunteer_id}")
            logger.error(f"📄 Submission ID: {submission_id}")
            logger.error(f"🕐 Duration: {(datetime.utcnow() - start_time).total_seconds():.2f}s")
            logger.error("=" * 60)
            
            print(f"❌ UNEXPECTED ERROR: {error_msg}")
            
            # Update submission status if we have one
            if submission_id:
                try:
                    await self.update_submission_status(submission_id, 'FAILED', error_msg)
                    logger.info(f"💾 Updated submission {submission_id} status to FAILED")
                except Exception as update_error:
                    logger.error(f"❌ Failed to update submission status: {update_error}")
            
            # Notify user
            if volunteer_id:
                await self.send_user_notification(
                    volunteer_id,
                    "❌ **Unexpected Error**\n\nSomething went wrong while processing your video. Please try again or contact support.",
                    "error"
                )
            
            return {
                "status": "unexpected_error", 
                "message": error_msg,
                "submission_id": submission_id,
                "volunteer_id": volunteer_id,
                "duration_seconds": (datetime.utcnow() - start_time).total_seconds()
            }
    
    async def direct_mtproto_to_s3_upload(self, message, document, submission_id: str, volunteer_id: str, download_info: dict):
        """Download from Telegram and upload directly to S3 without local storage"""
        
        logger.info("📥 Starting direct MTProto to S3 upload")
        
        try:
            # Generate S3 key
            file_extension = 'mp4'  # Default
            
            if document:
                for attr in document.attributes:
                    if hasattr(attr, 'file_name') and attr.file_name:
                        extension = attr.file_name.split('.')[-1] if '.' in attr.file_name else 'mp4'
                        file_extension = extension
                        logger.info(f"📝 Original filename detected: {attr.file_name}")
                        break
            
            s3_key = f"temp_videos/{submission_id}.{file_extension}"
            logger.info(f"☁️ Target S3 key: {s3_key}")
            
            # Create a temporary file that gets deleted automatically
            with tempfile.NamedTemporaryFile(delete=False, suffix=f'.{file_extension}') as temp_file:
                temp_file_path = temp_file.name
                logger.info(f"📁 Temporary file created: {temp_file_path}")
            
            try:
                # Download from Telegram to temp file
                logger.info("📥 Downloading from Telegram via MTProto...")
                
                downloaded_file = await message.download_media(
                    file=temp_file_path,
                    progress_callback=self.progress_callback
                )
                
                if not downloaded_file or not os.path.exists(downloaded_file):
                    raise Exception("MTProto download failed or file not found")
                
                # Get file info
                file_size = os.path.getsize(downloaded_file)
                logger.info(f"✅ MTProto download complete: {file_size/1024/1024:.2f} MB")
                
                # Upload to S3
                logger.info("☁️ Uploading to S3...")
                
                with open(downloaded_file, 'rb') as f:
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
                                'source': 'mtproto_fastapi_direct',
                                'original_size': str(file_size),
                                'sender_name': download_info.get('sender_name', ''),
                                'sender_phone': download_info.get('sender_phone', ''),
                                'file_extension': file_extension
                            }
                        }
                    )
                
                # Verify upload
                self.s3_client.head_object(Bucket=self.s3_bucket, Key=s3_key)
                logger.info(f"✅ S3 upload verified: {s3_key}")
                
                return s3_key
                
            finally:
                # Always clean up temp file
                try:
                    if os.path.exists(temp_file_path):
                        os.remove(temp_file_path)
                        logger.info(f"🧹 Temp file cleaned up: {temp_file_path}")
                except Exception as cleanup_error:
                    logger.warning(f"⚠️ Failed to cleanup temp file: {cleanup_error}")
            
        except Exception as e:
            logger.error(f"❌ Direct S3 upload failed: {e}", exc_info=True)
            raise
    
    async def trigger_lambda_2(self, submission_id: str, volunteer_id: str, s3_key: str, download_info: dict):
        """Trigger Lambda 2 Video Processor with comprehensive logging"""
        
        function_name = os.getenv('VIDEO_PROCESSOR_FUNCTION_NAME')
        
        logger.info(f"🚀 Triggering Lambda function: {function_name}")
        
        if not function_name:
            error_msg = "VIDEO_PROCESSOR_FUNCTION_NAME not set in environment"
            logger.error(f"❌ {error_msg}")
            raise Exception(error_msg)
        
        try:
            # Prepare payload (same structure as Lambda 1)
            payload = {
                'submission_id': submission_id,
                'volunteer_id': volunteer_id,
                's3_key': s3_key,
                'video_title': f"MTProto Submission from {download_info.get('sender_name', volunteer_id)}"
            }
            
            logger.info(f"📄 Lambda payload: {json.dumps(payload, indent=2)}")
            
            # Trigger Lambda 2 asynchronously
            response = self.lambda_client.invoke(
                FunctionName=function_name,
                InvocationType='Event',  # Async
                Payload=json.dumps(payload)
            )
            
            logger.info(f"📊 Lambda response status: {response['StatusCode']}")
            
            # Check response
            if response['StatusCode'] != 202:
                error_msg = f"Lambda invoke failed with status: {response['StatusCode']}"
                logger.error(f"❌ {error_msg}")
                raise Exception(error_msg)
            
            logger.info(f"✅ Lambda 2 triggered successfully: {function_name}")
            
        except Exception as e:
            logger.error(f"❌ Failed to trigger Lambda 2: {e}", exc_info=True)
            raise
    
    async def send_user_notification(self, chat_id: str, message: str, message_type: str = "info"):
        """Send notification to user via Telegram Bot API with logging"""
        
        if not self.telegram_bot_token:
            logger.warning(f"⚠️ No bot token - cannot send notification to {chat_id}")
            print(f"⚠️ No bot token - cannot send notification to {chat_id}")
            return
        
        try:
            logger.info(f"📤 Sending {message_type} notification to {chat_id}")
            
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
                logger.info(f"✅ Notification sent successfully to {chat_id}: {message_type}")
                print(f"📤 Notification sent to {chat_id}: {message_type}")
            else:
                logger.error(f"❌ Failed to send notification: {response.status_code} - {response.text}")
                print(f"❌ Failed to send notification: {response.status_code}")
                
        except Exception as e:
            logger.error(f"❌ Notification error: {e}")
            print(f"❌ Notification error: {e}")
    
    async def update_submission_status(self, submission_id: str, status: str, reason: str = None):
        """Update submission status in database with logging"""
        try:
            logger.info(f"💾 Updating submission {submission_id} status to {status}")
            if reason:
                logger.info(f"📝 Reason: {reason}")
            
            await self.db.update_submission_status(submission_id, status, reason)
            
            logger.info(f"✅ Submission status updated successfully")
            print(f"💾 Updated submission {submission_id[:8]}... status: {status}")
            
        except Exception as e:
            logger.error(f"❌ Failed to update submission status: {e}")
            raise
    
    async def progress_callback(self, current, total):
        """Progress callback for downloads with logging"""
        if total > 0:
            percent = (current / total) * 100
            if int(percent) % 20 == 0 and percent > 0:  # Log every 20%
                mb_current = current / 1024 / 1024
                mb_total = total / 1024 / 1024
                
                progress_msg = f"📊 Download progress: {percent:.1f}% ({mb_current:.1f}/{mb_total:.1f} MB)"
                logger.info(progress_msg)
                print(progress_msg)
