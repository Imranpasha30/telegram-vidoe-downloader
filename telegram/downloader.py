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
        logging.FileHandler('/tmp/logs/telegram_mtproto.log'),
        logging.StreamHandler()
    ]
)


logger = logging.getLogger(__name__)


class VideoDownloader:
    def __init__(self):
        logger.info("🚀 Initializing VideoDownloader for Railway production with SQS queue")
        
        # Database manager
        try:
            self.db = DatabaseManager()
            logger.info("✅ Database manager initialized")
        except Exception as e:
            logger.error(f"❌ Database manager failed to initialize: {e}")
            raise
        
        # AWS configuration
        self.aws_region = os.getenv('AWS_DEFAULT_REGION', 'ap-south-1')
        self.s3_bucket = os.getenv('S3_PROCESSING_BUCKET')
        self.sqs_queue_url = os.getenv('SQS_QUEUE_URL')
        self.telegram_bot_token = os.getenv('TELEGRAM_BOT_TOKEN')
        
        # AWS credentials
        self.aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        
        logger.info(f"📍 AWS Region: {self.aws_region}")
        logger.info(f"🪣 S3 Bucket: {self.s3_bucket}")
        logger.info(f"📮 SQS Queue: {self.sqs_queue_url}")
        logger.info(f"🔑 AWS Access Key: {'Set' if self.aws_access_key_id else 'Not Set'}")
        logger.info(f"🤖 Bot Token: {'Set' if self.telegram_bot_token else 'Not Set'}")
        
        # Initialize AWS clients
        self.s3_client = None
        self.sqs_client = None
        
        if self.s3_bucket and self.sqs_queue_url and self.aws_access_key_id and self.aws_secret_access_key:
            try:
                # Create S3 client
                self.s3_client = boto3.client(
                    's3',
                    region_name=self.aws_region,
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key
                )
                
                # Create SQS client
                self.sqs_client = boto3.client(
                    'sqs',
                    region_name=self.aws_region,
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key
                )
                
                # Test connections
                self.s3_client.head_bucket(Bucket=self.s3_bucket)
                self.sqs_client.get_queue_attributes(QueueUrl=self.sqs_queue_url, AttributeNames=['QueueArn'])
                
                logger.info(f"✅ AWS clients initialized successfully")
                logger.info(f"✅ S3 bucket '{self.s3_bucket}' is accessible")
                logger.info(f"✅ SQS queue '{self.sqs_queue_url}' is accessible")
                
            except Exception as e:
                self.s3_client = None
                self.sqs_client = None
                logger.error(f"❌ AWS initialization failed: {e}")
        else:
            logger.error("❌ AWS configuration incomplete")
    
    async def download_video(self, message, document=None, download_info=None):
        """Process video - Upload to S3 and send to SQS queue"""
        
        submission_id = None
        volunteer_id = None
        start_time = datetime.utcnow()
        
        logger.info("=" * 60)
        logger.info("🎬 NEW VIDEO PROCESSING REQUEST (RAILWAY + SQS)")
        logger.info("=" * 60)
        
        try:
            # Extract information
            volunteer_id = str(download_info.get('sender_id'))
            telegram_file_id = str(document.id) if document else f"msg_{message.id}"
            file_size_mb = document.size/1024/1024 if document else 0
            
            logger.info(f"👤 User ID: {volunteer_id}")
            logger.info(f"📄 Telegram File ID: {telegram_file_id}")
            logger.info(f"📊 File Size: {file_size_mb:.2f} MB")
            
            print(f"\n🔍 PROCESSING VIDEO")
            print(f"👤 User ID: {volunteer_id}")
            print(f"📊 File Size: {file_size_mb:.2f} MB")
            
            # Send acknowledgment
            await self.send_user_notification(
                volunteer_id, 
                f"📥 **Video Received!**\n\nYour {file_size_mb:.2f} MB video is being uploaded to our processing queue.\nPlease wait...",
                "info"
            )
            
            # Pre-flight checks
            if not self.s3_client or not self.sqs_client or not self.s3_bucket or not self.sqs_queue_url:
                error_msg = "AWS S3/SQS not configured"
                logger.error(f"❌ {error_msg}")
                await self.send_user_notification(
                    volunteer_id,
                    "❌ **System Error**\n\nVideo processing service is temporarily unavailable.",
                    "error"
                )
                return {"status": "configuration_error", "message": error_msg}
            
            # User validation
            volunteer = await self.db.check_volunteer_exists(volunteer_id)
            
            if not volunteer:
                logger.info(f"🆕 User {volunteer_id} not registered - auto-registering")
                first_name = download_info.get('sender_name', 'Unknown User')
                registration_success = await self.db.complete_user_registration(
                    volunteer_id, 
                    download_info.get('sender_phone', '+0000000000'),
                    first_name.split(' ')[0],
                    ' '.join(first_name.split(' ')[1:]) if ' ' in first_name else None,
                    download_info.get('sender_username')
                )
                
                if registration_success:
                    volunteer = await self.db.check_volunteer_exists(volunteer_id)
                    await self.send_user_notification(
                        volunteer_id,
                        f"✅ **Welcome {first_name.split(' ')[0]}!**\n\nYou've been registered successfully. Processing your video...",
                        "success"
                    )
                else:
                    logger.error(f"❌ Auto-registration failed")
                    return {"status": "registration_failed"}
            
            logger.info(f"✅ User validated: {volunteer['first_name']}")
            
            # 🆕 CREATE SUBMISSION WITH DESCRIPTION AND DUPLICATE CHECK
            description = download_info.get('description')  # GET DESCRIPTION FROM MESSAGE
            submission_id = await self.db.create_video_submission(volunteer_id, telegram_file_id, description)
            
            # 🆕 CHECK IF SUBMISSION CREATION RETURNED None (duplicate with video URL)
            if not submission_id:
                error_msg = "Video already successfully submitted"
                logger.warning(f"⚠️ {error_msg}")
                await self.send_user_notification(
                    volunteer_id,
                    f"⚠️ **Video Already Submitted**\n\nThis video has already been successfully submitted and processed.\n\nIf you want to submit a new video, please send a different file.",
                    "warning"
                )
                return {
                    "status": "duplicate_video",
                    "message": "Video already submitted and processed"
                }
            
            logger.info(f"💾 Submission created/updated: {submission_id}")
            if description:
                logger.info(f"📝 Description: {description[:100]}{'...' if len(description) > 100 else ''}")
            
            # Upload to S3
            logger.info("🔍 Uploading to S3...")
            s3_key = await self.upload_to_s3(message, document, submission_id, volunteer_id, download_info)
            logger.info(f"☁️ S3 upload successful: {s3_key}")
            
            # Send job to SQS queue
            logger.info("📮 Sending job to SQS queue...")
            await self.send_to_sqs_queue(submission_id, volunteer_id, s3_key, download_info)
            logger.info(f"✅ Job sent to SQS queue successfully")
            
            # Success notification
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            await self.send_user_notification(
                volunteer_id,
                f"✅ **Upload Complete!**\n\nYour video has been uploaded and added to the processing queue.\n\n📊 **Details:**\n• Size: {file_size_mb:.2f} MB\n• Upload Time: {processing_time:.1f}s\n• Submission ID: `{submission_id[:8]}...`\n\nProcessing will begin shortly. You'll receive another message when it's complete!",
                "success"
            )
            
            total_duration = (datetime.utcnow() - start_time).total_seconds()
            logger.info("=" * 60)
            logger.info("✅ VIDEO UPLOAD COMPLETED SUCCESSFULLY")
            logger.info("=" * 60)
            logger.info(f"📊 Total Duration: {total_duration:.2f} seconds")
            logger.info(f"📄 Submission ID: {submission_id}")
            logger.info(f"☁️ S3 Key: {s3_key}")
            logger.info(f"📮 SQS Status: Queued for processing")
            logger.info("=" * 60)
            
            return {
                "status": "success",
                "submission_id": submission_id,
                "volunteer_id": volunteer_id,
                "s3_key": s3_key,
                "message": "Video uploaded to S3 and queued for processing"
            }
            
        except Exception as e:
            error_msg = f"Unexpected error: {e}"
            logger.error(f"❌ {error_msg}", exc_info=True)
            
            if submission_id:
                try:
                    await self.db.update_submission_status(submission_id, 'DECLINED', error_msg)
                except:
                    pass
            
            if volunteer_id:
                await self.send_user_notification(
                    volunteer_id,
                    "❌ **Unexpected Error**\n\nSomething went wrong. Please try again.",
                    "error"
                )
            
            return {"status": "error", "message": error_msg}
    
    async def upload_to_s3(self, message, document, submission_id: str, volunteer_id: str, download_info: dict):
        """Stream video directly from Telegram to S3 without saving to disk"""
        
        logger.info("📥 Starting MTProto to S3 STREAMING upload (no disk I/O)")
        
        try:
            # Get file extension
            file_extension = 'mp4'
            if document:
                for attr in document.attributes:
                    if hasattr(attr, 'file_name') and attr.file_name:
                        extension = attr.file_name.split('.')[-1] if '.' in attr.file_name else 'mp4'
                        file_extension = extension
                        break
            
            s3_key = f"queue_videos/{submission_id}.{file_extension}"
            logger.info(f"☁️ Target S3 key: {s3_key}")
            
            # 🚀 STREAMING APPROACH - Use multipart upload
            logger.info("🚀 Starting S3 multipart upload (streaming mode)")
            
            # Initialize multipart upload
            multipart_upload = self.s3_client.create_multipart_upload(
                Bucket=self.s3_bucket,
                Key=s3_key,
                ContentType='video/mp4',
                Metadata={
                    'submission_id': submission_id,
                    'volunteer_id': volunteer_id,
                    'uploaded_at': datetime.utcnow().isoformat(),
                    'source': 'railway_sqs_queue',
                    'sender_name': download_info.get('sender_name', '') or 'Unknown',
                    'sender_phone': download_info.get('sender_phone', '') or 'Unknown',
                    'file_extension': file_extension,
                    'streaming_upload': 'true'
                }
            )
            
            upload_id = multipart_upload['UploadId']
            logger.info(f"📤 Multipart upload started: {upload_id}")
            
            parts = []
            part_number = 1
            chunk_size = 5 * 1024 * 1024  # 5 MB chunks (S3 minimum for multipart)
            buffer = bytearray()
            total_uploaded = 0
            file_size = document.size if document else 0
            
            try:
                # Download from Telegram in chunks and upload directly to S3
                logger.info("⚡ Streaming from Telegram → S3 (no disk)")
                
                async for chunk in message.client.iter_download(message.media, chunk_size=chunk_size):
                    buffer.extend(chunk)
                    total_uploaded += len(chunk)
                    
                    # When buffer reaches chunk_size, upload to S3
                    if len(buffer) >= chunk_size:
                        # Upload this part to S3
                        part_response = self.s3_client.upload_part(
                            Bucket=self.s3_bucket,
                            Key=s3_key,
                            PartNumber=part_number,
                            UploadId=upload_id,
                            Body=bytes(buffer)
                        )
                        
                        parts.append({
                            'ETag': part_response['ETag'],
                            'PartNumber': part_number
                        })
                        
                        # Log progress
                        if file_size > 0:
                            percent = (total_uploaded / file_size) * 100
                            logger.info(f"📊 Streaming progress: Part {part_number} uploaded | {percent:.1f}% ({total_uploaded/1024/1024:.1f}/{file_size/1024/1024:.1f} MB)")
                        
                        part_number += 1
                        buffer.clear()  # Clear buffer for next chunk
                
                # Upload remaining data in buffer (last part)
                if len(buffer) > 0:
                    part_response = self.s3_client.upload_part(
                        Bucket=self.s3_bucket,
                        Key=s3_key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=bytes(buffer)
                    )
                    
                    parts.append({
                        'ETag': part_response['ETag'],
                        'PartNumber': part_number
                    })
                    
                    logger.info(f"📊 Final part {part_number} uploaded | 100.0%")
                
                # Complete multipart upload
                self.s3_client.complete_multipart_upload(
                    Bucket=self.s3_bucket,
                    Key=s3_key,
                    UploadId=upload_id,
                    MultipartUpload={'Parts': parts}
                )
                
                logger.info(f"✅ S3 streaming upload complete: {s3_key}")
                logger.info(f"📊 Total uploaded: {total_uploaded/1024/1024:.2f} MB in {part_number} parts")
                
                # Verify upload
                self.s3_client.head_object(Bucket=self.s3_bucket, Key=s3_key)
                logger.info(f"✅ S3 upload verified")
                
                return s3_key
                
            except Exception as e:
                # Abort multipart upload on error
                logger.error(f"❌ Streaming upload failed, aborting: {e}")
                try:
                    self.s3_client.abort_multipart_upload(
                        Bucket=self.s3_bucket,
                        Key=s3_key,
                        UploadId=upload_id
                    )
                    logger.info("🗑️ Multipart upload aborted")
                except:
                    pass
                raise
                
        except Exception as e:
            logger.error(f"❌ S3 streaming upload failed: {e}", exc_info=True)
            raise

        
    async def send_to_sqs_queue(self, submission_id: str, volunteer_id: str, s3_key: str, download_info: dict):
        """Send processing job to SQS queue with description"""
        
        try:
            message_body = {
                'submission_id': submission_id,
                'volunteer_id': volunteer_id,
                's3_key': s3_key,
                's3_bucket': self.s3_bucket,
                'video_title': f"News Video from {download_info.get('sender_name', volunteer_id)}",
                'description': download_info.get('description'),  # 🆕 ADD DESCRIPTION TO QUEUE MESSAGE
                'sender_name': download_info.get('sender_name', ''),
                'sender_phone': download_info.get('sender_phone', ''),
                'timestamp': datetime.utcnow().isoformat(),
                'source': 'railway_telegram_receiver'
            }
            
            logger.info(f"📮 SQS Message: {json.dumps(message_body, indent=2)}")
            
            response = self.sqs_client.send_message(
                QueueUrl=self.sqs_queue_url,
                MessageBody=json.dumps(message_body),
                MessageAttributes={
                    'submission_id': {
                        'StringValue': submission_id,
                        'DataType': 'String'
                    },
                    'volunteer_id': {
                        'StringValue': volunteer_id,
                        'DataType': 'String'
                    }
                }
            )
            
            logger.info(f"✅ SQS Message sent: {response['MessageId']}")
            
        except Exception as e:
            logger.error(f"❌ Failed to send to SQS: {e}", exc_info=True)
            raise
    
    async def send_user_notification(self, chat_id: str, message: str, message_type: str = "info"):
        """Send notification via Telegram Bot API"""
        
        if not self.telegram_bot_token:
            logger.warning(f"⚠️ No bot token")
            return
        
        try:
            send_url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"
            
            emoji_map = {"info": "ℹ️", "success": "✅", "error": "❌", "warning": "⚠️"}
            formatted_message = f"{emoji_map.get(message_type, 'ℹ️')} {message}"
            
            payload = {
                "chat_id": chat_id,
                "text": formatted_message,
                "parse_mode": "Markdown",
                "disable_web_page_preview": True
            }
            
            response = requests.post(send_url, json=payload, timeout=10)
            
            if response.status_code == 200:
                logger.info(f"✅ Notification sent to {chat_id}")
            else:
                logger.error(f"❌ Failed to send notification: {response.status_code}")
                
        except Exception as e:
            logger.error(f"❌ Notification error: {e}")
    
    async def progress_callback(self, current, total):
        """Progress callback for downloads"""
        if total > 0:
            percent = (current / total) * 100
            if int(percent) % 20 == 0 and percent > 0:
                mb_current = current / 1024 / 1024
                mb_total = total / 1024 / 1024
                logger.info(f"📊 Download progress: {percent:.1f}% ({mb_current:.1f}/{mb_total:.1f} MB)")
