# telegram/database.py - New file for database operations
import os
import uuid
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import asyncio
import logging

logger = logging.getLogger(__name__)

def convert_database_url(database_url):
    """Convert SQLAlchemy URL to psycopg2 format (same as Lambda 1)"""
    if database_url.startswith('postgresql+asyncpg://'):
        return database_url.replace('postgresql+asyncpg://', 'postgresql://')
    elif database_url.startswith('postgresql://'):
        return database_url
    else:
        raise ValueError(f"Unsupported database URL format: {database_url}")

class DatabaseManager:
    def __init__(self):
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            raise Exception("DATABASE_URL environment variable not set")
        self.psycopg2_url = convert_database_url(self.database_url)
    
    async def check_volunteer_exists(self, chat_id: str) -> dict:
        """Check if volunteer exists (exactly like Lambda 1)"""
        try:
            conn = psycopg2.connect(self.psycopg2_url)
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT * FROM volunteers WHERE id = %s", (chat_id,))
                    volunteer = cur.fetchone()
                    
                    if volunteer:
                        logger.info(f"✅ Registered user found: {chat_id}")
                        return dict(volunteer)
                    else:
                        logger.info(f"❌ User not registered: {chat_id}")
                        return None
                        
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"Error checking volunteer: {str(e)}")
            return None
    
    async def complete_user_registration(self, chat_id: str, phone_number: str, first_name: str, last_name: str = None, username: str = None) -> bool:
        """Complete user registration (exactly like Lambda 1)"""
        try:
            conn = psycopg2.connect(self.psycopg2_url)
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # Same exact SQL as Lambda 1
                    cur.execute("""
                        INSERT INTO volunteers (id, first_name, last_name, username, phone_number, phone_verified, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET
                            phone_number = EXCLUDED.phone_number,
                            phone_verified = EXCLUDED.phone_verified,
                            updated_at = EXCLUDED.updated_at
                    """, (
                        chat_id,
                        first_name,
                        last_name,
                        username,
                        phone_number,
                        True,
                        datetime.utcnow(),
                        datetime.utcnow()
                    ))
                    
                    conn.commit()
                    logger.info(f"✅ Registration completed: {chat_id} | Phone: {phone_number}")
                    return True
                    
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"Registration failed for {chat_id}: {str(e)}")
            return False
    
    async def create_video_submission(self, volunteer_id: str, telegram_file_id: str) -> str:
        """Create video submission record (exactly like Lambda 1)"""
        try:
            conn = psycopg2.connect(self.psycopg2_url)
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # Check for duplicate submission (same as Lambda 1)
                    cur.execute(
                        "SELECT id FROM video_submissions WHERE telegram_file_id = %s", 
                        (telegram_file_id,)
                    )
                    existing = cur.fetchone()
                    
                    if existing:
                        logger.warning(f"⚠️ Duplicate submission: {telegram_file_id}")
                        return str(existing['id'])
                    
                    # Generate UUID and create submission (same as Lambda 1)
                    submission_uuid = str(uuid.uuid4())
                    logger.info(f"Generated UUID: {submission_uuid}")
                    
                    cur.execute("""
                        INSERT INTO video_submissions 
                        (id, volunteer_id, telegram_file_id, status, created_at, updated_at)
                        VALUES (%s, %s, %s, 'PROCESSING', %s, %s)
                        RETURNING id
                    """, (submission_uuid, volunteer_id, telegram_file_id, datetime.utcnow(), datetime.utcnow()))
                    
                    submission_record = cur.fetchone()
                    submission_id = submission_record['id']
                    conn.commit()
                    
                    logger.info(f"✅ Created submission: {submission_id} for user: {volunteer_id}")
                    return submission_id
                    
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"❌ Error creating submission: {str(e)}")
            raise



    async def update_submission_status(self, submission_id: str, status: str, reason: str = None):
        """Update submission status (add this method to DatabaseManager)"""
        try:
            conn = psycopg2.connect(self.psycopg2_url)
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE video_submissions 
                        SET status = %s, decline_reason = %s, updated_at = %s
                        WHERE id = %s
                    """, (status, reason, datetime.utcnow(), submission_id))
                    conn.commit()
                    
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"Failed to update submission status: {str(e)}")
            raise
