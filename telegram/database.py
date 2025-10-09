import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
import uuid


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self):
        """Initialize database manager with PostgreSQL connection"""
        self.database_url = os.getenv('DATABASE_URL')
        
        if not self.database_url:
            raise Exception("DATABASE_URL environment variable not set")
        
        # Convert asyncpg URL to psycopg2 format if needed
        self.psycopg2_url = self.convert_database_url(self.database_url)
        
        logger.info("Database manager initialized with PostgreSQL")
    
    def convert_database_url(self, database_url):
        """Convert SQLAlchemy URL to psycopg2 format"""
        if database_url.startswith('postgresql+asyncpg://'):
            return database_url.replace('postgresql+asyncpg://', 'postgresql://')
        elif database_url.startswith('postgresql://'):
            return database_url
        else:
            raise ValueError(f"Unsupported database URL format: {database_url}")
    
    async def check_volunteer_exists(self, volunteer_id: str):
        """Check if volunteer exists in database"""
        try:
            conn = psycopg2.connect(self.psycopg2_url)
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT id, first_name, last_name, username, phone_number
                        FROM volunteers 
                        WHERE id = %s
                    """, (volunteer_id,))
                    
                    volunteer = cur.fetchone()
                    
                    if volunteer:
                        logger.info(f"✅ Registered user found: {volunteer_id}")
                        return dict(volunteer)
                    else:
                        logger.info(f"❌ User not registered: {volunteer_id}")
                        return None
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"Database query failed: {str(e)}")
            return None
    
    async def complete_user_registration(self, volunteer_id: str, phone: str, first_name: str = None, last_name: str = None, username: str = None):
        """Complete user registration"""
        try:
            conn = psycopg2.connect(self.psycopg2_url)
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO volunteers (id, phone_number, first_name, last_name, username, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (id) 
                        DO UPDATE SET 
                            phone_number = EXCLUDED.phone_number,
                            first_name = EXCLUDED.first_name,
                            last_name = EXCLUDED.last_name,
                            username = EXCLUDED.username,
                            updated_at = EXCLUDED.updated_at
                    """, (
                        volunteer_id,
                        phone,
                        first_name or 'Unknown',
                        last_name or '',
                        username,
                        datetime.utcnow(),
                        datetime.utcnow()
                    ))
                    
                    conn.commit()
                    logger.info(f"✅ Registration completed: {volunteer_id} | Phone: {phone}")
                    return True
                    
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"Registration failed: {str(e)}")
            return False
    
    async def create_video_submission(self, volunteer_id: str, telegram_file_id: str, description: str = None):
        """Create new video submission OR update existing if duplicate - SMART RETRY LOGIC"""
        try:
            conn = psycopg2.connect(self.psycopg2_url)
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # 🆕 CHECK IF TELEGRAM FILE ID ALREADY EXISTS
                    cur.execute("""
                        SELECT id, status, video_platform_url, created_at
                        FROM video_submissions 
                        WHERE telegram_file_id = %s
                    """, (telegram_file_id,))
                    
                    existing = cur.fetchone()
                    
                    if existing:
                        existing_id = existing['id']
                        existing_status = existing['status']
                        has_video_url = existing['video_platform_url'] is not None
                        
                        # 🆕 SMART RETRY LOGIC
                        if has_video_url and existing_status == 'PENDING_REVIEW':
                            # Video successfully processed before
                            logger.warning(f"⚠️ Video already successfully submitted: {existing_id}")
                            logger.info(f"📹 Existing submission has video URL - skipping duplicate")
                            return None  # Return None to indicate duplicate
                        
                        elif existing_status in ['PROCESSING', 'DECLINED']:
                            # Previous attempt failed or still processing - RETRY!
                            logger.info(f"🔄 Retrying previous failed/incomplete submission: {existing_id}")
                            logger.info(f"   Previous status: {existing_status}")
                            
                            # Update existing record with new description and reset status
                            cur.execute("""
                                UPDATE video_submissions 
                                SET 
                                    status = 'PROCESSING',
                                    description = COALESCE(%s, description),
                                    decline_reason = NULL,
                                    updated_at = %s
                                WHERE id = %s
                                RETURNING id
                            """, (description, datetime.utcnow(), existing_id))
                            
                            conn.commit()
                            logger.info(f"✅ Updated existing submission for retry: {existing_id}")
                            if description:
                                logger.info(f"📝 Description updated: {description[:100]}{'...' if len(description) > 100 else ''}")
                            
                            return existing_id  # Return existing ID for retry
                        
                        else:
                            # Other status - treat as duplicate
                            logger.warning(f"⚠️ Video already exists with status: {existing_status}")
                            return None
                    
                    # 🆕 NO EXISTING RECORD - CREATE NEW
                    submission_id = str(uuid.uuid4())
                    logger.info(f"Generated UUID: {submission_id}")
                    
                    cur.execute("""
                        INSERT INTO video_submissions 
                        (id, volunteer_id, telegram_file_id, status, description, created_at, updated_at)
                        VALUES (%s, %s, %s, 'PROCESSING', %s, %s, %s)
                        RETURNING id
                    """, (
                        submission_id,
                        volunteer_id,
                        telegram_file_id,
                        description,
                        datetime.utcnow(),
                        datetime.utcnow()
                    ))
                    
                    result = cur.fetchone()
                    conn.commit()
                    
                    if result:
                        logger.info(f"✅ Created NEW submission: {submission_id} for user: {volunteer_id}")
                        if description:
                            logger.info(f"📝 Description saved: {description[:100]}{'...' if len(description) > 100 else ''}")
                        return submission_id
                    else:
                        logger.error("Failed to create submission")
                        return None
                        
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"Failed to create/update video submission: {str(e)}")
            return None
    
    async def update_submission_status(self, submission_id: str, status: str, reason: str = None):
        """Update submission status"""
        try:
            conn = psycopg2.connect(self.psycopg2_url)
            try:
                with conn.cursor() as cur:
                    # Map FAILED to DECLINED (match database enum)
                    db_status = 'DECLINED' if status == 'FAILED' else status
                    
                    cur.execute("""
                        UPDATE video_submissions 
                        SET status = %s, decline_reason = %s, updated_at = %s
                        WHERE id = %s
                    """, (db_status, reason, datetime.utcnow(), submission_id))
                    conn.commit()
                    
                    logger.info(f"✅ Updated submission {submission_id} to status: {db_status}")
                    
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"Failed to update submission status: {str(e)}")
            raise
    
    async def get_submission(self, submission_id: str):
        """Get submission details"""
        try:
            conn = psycopg2.connect(self.psycopg2_url)
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT vs.*, v.first_name, v.last_name, v.username 
                        FROM video_submissions vs
                        JOIN volunteers v ON vs.volunteer_id = v.id
                        WHERE vs.id = %s
                    """, (submission_id,))
                    
                    submission = cur.fetchone()
                    return dict(submission) if submission else None
                    
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"Failed to get submission: {str(e)}")
            return None
