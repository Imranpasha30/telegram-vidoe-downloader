from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from telegram.client import TelegramService
import asyncio
import os
import sys
from datetime import datetime
import psycopg2
import boto3
import logging


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Global telegram service instance
telegram_service = None


# 🆕 LIFESPAN CONTEXT MANAGER (replaces @app.on_event)@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan - startup and shutdown"""
    global telegram_service
    
    # STARTUP
    logger.info("🚀 Starting Telegram MTProto Video Processor on Railway...")
    logger.info(f"📊 Environment: {os.getenv('ENVIRONMENT', 'development')}")
    logger.info(f"🐍 Python version: {sys.version}")
    logger.info(f"🚂 Railway Deployment: {os.getenv('ENVIRONMENT', 'local')}")
    
    # Create directories
    try:
        sessions_dir = os.path.join(os.getcwd(), "sessions")
        logs_dir = "/tmp/logs"
        os.makedirs(sessions_dir, exist_ok=True)
        os.makedirs(logs_dir, exist_ok=True)
        logger.info(f"✅ Created directories: {sessions_dir}, {logs_dir}")
    except Exception as e:
        logger.error(f"❌ Failed to create directories: {e}")
    
    try:
        # Initialize Telegram Service (reads config from env vars)
        telegram_service = TelegramService()
        logger.info("📱 Telegram service initialized")
        
        # Start telegram client in background
        asyncio.create_task(telegram_service.start())
        logger.info("🔄 Telegram client startup task created")
        
        logger.info("✅ Application startup completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Failed to start application: {e}")
        raise
    
    # Application runs here
    yield
    
    # SHUTDOWN
    logger.info("🛑 Shutting down Telegram MTProto Video Processor...")
    
    try:
        if telegram_service:
            await telegram_service.stop()
            logger.info("📱 Telegram service stopped")
        
        logger.info("✅ Application shutdown completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Error during shutdown: {e}")



# 🆕 CREATE APP WITH LIFESPAN
app = FastAPI(
    title="Telegram MTProto Video Processor",
    description="Railway-based video processing system with SQS queue",
    version="2.0.0",
    docs_url="/docs" if os.getenv("ENVIRONMENT") != "production" else None,
    redoc_url="/redoc" if os.getenv("ENVIRONMENT") != "production" else None,
    lifespan=lifespan  # 🆕 Use lifespan instead of @on_event
)


@app.get("/")
async def root():
    """Root endpoint with basic service information"""
    return {
        "service": "Telegram MTProto Video Processor",
        "version": "2.0.0",
        "status": "running",
        "timestamp": datetime.utcnow().isoformat(),
        "environment": os.getenv("ENVIRONMENT", "development"),
        "description": "Advanced Telegram video processing with SQS queue and S3 storage"
    }


@app.get("/health")
async def health_check():
    """Comprehensive health check endpoint for deployment validation"""
    
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "environment": os.getenv("ENVIRONMENT", "development"),
        "version": "2.0.0",
        "service": "telegram-mtproto-video-processor",
        "checks": {}
    }
    
    all_healthy = True
    
    # 1. Basic System Health
    try:
        health_status["checks"]["system"] = {
            "status": "healthy",
            "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
            "platform": sys.platform,
            "fastapi_running": True
        }
        logger.info("✅ System health check passed")
    except Exception as e:
        health_status["checks"]["system"] = {
            "status": "unhealthy",
            "error": str(e)
        }
        all_healthy = False
        logger.error(f"❌ System health check failed: {e}")
    
    # 2. Environment Variables Check (UPDATED for SQS)
    try:
        required_env_vars = [
            "TELEGRAM_API_ID",
            "TELEGRAM_API_HASH", 
            "TELEGRAM_PHONE",
            "DATABASE_URL",
            "S3_PROCESSING_BUCKET",  # 🆕 Updated
            "SQS_QUEUE_URL",  # 🆕 New for queue system
            "AWS_DEFAULT_REGION",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "API_VIDEO_KEY"  # 🆕 New
        ]
        
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        
        if missing_vars:
            health_status["checks"]["environment"] = {
                "status": "unhealthy",
                "missing_variables": missing_vars,
                "configured_count": len(required_env_vars) - len(missing_vars)
            }
            all_healthy = False
            logger.error(f"❌ Missing environment variables: {missing_vars}")
        else:
            health_status["checks"]["environment"] = {
                "status": "healthy",
                "configured_variables": len(required_env_vars),
                "aws_region": os.getenv("AWS_DEFAULT_REGION"),
                "queue_system": "SQS"  # 🆕 Indicate queue system
            }
            logger.info("✅ Environment variables check passed")
    except Exception as e:
        health_status["checks"]["environment"] = {
            "status": "unhealthy",
            "error": str(e)
        }
        all_healthy = False
        logger.error(f"❌ Environment check failed: {e}")
    
    # 3. Database Connection Check
    try:
        database_url = os.getenv("DATABASE_URL")
        if database_url:
            conn = psycopg2.connect(database_url, connect_timeout=5)
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
            conn.close()
            
            health_status["checks"]["database"] = {
                "status": "healthy",
                "connection": "successful",
                "type": "postgresql"
            }
            logger.info("✅ Database connection check passed")
        else:
            health_status["checks"]["database"] = {
                "status": "unhealthy",
                "error": "DATABASE_URL not configured"
            }
            all_healthy = False
            logger.error("❌ DATABASE_URL not configured")
            
    except Exception as e:
        health_status["checks"]["database"] = {
            "status": "unhealthy",
            "error": str(e)
        }
        all_healthy = False
        logger.error(f"❌ Database connection failed: {e}")
    
    # 4. AWS Services Check (UPDATED for SQS)
    try:
        aws_region = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")
        s3_bucket = os.getenv("S3_PROCESSING_BUCKET")  # 🆕 Updated
        sqs_queue_url = os.getenv("SQS_QUEUE_URL")  # 🆕 New
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        
        if not aws_access_key or not aws_secret_key:
            health_status["checks"]["aws"] = {
                "status": "unhealthy",
                "error": "AWS credentials not configured"
            }
            all_healthy = False
            logger.error("❌ AWS credentials not configured")
        elif s3_bucket and sqs_queue_url:
            # Test S3 access
            s3_client = boto3.client(
                's3',
                region_name=aws_region,
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key
            )
            s3_client.head_bucket(Bucket=s3_bucket)
            
            # Test SQS access
            sqs_client = boto3.client(
                'sqs',
                region_name=aws_region,
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key
            )
            sqs_client.get_queue_attributes(QueueUrl=sqs_queue_url, AttributeNames=['QueueArn'])
            
            health_status["checks"]["aws"] = {
                "status": "healthy",
                "s3_bucket": s3_bucket,
                "sqs_queue": sqs_queue_url[:50] + "...",  # 🆕 Show partial URL
                "region": aws_region,
                "services": ["s3", "sqs"],  # 🆕 Updated services
                "auth_method": "access_keys"
            }
            logger.info(f"✅ AWS services check passed - S3: {s3_bucket}, SQS configured")
        else:
            health_status["checks"]["aws"] = {
                "status": "unhealthy",
                "error": "S3_PROCESSING_BUCKET or SQS_QUEUE_URL not configured",
                "s3_configured": bool(s3_bucket),
                "sqs_configured": bool(sqs_queue_url)
            }
            all_healthy = False
            logger.error("❌ AWS services not fully configured")
            
    except Exception as e:
        health_status["checks"]["aws"] = {
            "status": "unhealthy",
            "error": str(e)
        }
        all_healthy = False
        logger.error(f"❌ AWS services check failed: {e}")
    
    # 5. Telegram MTProto Check
    try:
        api_id = os.getenv("TELEGRAM_API_ID")
        api_hash = os.getenv("TELEGRAM_API_HASH")
        phone = os.getenv("TELEGRAM_PHONE")
        
        if api_id and api_hash and phone:
            telegram_connected = telegram_service and telegram_service.is_connected() if telegram_service else False
            
            health_status["checks"]["telegram"] = {
                "status": "healthy",
                "api_configured": True,
                "phone_configured": bool(phone),
                "service_connected": telegram_connected,
                "api_id": api_id[:4] + "****"
            }
            logger.info("✅ Telegram configuration check passed")
        else:
            health_status["checks"]["telegram"] = {
                "status": "unhealthy",
                "error": "Telegram API credentials not fully configured",
                "api_configured": bool(api_id and api_hash),
                "phone_configured": bool(phone)
            }
            all_healthy = False
            logger.error("❌ Telegram API credentials incomplete")
            
    except Exception as e:
        health_status["checks"]["telegram"] = {
            "status": "unhealthy",
            "error": str(e)
        }
        all_healthy = False
        logger.error(f"❌ Telegram check failed: {e}")
    
    # 6. File System Check
    try:
        sessions_dir = os.path.join(os.getcwd(), "sessions")
        logs_dir = "/tmp/logs"
        
        os.makedirs(sessions_dir, exist_ok=True)
        os.makedirs(logs_dir, exist_ok=True)
        
        sessions_exists = os.path.exists(sessions_dir)
        logs_exists = os.path.exists(logs_dir)
        sessions_writable = os.access(sessions_dir, os.W_OK) if sessions_exists else False
        logs_writable = os.access(logs_dir, os.W_OK) if logs_exists else False
        
        # 🆕 Check for session file
        session_file_exists = os.path.exists(os.path.join(sessions_dir, "session_name.session"))
        
        if sessions_writable and logs_writable:
            health_status["checks"]["filesystem"] = {
                "status": "healthy",
                "sessions_dir": sessions_dir,
                "logs_dir": logs_dir,
                "sessions_writable": sessions_writable,
                "logs_writable": logs_writable,
                "session_file_exists": session_file_exists  # 🆕 Important check
            }
            logger.info("✅ Filesystem check passed")
        else:
            health_status["checks"]["filesystem"] = {
                "status": "unhealthy",
                "sessions_writable": sessions_writable,
                "logs_writable": logs_writable,
                "session_file_exists": session_file_exists
            }
            all_healthy = False
            logger.error("❌ Filesystem check failed")
            
    except Exception as e:
        health_status["checks"]["filesystem"] = {
            "status": "unhealthy",
            "error": str(e)
        }
        all_healthy = False
        logger.error(f"❌ Filesystem check failed: {e}")
    
    # 7. api.video Configuration Check (🆕 NEW)
    try:
        api_video_key = os.getenv("API_VIDEO_KEY")
        
        if api_video_key:
            health_status["checks"]["api_video"] = {
                "status": "healthy",
                "configured": True,
                "key_preview": api_video_key[:10] + "****"
            }
            logger.info("✅ api.video configuration check passed")
        else:
            health_status["checks"]["api_video"] = {
                "status": "unhealthy",
                "error": "API_VIDEO_KEY not configured"
            }
            all_healthy = False
            logger.error("❌ api.video key not configured")
            
    except Exception as e:
        health_status["checks"]["api_video"] = {
            "status": "unhealthy",
            "error": str(e)
        }
        all_healthy = False
    
    # Set overall status
    health_status["status"] = "healthy" if all_healthy else "unhealthy"
    
    # Add deployment metadata
    if os.getenv("RAILWAY_DEPLOYMENT_ID"):
        health_status["deployment"] = {
            "deployment_id": os.getenv("RAILWAY_DEPLOYMENT_ID"),
            "service_id": os.getenv("RAILWAY_SERVICE_ID"),
            "environment_id": os.getenv("RAILWAY_ENVIRONMENT_ID")
        }
    
    # Return appropriate HTTP status code
    status_code = 200 if all_healthy else 503
    
    if not all_healthy:
        logger.warning("⚠️ Health check failed - service is unhealthy")
    else:
        logger.info("✅ Health check passed - all systems operational")
    
    return JSONResponse(
        status_code=status_code,
        content=health_status
    )


@app.get("/health/simple")
async def simple_health_check():
    """Simple health check for basic monitoring"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "telegram-mtproto-video-processor",
        "version": "2.0.0"
    }


@app.get("/version")
async def version_info():
    """Version and deployment information"""
    return {
        "service": "Telegram MTProto Video Processor",
        "version": "2.0.0",
        "queue_system": "SQS",  # 🆕 Indicate queue system
        "environment": os.getenv("ENVIRONMENT", "development"),
        "railway_deployment_id": os.getenv("RAILWAY_DEPLOYMENT_ID", "unknown"),
        "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        "fastapi_version": "0.104.1"
    }


@app.get("/status")
async def telegram_status():
    """Telegram service connection status"""
    try:
        if telegram_service and telegram_service.is_connected():
            user_info = await telegram_service.get_me()
            return {
                "status": "connected",
                "user": f"{user_info.first_name} {user_info.last_name or ''}".strip(),
                "phone": user_info.phone,
                "username": getattr(user_info, 'username', None)
            }
        else:
            return {
                "status": "disconnected",
                "reason": "Telegram service not initialized or connection lost",
                "service_exists": telegram_service is not None
            }
    except Exception as e:
        logger.error(f"Error getting Telegram status: {e}")
        return {
            "status": "error",
            "error": str(e)
        }


@app.get("/metrics")
async def metrics():
    """Basic application metrics"""
    try:
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "telegram_connected": telegram_service.is_connected() if telegram_service else False,
            "environment": os.getenv("ENVIRONMENT", "development"),
            "queue_system": "SQS",
            "version": "2.0.0"
        }
    except Exception as e:
        return {"error": str(e), "timestamp": datetime.utcnow().isoformat()}


# Error Handlers
@app.exception_handler(404)
async def not_found_handler(request, exc):
    return JSONResponse(
        status_code=404,
        content={
            "error": "Not Found",
            "message": "The requested endpoint was not found",
            "timestamp": datetime.utcnow().isoformat()
        }
    )


@app.exception_handler(500)
async def internal_error_handler(request, exc):
    logger.error(f"Internal server error: {exc}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "message": "An internal error occurred",
            "timestamp": datetime.utcnow().isoformat()
        }
    )


# Production ASGI application
if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("PORT", 8000))
    
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=port,
        log_level="info",
        access_log=True
    )
