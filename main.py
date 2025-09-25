from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
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

# Initialize FastAPI app
app = FastAPI(
    title="Telegram MTProto Video Processor",
    description="Production-ready Telegram video processing service with AWS S3 and Lambda integration",
    version="1.0.0",
    docs_url="/docs" if os.getenv("ENVIRONMENT") != "production" else None,
    redoc_url="/redoc" if os.getenv("ENVIRONMENT") != "production" else None
)

# Global telegram service instance
telegram_service = None

@app.get("/")
async def root():
    """Root endpoint with basic service information"""
    return {
        "service": "Telegram MTProto Video Processor",
        "version": "1.0.0",
        "status": "running",
        "timestamp": datetime.utcnow().isoformat(),
        "environment": os.getenv("ENVIRONMENT", "development"),
        "description": "Advanced Telegram video processing with direct S3 upload and Lambda integration"
    }

@app.get("/health")
async def health_check():
    """Comprehensive health check endpoint for deployment validation"""
    
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "environment": os.getenv("ENVIRONMENT", "development"),
        "version": "1.0.0",
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
    
    # 2. Environment Variables Check
    try:
        required_env_vars = [
            "TELEGRAM_API_ID",
            "TELEGRAM_API_HASH", 
            "TELEGRAM_PHONE",
            "DATABASE_URL",
            "S3_BUCKET_NAME",
            "AWS_DEFAULT_REGION"
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
                "aws_region": os.getenv("AWS_DEFAULT_REGION")
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
            # Quick connection test (don't keep connection open)
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
    
    # 4. AWS Services Check
    try:
        aws_region = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")
        s3_bucket = os.getenv("S3_BUCKET_NAME")
        
        if s3_bucket:
            # Test S3 access
            s3_client = boto3.client('s3', region_name=aws_region)
            s3_client.head_bucket(Bucket=s3_bucket)
            
            # Test Lambda access (just check client creation)
            lambda_client = boto3.client('lambda', region_name=aws_region)
            
            health_status["checks"]["aws"] = {
                "status": "healthy",
                "s3_bucket": s3_bucket,
                "region": aws_region,
                "services": ["s3", "lambda"],
                "iam_role": "attached"
            }
            logger.info(f"✅ AWS services check passed - S3: {s3_bucket}")
        else:
            health_status["checks"]["aws"] = {
                "status": "unhealthy",
                "error": "S3_BUCKET_NAME not configured"
            }
            all_healthy = False
            logger.error("❌ S3_BUCKET_NAME not configured")
            
    except Exception as e:
        health_status["checks"]["aws"] = {
            "status": "unhealthy",
            "error": str(e)
        }
        all_healthy = False
        logger.error(f"❌ AWS services check failed: {e}")
    
    # 5. Telegram MTProto Configuration Check
    try:
        api_id = os.getenv("TELEGRAM_API_ID")
        api_hash = os.getenv("TELEGRAM_API_HASH")
        phone = os.getenv("TELEGRAM_PHONE")
        
        if api_id and api_hash and phone:
            # Check if telegram service is running
            telegram_connected = telegram_service and telegram_service.is_connected() if telegram_service else False
            
            health_status["checks"]["telegram"] = {
                "status": "healthy",
                "api_configured": True,
                "phone_configured": bool(phone),
                "service_connected": telegram_connected,
                "api_id": api_id[:4] + "****"  # Partially mask for security
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
    
    # 6. File System Check (sessions and logs directories)
    try:
        sessions_dir = "/app/sessions"
        logs_dir = "/tmp/logs"
        
        sessions_exists = os.path.exists(sessions_dir)
        logs_exists = os.path.exists(logs_dir)
        sessions_writable = os.access(sessions_dir, os.W_OK) if sessions_exists else False
        logs_writable = os.access(logs_dir, os.W_OK) if logs_exists else False
        
        if sessions_writable and logs_writable:
            health_status["checks"]["filesystem"] = {
                "status": "healthy",
                "sessions_dir": sessions_dir,
                "logs_dir": logs_dir,
                "sessions_writable": sessions_writable,
                "logs_writable": logs_writable
            }
            logger.info("✅ Filesystem check passed")
        else:
            health_status["checks"]["filesystem"] = {
                "status": "unhealthy",
                "sessions_dir": sessions_dir,
                "logs_dir": logs_dir,
                "sessions_exists": sessions_exists,
                "logs_exists": logs_exists,
                "sessions_writable": sessions_writable,
                "logs_writable": logs_writable
            }
            all_healthy = False
            logger.error("❌ Filesystem check failed - directories not writable")
            
    except Exception as e:
        health_status["checks"]["filesystem"] = {
            "status": "unhealthy",
            "error": str(e)
        }
        all_healthy = False
        logger.error(f"❌ Filesystem check failed: {e}")
    
    # 7. Lambda Functions Check
    try:
        video_processor = os.getenv("VIDEO_PROCESSOR_FUNCTION_NAME")
        response_handler = os.getenv("RESPONSE_HANDLER_FUNCTION_NAME")
        
        if video_processor and response_handler:
            health_status["checks"]["lambda_functions"] = {
                "status": "healthy",
                "video_processor": video_processor,
                "response_handler": response_handler,
                "configured": True
            }
            logger.info("✅ Lambda functions configuration check passed")
        else:
            health_status["checks"]["lambda_functions"] = {
                "status": "unhealthy",
                "error": "Lambda function names not configured",
                "video_processor_set": bool(video_processor),
                "response_handler_set": bool(response_handler)
            }
            all_healthy = False
            logger.error("❌ Lambda function names not configured")
            
    except Exception as e:
        health_status["checks"]["lambda_functions"] = {
            "status": "unhealthy",
            "error": str(e)
        }
        all_healthy = False
        logger.error(f"❌ Lambda functions check failed: {e}")
    
    # Set overall status
    health_status["status"] = "healthy" if all_healthy else "unhealthy"
    
    # Add deployment metadata if available
    if os.getenv("GITHUB_SHA"):
        health_status["deployment"] = {
            "github_sha": os.getenv("GITHUB_SHA"),
            "github_ref": os.getenv("GITHUB_REF"),
            "deployed_at": os.getenv("DEPLOYED_AT"),
            "deployed_by": os.getenv("DEPLOYED_BY", "manual")
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
        "service": "telegram-mtproto-video-processor"
    }

@app.get("/version")
async def version_info():
    """Version and deployment information"""
    return {
        "service": "Telegram MTProto Video Processor",
        "version": "1.0.0",
        "environment": os.getenv("ENVIRONMENT", "development"),
        "github_sha": os.getenv("GITHUB_SHA", "unknown"),
        "github_ref": os.getenv("GITHUB_REF", "unknown"),
        "deployed_at": os.getenv("DEPLOYED_AT", "unknown"),
        "deployed_by": os.getenv("DEPLOYED_BY", "unknown"),
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
                "username": getattr(user_info, 'username', None),
                "connected_at": getattr(telegram_service, 'connected_at', None)
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
            "uptime_seconds": getattr(telegram_service, 'uptime_seconds', 0) if telegram_service else 0,
            "telegram_connected": telegram_service.is_connected() if telegram_service else False,
            "processed_videos": getattr(telegram_service, 'processed_videos_count', 0) if telegram_service else 0,
            "environment": os.getenv("ENVIRONMENT", "development")
        }
    except Exception as e:
        return {"error": str(e), "timestamp": datetime.utcnow().isoformat()}

# Startup and Shutdown Events
@app.on_event("startup")
async def startup_event():
    """Initialize services on application startup"""
    global telegram_service
    
    logger.info("🚀 Starting Telegram MTProto Video Processor...")
    logger.info(f"📊 Environment: {os.getenv('ENVIRONMENT', 'development')}")
    logger.info(f"🐍 Python version: {sys.version}")
    
    try:
        # Initialize Telegram Service
        telegram_service = TelegramService()
        logger.info("📱 Telegram service initialized")
        
        # Start telegram client as background task
        asyncio.create_task(telegram_service.start())
        logger.info("🔄 Telegram client startup task created")
        
        logger.info("✅ Application startup completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Failed to start application: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Clean shutdown of services"""
    logger.info("🛑 Shutting down Telegram MTProto Video Processor...")
    
    try:
        if telegram_service:
            await telegram_service.stop()
            logger.info("📱 Telegram service stopped")
        
        logger.info("✅ Application shutdown completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Error during shutdown: {e}")

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
    
    # Production configuration
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8000,
        log_level="info",
        access_log=True
    )
