from fastapi import FastAPI
from telegram.client import TelegramService
import asyncio

app = FastAPI(title="Telegram Video Downloader", version="1.0.0")

# Global telegram service instance
telegram_service = None

@app.get("/")
async def root():
    return {"message": "Telegram Video Downloader is running!"}

@app.get("/status")
async def status():
    if telegram_service and telegram_service.is_connected():
        user_info = await telegram_service.get_me()
        return {
            "status": "connected",
            "user": f"{user_info.first_name} {user_info.last_name}",
            "phone": user_info.phone
        }
    return {"status": "disconnected"}

@app.on_event("startup")
async def startup_event():
    global telegram_service
    telegram_service = TelegramService()
    # Start telegram client as background task
    asyncio.create_task(telegram_service.start())

@app.on_event("shutdown")
async def shutdown_event():
    if telegram_service:
        await telegram_service.stop()
