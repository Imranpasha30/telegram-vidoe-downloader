# Telegram MTProto Video Processor

Production-ready Telegram video processing service with AWS S3 and Lambda integration.

## Features

- 🚀 **FastAPI** web framework
- 📱 **Telegram MTProto** client integration  
- ☁️ **AWS S3** direct upload (no local storage)
- ⚡ **AWS Lambda** processing pipeline
- 🐳 **Docker** containerization
- 🔄 **GitHub Actions** automated deployment
- 🏥 **Health checks** and monitoring
- 📊 **Production logging**

## Architecture

1. **Telegram MTProto** → Download videos
2. **Direct S3 Upload** → No local storage
3. **Lambda Functions** → Video processing
4. **Database** → Track submissions
5. **User Notifications** → Real-time feedback

## Deployment

Automated deployment via GitHub Actions to AWS EC2 with Docker.

## Health Checks

- `GET /health` - Comprehensive health check
- `GET /status` - Telegram connection status  
- `GET /version` - Version and deployment info

## Environment

- **Production**: Deployed on AWS EC2 with IAM roles
- **Monitoring**: Health checks and structured logging
- **Security**: No credentials in code, IAM role-based access
-