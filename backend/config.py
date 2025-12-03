"""
Configuration Module
GroundTruth Hackathon | Automated Insight Engine

This module contains all configuration settings for the application.
"""

import os
from pathlib import Path
from typing import Optional

# Base paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
REPORTS_DIR = BASE_DIR / "reports"
TEMPLATES_DIR = BASE_DIR / "templates"

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)
TEMPLATES_DIR.mkdir(exist_ok=True)


class Settings:
    """Application settings."""
    
    # API Settings
    APP_NAME: str = "Automated Insight Engine"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"
    
    # Server Settings
    HOST: str = os.getenv("HOST", "0.0.0.0")
    PORT: int = int(os.getenv("PORT", "8000"))
    
    # OpenAI Settings
    OPENAI_API_KEY: Optional[str] = os.getenv("OPENAI_API_KEY")
    OPENAI_MODEL: str = os.getenv("OPENAI_MODEL", "gpt-4o")
    OPENAI_MAX_TOKENS: int = int(os.getenv("OPENAI_MAX_TOKENS", "1500"))
    OPENAI_TEMPERATURE: float = float(os.getenv("OPENAI_TEMPERATURE", "0.7"))
    
    # Inngest Settings
    INNGEST_APP_ID: str = "automated-insight-engine"
    INNGEST_ENV: str = os.getenv("INNGEST_ENV", "development")
    INNGEST_SIGNING_KEY: Optional[str] = os.getenv("INNGEST_SIGNING_KEY")
    
    # File Settings
    MAX_UPLOAD_SIZE_MB: int = int(os.getenv("MAX_UPLOAD_SIZE_MB", "50"))
    ALLOWED_EXTENSIONS: set = {".csv", ".json"}
    
    # Report Settings
    DEFAULT_REPORT_FORMAT: str = "pdf"
    DEFAULT_REPORT_TITLE: str = "Performance Report"
    
    # Chart Settings
    CHART_DPI: int = 150
    CHART_STYLE: str = "seaborn-v0_8-whitegrid"
    
    @classmethod
    def validate(cls) -> None:
        """Validate required settings."""
        warnings = []
        
        if not cls.OPENAI_API_KEY:
            warnings.append("OPENAI_API_KEY not set. LLM insights will use fallback generation.")
        
        if warnings:
            for warning in warnings:
                print(f"⚠️  Warning: {warning}")


# Create settings instance
settings = Settings()


# Environment variable template for .env file
ENV_TEMPLATE = """
# Automated Insight Engine Configuration
# Copy this file to .env and fill in your values

# OpenAI Configuration
OPENAI_API_KEY=your-openai-api-key-here
OPENAI_MODEL=gpt-4o
OPENAI_MAX_TOKENS=1500
OPENAI_TEMPERATURE=0.7

# Server Configuration
HOST=0.0.0.0
PORT=8000
DEBUG=false

# Inngest Configuration
INNGEST_ENV=development
INNGEST_SIGNING_KEY=

# File Upload Settings
MAX_UPLOAD_SIZE_MB=50
""".strip()

