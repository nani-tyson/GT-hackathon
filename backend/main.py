"""
Automated Insight Engine - FastAPI Backend
GroundTruth Hackathon | Problem Statement 1

This module provides the main FastAPI application with endpoints for:
- Uploading raw data (CSV/JSON)
- Triggering report generation via Inngest workflow
- Downloading generated reports (PDF/PPTX)
"""

import os
import json
import uuid
import logging
from datetime import datetime
from typing import Optional, List
from pathlib import Path

from fastapi import FastAPI, UploadFile, File, HTTPException, Query, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import aiofiles

from inngest_client import inngest_client, report_pipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Automated Insight Engine",
    description="AI-powered data analytics and report generation system",
    version="1.0.0"
)

# CORS middleware for frontend integration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Directory paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
REPORTS_DIR = BASE_DIR / "reports"
METADATA_FILE = REPORTS_DIR / "reports.json"

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)


def load_reports_metadata() -> dict:
    """Load reports metadata from JSON file."""
    if METADATA_FILE.exists():
        with open(METADATA_FILE, "r") as f:
            return json.load(f)
    return {}


def save_reports_metadata(metadata: dict) -> None:
    """Save reports metadata to JSON file."""
    with open(METADATA_FILE, "w") as f:
        json.dump(metadata, f, indent=2, default=str)


@app.get("/")
async def root():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "Automated Insight Engine",
        "version": "1.0.0"
    }


@app.post("/upload")
async def upload_files(
    files: List[UploadFile] = File(..., description="CSV or JSON files to upload")
):
    """
    Upload raw data files (CSV/JSON) for processing.
    
    Returns:
        - upload_id: Unique identifier for this upload batch
        - files: List of uploaded file details
    """
    upload_id = str(uuid.uuid4())
    upload_dir = DATA_DIR / upload_id
    upload_dir.mkdir(exist_ok=True)
    
    uploaded_files = []
    
    try:
        for file in files:
            # Validate file type (structured + unstructured)
            valid_extensions = ('.csv', '.json', '.txt', '.pdf', '.md', '.markdown')
            if not file.filename.endswith(valid_extensions):
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid file type: {file.filename}. Supported: CSV, JSON, TXT, PDF, MD"
                )
            
            # Save file
            file_path = upload_dir / file.filename
            async with aiofiles.open(file_path, 'wb') as out_file:
                content = await file.read()
                await out_file.write(content)
            
            uploaded_files.append({
                "filename": file.filename,
                "size": len(content),
                "path": str(file_path)
            })
            
            logger.info(f"Uploaded file: {file.filename} ({len(content)} bytes)")
        
        return {
            "status": "success",
            "upload_id": upload_id,
            "files": uploaded_files,
            "message": f"Successfully uploaded {len(uploaded_files)} file(s)"
        }
        
    except Exception as e:
        logger.error(f"Upload failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/generate-report")
async def generate_report(
    upload_id: str = Query(..., description="Upload ID from /upload endpoint"),
    report_format: str = Query("pdf", description="Report format: 'pdf' or 'pptx'"),
    report_title: Optional[str] = Query("Performance Report", description="Custom report title")
):
    """
    Trigger report generation workflow via Inngest.
    
    This endpoint triggers the background report_pipeline workflow that:
    1. Ingests and validates data
    2. Transforms and cleans data
    3. Computes KPIs and metrics
    4. Generates charts and visualizations
    5. Runs LLM for insights generation
    6. Builds PDF or PPTX report
    7. Saves and returns output
    
    Returns:
        - report_id: Unique identifier for the generated report
        - execution_id: Inngest workflow execution ID
        - status: Current status of the report generation
    """
    # Validate upload_id
    upload_dir = DATA_DIR / upload_id
    if not upload_dir.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Upload ID not found: {upload_id}"
        )
    
    # Validate report format
    if report_format not in ["pdf", "pptx"]:
        raise HTTPException(
            status_code=400,
            detail="Invalid report format. Use 'pdf' or 'pptx'."
        )
    
    # Generate report ID
    report_id = str(uuid.uuid4())
    
    # Initialize report metadata
    metadata = load_reports_metadata()
    metadata[report_id] = {
        "upload_id": upload_id,
        "format": report_format,
        "title": report_title,
        "status": "pending",
        "created_at": datetime.now().isoformat(),
        "completed_at": None,
        "file_path": None,
        "error": None
    }
    save_reports_metadata(metadata)
    
    try:
        # Trigger Inngest workflow
        await inngest_client.send(
            inngest_client.create_event(
                name="report/generate",
                data={
                    "report_id": report_id,
                    "upload_id": upload_id,
                    "report_format": report_format,
                    "report_title": report_title,
                    "data_dir": str(DATA_DIR),
                    "reports_dir": str(REPORTS_DIR)
                }
            )
        )
        
        logger.info(f"Triggered report generation: {report_id}")
        
        return {
            "status": "processing",
            "report_id": report_id,
            "message": "Report generation started. Use /status/{report_id} to check progress.",
            "download_url": f"/download/{report_id}"
        }
        
    except Exception as e:
        logger.error(f"Failed to trigger workflow: {str(e)}")
        # Update metadata with error
        metadata[report_id]["status"] = "failed"
        metadata[report_id]["error"] = str(e)
        save_reports_metadata(metadata)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/status/{report_id}")
async def get_report_status(report_id: str):
    """
    Get the status of a report generation job.
    
    Returns:
        - status: pending, processing, completed, or failed
        - report_id: The report identifier
        - details: Additional information about the report
    """
    metadata = load_reports_metadata()
    
    if report_id not in metadata:
        raise HTTPException(
            status_code=404,
            detail=f"Report not found: {report_id}"
        )
    
    report_info = metadata[report_id]
    
    return {
        "report_id": report_id,
        "status": report_info["status"],
        "format": report_info["format"],
        "title": report_info["title"],
        "created_at": report_info["created_at"],
        "completed_at": report_info["completed_at"],
        "download_url": f"/download/{report_id}" if report_info["status"] == "completed" else None,
        "error": report_info.get("error")
    }


@app.get("/download/{report_id}")
async def download_report(report_id: str):
    """
    Download a generated report (PDF or PPTX).
    
    Returns:
        - FileResponse with the generated report
    """
    metadata = load_reports_metadata()
    
    if report_id not in metadata:
        raise HTTPException(
            status_code=404,
            detail=f"Report not found: {report_id}"
        )
    
    report_info = metadata[report_id]
    
    if report_info["status"] != "completed":
        raise HTTPException(
            status_code=400,
            detail=f"Report not ready. Current status: {report_info['status']}"
        )
    
    file_path = report_info.get("file_path")
    if not file_path or not Path(file_path).exists():
        raise HTTPException(
            status_code=404,
            detail="Report file not found on server"
        )
    
    # Determine media type
    media_type = "application/pdf" if report_info["format"] == "pdf" else \
                 "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    
    filename = f"{report_info['title'].replace(' ', '_')}_{report_id[:8]}.{report_info['format']}"
    
    return FileResponse(
        path=file_path,
        media_type=media_type,
        filename=filename
    )


@app.get("/reports")
async def list_reports():
    """
    List all generated reports with their status.
    
    Returns:
        - List of report metadata objects
    """
    metadata = load_reports_metadata()
    
    reports = []
    for report_id, info in metadata.items():
        reports.append({
            "report_id": report_id,
            "status": info["status"],
            "format": info["format"],
            "title": info["title"],
            "created_at": info["created_at"],
            "completed_at": info["completed_at"]
        })
    
    # Sort by creation date (newest first)
    reports.sort(key=lambda x: x["created_at"], reverse=True)
    
    return {"reports": reports, "total": len(reports)}


# Inngest serve endpoint
from inngest.fast_api import serve

serve(
    app,
    inngest_client,
    [report_pipeline]
)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)

