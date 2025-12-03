"""
Automated Insight Engine - Simplified FastAPI Backend (No Inngest)
GroundTruth Hackathon | Problem Statement 1

This is a simplified version that runs the pipeline directly without Inngest.
Use this for testing when Inngest has issues.
"""

import os
import json
import uuid
import logging
from datetime import datetime
from typing import Optional, List
from pathlib import Path

from fastapi import FastAPI, UploadFile, File, HTTPException, Query, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import aiofiles

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Automated Insight Engine (Simple)",
    description="AI-powered data analytics and report generation system",
    version="1.0.0"
)

# CORS middleware
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
STATIC_DIR = BASE_DIR / "static"
METADATA_FILE = REPORTS_DIR / "reports.json"

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)
STATIC_DIR.mkdir(exist_ok=True)

# Mount static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


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


def run_pipeline(
    report_id: str,
    upload_id: str,
    report_format: str,
    report_title: str
):
    """Run the complete pipeline synchronously."""
    from pipeline.ingest import ingest_data
    from pipeline.transform import transform_data
    from pipeline.kpis import compute_kpis
    from pipeline.charts import generate_charts
    from pipeline.insights import run_llm_insights
    from pipeline.generate_pdf import generate_pdf_report
    from pipeline.generate_ppt import generate_ppt_report
    
    try:
        logger.info(f"Starting pipeline for report: {report_id}")
        
        # Update status
        metadata = load_reports_metadata()
        metadata[report_id]["status"] = "processing"
        save_reports_metadata(metadata)
        
        # Step 1: Ingest
        logger.info("Step 1: Ingesting data...")
        ingested = ingest_data(upload_id, str(DATA_DIR))
        
        # Step 2: Transform
        logger.info("Step 2: Transforming data...")
        transformed = transform_data(ingested["output_path"], str(DATA_DIR))
        
        # Step 3: Compute KPIs
        logger.info("Step 3: Computing KPIs...")
        kpis = compute_kpis(transformed["output_path"])
        
        # Step 4: Generate Charts
        logger.info("Step 4: Generating charts...")
        chart_dir = str(REPORTS_DIR / report_id)
        charts = generate_charts(transformed["output_path"], kpis, chart_dir)
        
        # Step 5: LLM Insights
        logger.info("Step 5: Generating insights...")
        insights = run_llm_insights(kpis, charts.get("summary", {}))
        
        # Step 6: Generate Report
        logger.info(f"Step 6: Generating {report_format.upper()} report...")
        if report_format == "pdf":
            result = generate_pdf_report(
                report_id, report_title, kpis, charts, insights, str(REPORTS_DIR)
            )
        else:
            result = generate_ppt_report(
                report_id, report_title, kpis, charts, insights, str(REPORTS_DIR)
            )
        
        # Update metadata
        metadata = load_reports_metadata()
        metadata[report_id]["status"] = "completed"
        metadata[report_id]["file_path"] = result["file_path"]
        metadata[report_id]["completed_at"] = datetime.now().isoformat()
        save_reports_metadata(metadata)
        
        logger.info(f"Pipeline completed for report: {report_id}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        metadata = load_reports_metadata()
        metadata[report_id]["status"] = "failed"
        metadata[report_id]["error"] = str(e)
        save_reports_metadata(metadata)


@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve the frontend UI."""
    index_file = STATIC_DIR / "index.html"
    if index_file.exists():
        async with aiofiles.open(index_file, "r", encoding="utf-8") as f:
            content = await f.read()
        return HTMLResponse(content=content)
    return HTMLResponse(content="<h1>TrendSpotter - Automated Insight Engine</h1><p>Frontend not found. Visit <a href='/docs'>/docs</a> for API.</p>")


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "Automated Insight Engine",
        "version": "1.0.0"
    }


@app.post("/upload")
async def upload_files(
    files: List[UploadFile] = File(..., description="CSV, JSON, TXT, PDF, or MD files")
):
    """Upload raw data files for processing."""
    upload_id = str(uuid.uuid4())
    upload_dir = DATA_DIR / upload_id
    upload_dir.mkdir(exist_ok=True)
    
    uploaded_files = []
    valid_extensions = ('.csv', '.json', '.txt', '.pdf', '.md', '.markdown')
    
    try:
        for file in files:
            if not file.filename.endswith(valid_extensions):
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid file type: {file.filename}. Supported: CSV, JSON, TXT, PDF, MD"
                )
            
            file_path = upload_dir / file.filename
            async with aiofiles.open(file_path, 'wb') as out_file:
                content = await file.read()
                await out_file.write(content)
            
            uploaded_files.append({
                "filename": file.filename,
                "size": len(content),
                "path": str(file_path)
            })
            
            logger.info(f"Uploaded: {file.filename} ({len(content)} bytes)")
        
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
    background_tasks: BackgroundTasks,
    upload_id: str = Query(..., description="Upload ID from /upload endpoint"),
    report_format: str = Query("pdf", description="Report format: 'pdf' or 'pptx'"),
    report_title: Optional[str] = Query("Performance Report", description="Report title")
):
    """Trigger report generation (runs in background)."""
    upload_dir = DATA_DIR / upload_id
    if not upload_dir.exists():
        raise HTTPException(status_code=404, detail=f"Upload ID not found: {upload_id}")
    
    if report_format not in ["pdf", "pptx"]:
        raise HTTPException(status_code=400, detail="Invalid format. Use 'pdf' or 'pptx'.")
    
    report_id = str(uuid.uuid4())
    
    # Initialize metadata
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
    
    # Run pipeline in background
    background_tasks.add_task(run_pipeline, report_id, upload_id, report_format, report_title)
    
    return {
        "status": "processing",
        "report_id": report_id,
        "message": "Report generation started. Check /status/{report_id} for progress.",
        "download_url": f"/download/{report_id}"
    }


@app.get("/status/{report_id}")
async def get_report_status(report_id: str):
    """Get report generation status."""
    metadata = load_reports_metadata()
    
    if report_id not in metadata:
        raise HTTPException(status_code=404, detail=f"Report not found: {report_id}")
    
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
    """Download generated report."""
    metadata = load_reports_metadata()
    
    if report_id not in metadata:
        raise HTTPException(status_code=404, detail=f"Report not found: {report_id}")
    
    report_info = metadata[report_id]
    
    if report_info["status"] != "completed":
        raise HTTPException(
            status_code=400,
            detail=f"Report not ready. Current status: {report_info['status']}"
        )
    
    file_path = report_info.get("file_path")
    if not file_path or not Path(file_path).exists():
        raise HTTPException(status_code=404, detail="Report file not found")
    
    media_type = "application/pdf" if report_info["format"] == "pdf" else \
                 "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    
    filename = f"{report_info['title'].replace(' ', '_')}_{report_id[:8]}.{report_info['format']}"
    
    return FileResponse(path=file_path, media_type=media_type, filename=filename)


@app.get("/reports")
async def list_reports():
    """List all reports."""
    metadata = load_reports_metadata()
    
    reports = []
    for report_id, info in metadata.items():
        reports.append({
            "report_id": report_id,
            "status": info["status"],
            "format": info["format"],
            "title": info["title"],
            "created_at": info["created_at"]
        })
    
    reports.sort(key=lambda x: x["created_at"], reverse=True)
    return {"reports": reports, "total": len(reports)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

