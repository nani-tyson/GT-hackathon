"""
Inngest Client and Workflow Configuration
GroundTruth Hackathon | Automated Insight Engine

This module defines the Inngest client and the main report_pipeline workflow
that orchestrates the entire report generation process.
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import inngest

from pipeline.ingest import ingest_data
from pipeline.transform import transform_data
from pipeline.kpis import compute_kpis
from pipeline.charts import generate_charts
from pipeline.insights import run_llm_insights
from pipeline.generate_pdf import generate_pdf_report
from pipeline.generate_ppt import generate_ppt_report

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Initialize Inngest client
inngest_client = inngest.Inngest(
    app_id="automated-insight-engine",
    is_production=os.getenv("INNGEST_ENV", "development") == "production"
)


def update_report_status(
    reports_dir: str,
    report_id: str,
    status: str,
    file_path: str = None,
    error: str = None
) -> None:
    """Update report status in metadata file."""
    metadata_file = Path(reports_dir) / "reports.json"
    
    if metadata_file.exists():
        with open(metadata_file, "r") as f:
            metadata = json.load(f)
    else:
        metadata = {}
    
    if report_id in metadata:
        metadata[report_id]["status"] = status
        if file_path:
            metadata[report_id]["file_path"] = file_path
        if error:
            metadata[report_id]["error"] = error
        if status == "completed":
            metadata[report_id]["completed_at"] = datetime.now().isoformat()
    
    with open(metadata_file, "w") as f:
        json.dump(metadata, f, indent=2, default=str)


@inngest_client.create_function(
    fn_id="report-pipeline",
    trigger=inngest.TriggerEvent(event="report/generate"),
    retries=2
)
async def report_pipeline(
    ctx: inngest.Context,
    step: inngest.Step
) -> Dict[str, Any]:
    """
    Main report generation pipeline.
    
    This workflow orchestrates the entire report generation process:
    1. Ingest data from uploaded files
    2. Transform and clean the data
    3. Compute KPIs and metrics
    4. Generate charts and visualizations
    5. Run LLM for insights generation
    6. Build PDF or PPTX report
    7. Save and return output
    """
    event_data = ctx.event.data
    report_id = event_data["report_id"]
    upload_id = event_data["upload_id"]
    report_format = event_data["report_format"]
    report_title = event_data["report_title"]
    data_dir = event_data["data_dir"]
    reports_dir = event_data["reports_dir"]
    
    logger.info(f"Starting report pipeline for report_id: {report_id}")
    
    try:
        # Update status to processing
        update_report_status(reports_dir, report_id, "processing")
        
        # Step 1: Ingest Data
        ingested_data = await step.run(
            "ingest_data",
            lambda: ingest_data(
                upload_id=upload_id,
                data_dir=data_dir
            )
        )
        logger.info(f"Step 1 completed: Data ingested - {ingested_data['rows']} rows")
        
        # Step 2: Transform Data
        transformed_data = await step.run(
            "transform_data",
            lambda: transform_data(
                data_path=ingested_data["output_path"],
                data_dir=data_dir
            )
        )
        logger.info(f"Step 2 completed: Data transformed")
        
        # Step 3: Compute KPIs
        kpis = await step.run(
            "compute_kpis",
            lambda: compute_kpis(
                data_path=transformed_data["output_path"]
            )
        )
        logger.info(f"Step 3 completed: KPIs computed")
        
        # Step 4: Generate Charts
        charts = await step.run(
            "generate_charts",
            lambda: generate_charts(
                data_path=transformed_data["output_path"],
                kpis=kpis,
                output_dir=str(Path(reports_dir) / report_id)
            )
        )
        logger.info(f"Step 4 completed: {len(charts['chart_paths'])} charts generated")
        
        # Step 5: Run LLM Insights
        insights = await step.run(
            "run_llm_insights",
            lambda: run_llm_insights(
                kpis=kpis,
                charts_summary=charts.get("summary", {})
            )
        )
        logger.info(f"Step 5 completed: LLM insights generated")
        
        # Step 6: Build Report (PDF or PPTX)
        if report_format == "pdf":
            report_result = await step.run(
                "build_pdf_report",
                lambda: generate_pdf_report(
                    report_id=report_id,
                    report_title=report_title,
                    kpis=kpis,
                    charts=charts,
                    insights=insights,
                    output_dir=reports_dir
                )
            )
        else:
            report_result = await step.run(
                "build_ppt_report",
                lambda: generate_ppt_report(
                    report_id=report_id,
                    report_title=report_title,
                    kpis=kpis,
                    charts=charts,
                    insights=insights,
                    output_dir=reports_dir
                )
            )
        logger.info(f"Step 6 completed: {report_format.upper()} report generated")
        
        # Step 7: Save and Return Output
        final_result = await step.run(
            "save_and_return_output",
            lambda: save_output(
                report_id=report_id,
                report_path=report_result["file_path"],
                reports_dir=reports_dir
            )
        )
        
        logger.info(f"Pipeline completed successfully for report_id: {report_id}")
        
        return {
            "status": "completed",
            "report_id": report_id,
            "file_path": final_result["file_path"],
            "download_url": f"/download/{report_id}"
        }
        
    except Exception as e:
        logger.error(f"Pipeline failed for report_id {report_id}: {str(e)}")
        update_report_status(reports_dir, report_id, "failed", error=str(e))
        raise


def save_output(
    report_id: str,
    report_path: str,
    reports_dir: str
) -> Dict[str, Any]:
    """
    Save final output and update metadata.
    
    Args:
        report_id: Unique report identifier
        report_path: Path to generated report file
        reports_dir: Directory containing reports
        
    Returns:
        Dictionary with final output details
    """
    # Update report status to completed
    update_report_status(
        reports_dir=reports_dir,
        report_id=report_id,
        status="completed",
        file_path=report_path
    )
    
    logger.info(f"Report saved: {report_path}")
    
    return {
        "status": "completed",
        "report_id": report_id,
        "file_path": report_path
    }

