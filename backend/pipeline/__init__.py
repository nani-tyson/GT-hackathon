"""
Pipeline Module
GroundTruth Hackathon | Automated Insight Engine

This package contains all pipeline step implementations:
- ingest.py: Data ingestion from CSV/JSON files (STRUCTURED)
- unstructured.py: Data extraction from TXT/PDF/MD files (UNSTRUCTURED)
- transform.py: Data transformation and cleaning
- kpis.py: KPI computation and metrics
- charts.py: Chart generation using Matplotlib
- insights.py: LLM-powered insights generation
- generate_pdf.py: PDF report generation
- generate_ppt.py: PowerPoint report generation
"""

from .ingest import ingest_data
from .unstructured import (
    process_unstructured_files,
    extract_structured_data_with_llm,
    extract_metrics_from_text,
    extract_entities_from_text
)
from .transform import transform_data
from .kpis import compute_kpis
from .charts import generate_charts
from .insights import run_llm_insights
from .generate_pdf import generate_pdf_report
from .generate_ppt import generate_ppt_report

__all__ = [
    "ingest_data",
    "process_unstructured_files",
    "extract_structured_data_with_llm",
    "extract_metrics_from_text",
    "extract_entities_from_text",
    "transform_data",
    "compute_kpis",
    "generate_charts",
    "run_llm_insights",
    "generate_pdf_report",
    "generate_ppt_report"
]

