"""
Data Ingestion Pipeline Step
GroundTruth Hackathon | Automated Insight Engine

This module handles:
- Reading multiple CSV/JSON files (STRUCTURED data)
- Reading TXT/PDF/MD files (UNSTRUCTURED data)
- Merging data from different sources
- Normalizing column names
- Storing intermediate processed files
"""

import os
import re
import json
import logging
from pathlib import Path
from typing import Dict, Any, List

import pandas as pd

from .unstructured import (
    process_unstructured_files,
    merge_structured_and_unstructured
)

logger = logging.getLogger(__name__)

# Supported file types
STRUCTURED_EXTENSIONS = {'.csv', '.json'}
UNSTRUCTURED_EXTENSIONS = {'.txt', '.pdf', '.md', '.markdown'}


def normalize_column_name(col: str) -> str:
    """
    Normalize column names to snake_case.
    
    Args:
        col: Original column name
        
    Returns:
        Normalized column name in snake_case
    """
    # Convert to lowercase
    col = col.lower().strip()
    # Replace spaces and special characters with underscores
    col = re.sub(r'[\s\-\.]+', '_', col)
    # Remove any non-alphanumeric characters except underscores
    col = re.sub(r'[^a-z0-9_]', '', col)
    # Remove multiple consecutive underscores
    col = re.sub(r'_+', '_', col)
    # Remove leading/trailing underscores
    col = col.strip('_')
    return col


def read_csv_file(file_path: Path) -> pd.DataFrame:
    """
    Read a CSV file with automatic encoding detection.
    
    Args:
        file_path: Path to CSV file
        
    Returns:
        DataFrame with loaded data
    """
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            df = pd.read_csv(file_path, encoding=encoding)
            logger.info(f"Successfully read {file_path.name} with {encoding} encoding")
            return df
        except UnicodeDecodeError:
            continue
        except Exception as e:
            logger.warning(f"Error reading {file_path.name}: {str(e)}")
            continue
    
    raise ValueError(f"Could not read CSV file {file_path.name} with any supported encoding")


def read_json_file(file_path: Path) -> pd.DataFrame:
    """
    Read a JSON file and convert to DataFrame.
    
    Args:
        file_path: Path to JSON file
        
    Returns:
        DataFrame with loaded data
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Handle different JSON structures
    if isinstance(data, list):
        df = pd.DataFrame(data)
    elif isinstance(data, dict):
        # Check if it's a records-style dict
        if 'data' in data:
            df = pd.DataFrame(data['data'])
        else:
            df = pd.DataFrame([data])
    else:
        raise ValueError(f"Unsupported JSON structure in {file_path.name}")
    
    logger.info(f"Successfully read {file_path.name}")
    return df


def merge_dataframes(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Merge multiple DataFrames intelligently.
    
    Args:
        dfs: List of DataFrames to merge
        
    Returns:
        Merged DataFrame
    """
    if len(dfs) == 0:
        raise ValueError("No DataFrames to merge")
    
    if len(dfs) == 1:
        return dfs[0]
    
    # Try to find common columns for joining
    all_columns = [set(df.columns) for df in dfs]
    common_columns = set.intersection(*all_columns)
    
    # Identify potential key columns
    key_candidates = ['id', 'date', 'timestamp', 'campaign_id', 'location_id', 
                      'user_id', 'customer_id', 'region', 'category']
    
    merge_keys = [col for col in key_candidates if col in common_columns]
    
    if merge_keys:
        # Merge on common keys
        result = dfs[0]
        for df in dfs[1:]:
            result = pd.merge(result, df, on=merge_keys, how='outer', suffixes=('', '_dup'))
            # Remove duplicate columns
            result = result.loc[:, ~result.columns.str.endswith('_dup')]
        logger.info(f"Merged DataFrames on keys: {merge_keys}")
    else:
        # Concatenate if no common keys found
        result = pd.concat(dfs, ignore_index=True)
        logger.info("Concatenated DataFrames (no common keys found)")
    
    return result


def ingest_data(upload_id: str, data_dir: str) -> Dict[str, Any]:
    """
    Main data ingestion function.
    
    This function handles BOTH structured and unstructured data:
    
    STRUCTURED (CSV, JSON):
    1. Reads all CSV/JSON files from the upload directory
    2. Merges them into a single DataFrame
    3. Normalizes column names
    
    UNSTRUCTURED (TXT, PDF, MD):
    1. Reads text content from files
    2. Uses LLM/regex to extract structured metrics
    3. Converts to DataFrame format
    
    Finally:
    4. Merges structured + unstructured data
    5. Saves the intermediate result
    
    Args:
        upload_id: Unique identifier for the upload batch
        data_dir: Base directory containing uploaded data
        
    Returns:
        Dictionary with ingestion results including output path and stats
    """
    upload_path = Path(data_dir) / upload_id
    
    if not upload_path.exists():
        raise FileNotFoundError(f"Upload directory not found: {upload_path}")
    
    # ============================================
    # PART 1: Process STRUCTURED files (CSV, JSON)
    # ============================================
    csv_files = list(upload_path.glob("*.csv"))
    json_files = list(upload_path.glob("*.json"))
    structured_files = csv_files + json_files
    
    logger.info(f"Found {len(structured_files)} structured file(s) (CSV/JSON)")
    
    # Read structured files
    structured_dfs = []
    file_stats = []
    
    for file_path in csv_files:
        try:
            df = read_csv_file(file_path)
            structured_dfs.append(df)
            file_stats.append({
                "filename": file_path.name,
                "type": "csv",
                "category": "structured",
                "rows": len(df),
                "columns": len(df.columns)
            })
        except Exception as e:
            logger.error(f"Failed to read {file_path.name}: {str(e)}")
            raise
    
    for file_path in json_files:
        try:
            df = read_json_file(file_path)
            structured_dfs.append(df)
            file_stats.append({
                "filename": file_path.name,
                "type": "json",
                "category": "structured",
                "rows": len(df),
                "columns": len(df.columns)
            })
        except Exception as e:
            logger.error(f"Failed to read {file_path.name}: {str(e)}")
            raise
    
    # Merge structured DataFrames
    if structured_dfs:
        structured_df = merge_dataframes(structured_dfs)
        # Normalize column names
        original_columns = list(structured_df.columns)
        structured_df.columns = [normalize_column_name(col) for col in structured_df.columns]
        column_mapping = dict(zip(original_columns, structured_df.columns))
    else:
        structured_df = pd.DataFrame()
        column_mapping = {}
    
    # ================================================
    # PART 2: Process UNSTRUCTURED files (TXT, PDF, MD)
    # ================================================
    txt_files = list(upload_path.glob("*.txt"))
    pdf_files = list(upload_path.glob("*.pdf"))
    md_files = list(upload_path.glob("*.md")) + list(upload_path.glob("*.markdown"))
    unstructured_files = txt_files + pdf_files + md_files
    
    logger.info(f"Found {len(unstructured_files)} unstructured file(s) (TXT/PDF/MD)")
    
    unstructured_df = pd.DataFrame()
    unstructured_summary = {}
    
    if unstructured_files:
        try:
            unstructured_df, unstructured_summary = process_unstructured_files(unstructured_files)
            
            # Add file stats for unstructured files
            for file_path in unstructured_files:
                file_stats.append({
                    "filename": file_path.name,
                    "type": file_path.suffix.lstrip('.'),
                    "category": "unstructured",
                    "rows": 1,  # Each unstructured file becomes 1 row
                    "columns": len(unstructured_df.columns) if not unstructured_df.empty else 0
                })
        except Exception as e:
            logger.warning(f"Unstructured processing failed: {str(e)}")
            unstructured_summary = {"error": str(e)}
    
    # ============================================
    # PART 3: Merge structured + unstructured data
    # ============================================
    if structured_df.empty and unstructured_df.empty:
        raise ValueError(f"No processable files found in {upload_path}")
    
    merged_df = merge_structured_and_unstructured(structured_df, unstructured_df)
    
    # Normalize any new column names from unstructured data
    merged_df.columns = [normalize_column_name(col) for col in merged_df.columns]
    
    # Save intermediate result
    output_filename = f"ingested_{upload_id}.parquet"
    output_path = upload_path / output_filename
    merged_df.to_parquet(output_path, index=False)
    
    logger.info(f"Ingested data saved to {output_path}")
    logger.info(f"  - Structured rows: {len(structured_df)}")
    logger.info(f"  - Unstructured rows: {len(unstructured_df)}")
    logger.info(f"  - Total rows: {len(merged_df)}")
    
    return {
        "status": "success",
        "upload_id": upload_id,
        "output_path": str(output_path),
        "files_processed": file_stats,
        "rows": len(merged_df),
        "columns": len(merged_df.columns),
        "column_names": list(merged_df.columns),
        "column_mapping": column_mapping,
        "structured_files_count": len(structured_files),
        "unstructured_files_count": len(unstructured_files),
        "unstructured_summary": unstructured_summary
    }

