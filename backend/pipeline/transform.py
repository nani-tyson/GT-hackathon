"""
Data Transformation Pipeline Step
GroundTruth Hackathon | Automated Insight Engine

This module handles:
- Parsing datetime columns
- Grouping by day/region/location
- Handling missing values
- Computing derived metrics
"""

import logging
from pathlib import Path
from typing import Dict, Any, List, Optional

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def detect_datetime_columns(df: pd.DataFrame) -> List[str]:
    """
    Detect columns that likely contain datetime values.
    
    Args:
        df: Input DataFrame
        
    Returns:
        List of column names that appear to be datetime
    """
    datetime_columns = []
    # Keywords that indicate datetime columns
    datetime_keywords = ['date', 'timestamp', 'created_at', 'updated_at', 
                         'start_date', 'end_date', 'datetime']
    # Keywords that should NOT be treated as datetime even if they parse
    exclude_keywords = ['spend', 'revenue', 'cost', 'price', 'amount', 'clicks',
                        'impressions', 'conversions', 'traffic', 'visitors', 
                        'temp', 'temperature', 'rate', 'ctr', 'cpc', 'cpm']
    
    for col in df.columns:
        col_lower = col.lower()
        
        # Skip columns that are clearly numeric metrics
        if any(kw in col_lower for kw in exclude_keywords):
            continue
        
        # Skip if already numeric type
        if pd.api.types.is_numeric_dtype(df[col]):
            continue
            
        # Check column name for datetime keywords
        if any(kw in col_lower for kw in datetime_keywords):
            datetime_columns.append(col)
            continue
        
        # For 'date' specifically (exact match or starts with)
        if col_lower == 'date' or col_lower.startswith('date_'):
            datetime_columns.append(col)
            continue
    
    return datetime_columns


def parse_datetime_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    Parse detected datetime columns.
    
    Args:
        df: Input DataFrame
        columns: List of columns to parse as datetime
        
    Returns:
        DataFrame with parsed datetime columns
    """
    df = df.copy()
    
    for col in columns:
        if col in df.columns:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                logger.info(f"Parsed datetime column: {col}")
            except Exception as e:
                logger.warning(f"Could not parse {col} as datetime: {str(e)}")
    
    return df


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle missing values intelligently based on column type.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with handled missing values
    """
    df = df.copy()
    
    for col in df.columns:
        missing_count = df[col].isna().sum()
        if missing_count == 0:
            continue
        
        missing_pct = missing_count / len(df) * 100
        logger.info(f"Column '{col}' has {missing_count} ({missing_pct:.1f}%) missing values")
        
        # Handle based on dtype
        if pd.api.types.is_numeric_dtype(df[col]):
            # Fill numeric columns with median
            median_val = df[col].median()
            df[col] = df[col].fillna(median_val)
            logger.info(f"Filled '{col}' with median: {median_val}")
        
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            # Forward fill datetime columns
            df[col] = df[col].ffill().bfill()
            logger.info(f"Forward/backward filled datetime column: {col}")
        
        else:
            # Fill categorical/string columns with mode or 'Unknown'
            mode_val = df[col].mode()
            if len(mode_val) > 0:
                df[col] = df[col].fillna(mode_val.iloc[0])
                logger.info(f"Filled '{col}' with mode: {mode_val.iloc[0]}")
            else:
                df[col] = df[col].fillna('Unknown')
                logger.info(f"Filled '{col}' with 'Unknown'")
    
    return df


def compute_derived_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute derived metrics based on available columns.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame with additional derived metrics
    """
    df = df.copy()
    
    # Ensure numeric columns are actually numeric
    numeric_cols = ['clicks', 'impressions', 'conversions', 'spend', 'revenue', 
                    'visits', 'foot_traffic', 'unique_visitors', 'engagements']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # CTR (Click-Through Rate) = Clicks / Impressions
    if 'clicks' in df.columns and 'impressions' in df.columns:
        try:
            df['ctr'] = (df['clicks'] / df['impressions'].replace(0, np.nan) * 100).round(4)
            logger.info("Computed CTR (Click-Through Rate)")
        except Exception as e:
            logger.warning(f"Could not compute CTR: {e}")
    
    # CPC (Cost Per Click) = Spend / Clicks
    if 'spend' in df.columns and 'clicks' in df.columns:
        try:
            df['cpc'] = (df['spend'] / df['clicks'].replace(0, np.nan)).round(4)
            logger.info("Computed CPC (Cost Per Click)")
        except Exception as e:
            logger.warning(f"Could not compute CPC: {e}")
    
    # CPM (Cost Per Mille) = (Spend / Impressions) * 1000
    if 'spend' in df.columns and 'impressions' in df.columns:
        try:
            df['cpm'] = (df['spend'] / df['impressions'].replace(0, np.nan) * 1000).round(4)
            logger.info("Computed CPM (Cost Per Mille)")
        except Exception as e:
            logger.warning(f"Could not compute CPM: {e}")
    
    # Conversion Rate = Conversions / Clicks
    if 'conversions' in df.columns and 'clicks' in df.columns:
        try:
            df['conversion_rate'] = (df['conversions'] / df['clicks'].replace(0, np.nan) * 100).round(4)
            logger.info("Computed Conversion Rate")
        except Exception as e:
            logger.warning(f"Could not compute Conversion Rate: {e}")
    
    # CPA (Cost Per Acquisition) = Spend / Conversions
    if 'spend' in df.columns and 'conversions' in df.columns:
        try:
            df['cpa'] = (df['spend'] / df['conversions'].replace(0, np.nan)).round(4)
            logger.info("Computed CPA (Cost Per Acquisition)")
        except Exception as e:
            logger.warning(f"Could not compute CPA: {e}")
    
    # ROAS (Return on Ad Spend) = Revenue / Spend
    if 'revenue' in df.columns and 'spend' in df.columns:
        try:
            df['roas'] = (df['revenue'] / df['spend'].replace(0, np.nan)).round(4)
            logger.info("Computed ROAS (Return on Ad Spend)")
        except Exception as e:
            logger.warning(f"Could not compute ROAS: {e}")
    
    # Engagement Rate = (Engagements / Impressions) * 100
    if 'engagements' in df.columns and 'impressions' in df.columns:
        try:
            df['engagement_rate'] = (df['engagements'] / df['impressions'].replace(0, np.nan) * 100).round(4)
            logger.info("Computed Engagement Rate")
        except Exception as e:
            logger.warning(f"Could not compute Engagement Rate: {e}")
    
    # Traffic metrics
    if 'visits' in df.columns and 'unique_visitors' in df.columns:
        try:
            df['pages_per_visit'] = (df['visits'] / df['unique_visitors'].replace(0, np.nan)).round(2)
            logger.info("Computed Pages Per Visit")
        except Exception as e:
            logger.warning(f"Could not compute Pages Per Visit: {e}")
    
    return df


def create_time_aggregations(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Create time-based aggregations for analysis.
    
    Args:
        df: Input DataFrame with datetime columns
        
    Returns:
        Dictionary of aggregated DataFrames
    """
    aggregations = {}
    
    # Find datetime column
    datetime_cols = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
    
    if not datetime_cols:
        logger.warning("No datetime columns found for time aggregations")
        return aggregations
    
    date_col = datetime_cols[0]
    
    # Identify numeric columns for aggregation
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    
    if not numeric_cols:
        logger.warning("No numeric columns found for aggregation")
        return aggregations
    
    # Daily aggregation
    try:
        df_daily = df.copy()
        df_daily['date'] = df_daily[date_col].dt.date
        daily_agg = df_daily.groupby('date')[numeric_cols].agg(['sum', 'mean']).reset_index()
        daily_agg.columns = ['_'.join(col).strip('_') for col in daily_agg.columns]
        aggregations['daily'] = daily_agg
        logger.info(f"Created daily aggregation: {len(daily_agg)} rows")
    except Exception as e:
        logger.warning(f"Could not create daily aggregation: {str(e)}")
    
    # Weekly aggregation
    try:
        df_weekly = df.copy()
        df_weekly['week'] = df_weekly[date_col].dt.isocalendar().week
        df_weekly['year'] = df_weekly[date_col].dt.year
        weekly_agg = df_weekly.groupby(['year', 'week'])[numeric_cols].agg(['sum', 'mean']).reset_index()
        weekly_agg.columns = ['_'.join(col).strip('_') for col in weekly_agg.columns]
        aggregations['weekly'] = weekly_agg
        logger.info(f"Created weekly aggregation: {len(weekly_agg)} rows")
    except Exception as e:
        logger.warning(f"Could not create weekly aggregation: {str(e)}")
    
    return aggregations


def create_categorical_aggregations(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Create categorical aggregations (by region, location, etc.).
    
    Args:
        df: Input DataFrame
        
    Returns:
        Dictionary of aggregated DataFrames
    """
    aggregations = {}
    
    # Identify categorical columns
    categorical_keywords = ['region', 'location', 'category', 'segment', 'channel', 
                           'campaign', 'source', 'medium', 'device', 'platform', 'country', 'city']
    
    categorical_cols = [col for col in df.columns 
                       if any(kw in col.lower() for kw in categorical_keywords)
                       and df[col].dtype == 'object']
    
    # Identify numeric columns for aggregation
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    
    if not numeric_cols:
        return aggregations
    
    for cat_col in categorical_cols:
        try:
            cat_agg = df.groupby(cat_col)[numeric_cols].agg(['sum', 'mean', 'count']).reset_index()
            cat_agg.columns = ['_'.join(col).strip('_') for col in cat_agg.columns]
            aggregations[f'by_{cat_col}'] = cat_agg
            logger.info(f"Created aggregation by {cat_col}: {len(cat_agg)} groups")
        except Exception as e:
            logger.warning(f"Could not create aggregation by {cat_col}: {str(e)}")
    
    return aggregations


def transform_data(data_path: str, data_dir: str) -> Dict[str, Any]:
    """
    Main data transformation function.
    
    This function:
    1. Parses datetime columns
    2. Handles missing values
    3. Computes derived metrics
    4. Creates time and categorical aggregations
    5. Saves the transformed result
    
    Args:
        data_path: Path to ingested data file
        data_dir: Base directory for data storage
        
    Returns:
        Dictionary with transformation results including output path and stats
    """
    # Load ingested data
    df = pd.read_parquet(data_path)
    logger.info(f"Loaded data: {len(df)} rows, {len(df.columns)} columns")
    
    original_shape = df.shape
    
    # Step 1: Detect and parse datetime columns
    datetime_cols = detect_datetime_columns(df)
    df = parse_datetime_columns(df, datetime_cols)
    
    # Step 2: Handle missing values
    missing_before = df.isna().sum().sum()
    df = handle_missing_values(df)
    missing_after = df.isna().sum().sum()
    
    # Step 3: Compute derived metrics
    df = compute_derived_metrics(df)
    
    # Step 4: Create aggregations
    time_aggs = create_time_aggregations(df)
    cat_aggs = create_categorical_aggregations(df)
    
    # Save transformed data
    output_dir = Path(data_path).parent
    output_path = output_dir / "transformed_data.parquet"
    df.to_parquet(output_path, index=False)
    
    # Save aggregations
    agg_dir = output_dir / "aggregations"
    agg_dir.mkdir(exist_ok=True)
    
    saved_aggregations = []
    for name, agg_df in {**time_aggs, **cat_aggs}.items():
        agg_path = agg_dir / f"{name}.parquet"
        agg_df.to_parquet(agg_path, index=False)
        saved_aggregations.append(str(agg_path))
    
    logger.info(f"Transformed data saved to {output_path}")
    
    return {
        "status": "success",
        "output_path": str(output_path),
        "original_shape": original_shape,
        "transformed_shape": df.shape,
        "datetime_columns_parsed": datetime_cols,
        "missing_values_handled": missing_before - missing_after,
        "derived_metrics_added": len(df.columns) - original_shape[1],
        "aggregations_created": saved_aggregations,
        "columns": list(df.columns)
    }

