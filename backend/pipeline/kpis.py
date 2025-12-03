"""
KPI Computation Pipeline Step
GroundTruth Hackathon | Automated Insight Engine

This module handles:
- Computing CTR, CPC, Conversion Rate
- Identifying top performing segments
- Traffic vs weather correlation
- Anomaly detection
"""

import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd
import numpy as np
from scipy import stats

logger = logging.getLogger(__name__)


def compute_basic_kpis(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Compute basic KPIs from the data.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Dictionary of computed KPIs
    """
    kpis = {}
    
    # Total metrics
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    
    for col in numeric_cols:
        kpis[f'total_{col}'] = float(df[col].sum())
        kpis[f'avg_{col}'] = float(df[col].mean())
        kpis[f'max_{col}'] = float(df[col].max())
        kpis[f'min_{col}'] = float(df[col].min())
    
    # Advertising KPIs
    if 'impressions' in df.columns:
        total_impressions = df['impressions'].sum()
        kpis['total_impressions'] = int(total_impressions)
        
        if 'clicks' in df.columns:
            total_clicks = df['clicks'].sum()
            kpis['total_clicks'] = int(total_clicks)
            kpis['overall_ctr'] = round(total_clicks / total_impressions * 100, 4) if total_impressions > 0 else 0
    
    if 'spend' in df.columns:
        total_spend = df['spend'].sum()
        kpis['total_spend'] = round(float(total_spend), 2)
        
        if 'clicks' in df.columns:
            total_clicks = df['clicks'].sum()
            kpis['overall_cpc'] = round(total_spend / total_clicks, 4) if total_clicks > 0 else 0
        
        if 'impressions' in df.columns:
            total_impressions = df['impressions'].sum()
            kpis['overall_cpm'] = round(total_spend / total_impressions * 1000, 4) if total_impressions > 0 else 0
    
    if 'conversions' in df.columns:
        total_conversions = df['conversions'].sum()
        kpis['total_conversions'] = int(total_conversions)
        
        if 'clicks' in df.columns:
            total_clicks = df['clicks'].sum()
            kpis['overall_conversion_rate'] = round(total_conversions / total_clicks * 100, 4) if total_clicks > 0 else 0
        
        if 'spend' in df.columns:
            total_spend = df['spend'].sum()
            kpis['overall_cpa'] = round(total_spend / total_conversions, 4) if total_conversions > 0 else 0
    
    if 'revenue' in df.columns:
        total_revenue = df['revenue'].sum()
        kpis['total_revenue'] = round(float(total_revenue), 2)
        
        if 'spend' in df.columns:
            total_spend = df['spend'].sum()
            kpis['overall_roas'] = round(total_revenue / total_spend, 4) if total_spend > 0 else 0
    
    # Traffic KPIs
    if 'visits' in df.columns:
        kpis['total_visits'] = int(df['visits'].sum())
    
    if 'unique_visitors' in df.columns:
        kpis['total_unique_visitors'] = int(df['unique_visitors'].sum())
    
    if 'foot_traffic' in df.columns:
        kpis['total_foot_traffic'] = int(df['foot_traffic'].sum())
    
    logger.info(f"Computed {len(kpis)} basic KPIs")
    return kpis


def identify_top_performers(df: pd.DataFrame, n: int = 5) -> Dict[str, Any]:
    """
    Identify top performing segments.
    
    Args:
        df: Input DataFrame
        n: Number of top performers to return
        
    Returns:
        Dictionary of top performers by different dimensions
    """
    top_performers = {}
    
    # Identify categorical columns
    categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
    
    # Identify metric columns
    metric_cols = ['impressions', 'clicks', 'conversions', 'revenue', 'spend', 
                   'visits', 'foot_traffic', 'ctr', 'conversion_rate', 'roas']
    available_metrics = [col for col in metric_cols if col in df.columns]
    
    for cat_col in categorical_cols:
        if df[cat_col].nunique() > 1:
            for metric in available_metrics:
                try:
                    # Get top performers by this metric
                    top = df.groupby(cat_col)[metric].sum().nlargest(n)
                    key = f'top_{cat_col}_by_{metric}'
                    top_performers[key] = [
                        {'name': str(idx), 'value': round(float(val), 2)}
                        for idx, val in top.items()
                    ]
                except Exception as e:
                    logger.warning(f"Could not compute top performers for {cat_col}/{metric}: {str(e)}")
    
    logger.info(f"Identified top performers across {len(top_performers)} dimensions")
    return top_performers


def compute_period_comparison(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Compute period-over-period comparisons.
    
    Args:
        df: Input DataFrame with datetime columns
        
    Returns:
        Dictionary of period comparison metrics
    """
    comparisons = {}
    
    # Find datetime column
    datetime_cols = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
    
    if not datetime_cols:
        logger.warning("No datetime columns found for period comparison")
        return comparisons
    
    date_col = datetime_cols[0]
    df = df.copy()
    df['date'] = pd.to_datetime(df[date_col]).dt.date
    
    # Get date range
    min_date = df['date'].min()
    max_date = df['date'].max()
    date_range = (max_date - min_date).days
    
    if date_range < 2:
        logger.warning("Insufficient date range for period comparison")
        return comparisons
    
    # Split into two periods
    mid_date = min_date + pd.Timedelta(days=date_range // 2)
    
    period1 = df[df['date'] < mid_date]
    period2 = df[df['date'] >= mid_date]
    
    # Compare metrics
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    numeric_cols = [col for col in numeric_cols if col != 'date']
    
    for col in numeric_cols:
        p1_sum = period1[col].sum()
        p2_sum = period2[col].sum()
        
        if p1_sum > 0:
            change_pct = ((p2_sum - p1_sum) / p1_sum) * 100
        else:
            change_pct = 100 if p2_sum > 0 else 0
        
        comparisons[f'{col}_period_change_pct'] = round(float(change_pct), 2)
        comparisons[f'{col}_period1_total'] = round(float(p1_sum), 2)
        comparisons[f'{col}_period2_total'] = round(float(p2_sum), 2)
    
    comparisons['period1_dates'] = f"{min_date} to {mid_date}"
    comparisons['period2_dates'] = f"{mid_date} to {max_date}"
    
    logger.info("Computed period-over-period comparisons")
    return comparisons


def detect_anomalies(df: pd.DataFrame, threshold: float = 2.0) -> Dict[str, Any]:
    """
    Detect anomalies in the data using Z-score method.
    
    Args:
        df: Input DataFrame
        threshold: Z-score threshold for anomaly detection
        
    Returns:
        Dictionary of detected anomalies
    """
    anomalies = {}
    
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    
    for col in numeric_cols:
        data = df[col].dropna()
        if len(data) < 3:
            continue
        
        # Calculate Z-scores
        z_scores = np.abs(stats.zscore(data))
        anomaly_mask = z_scores > threshold
        
        if anomaly_mask.any():
            anomaly_indices = np.where(anomaly_mask)[0]
            anomaly_values = data.iloc[anomaly_indices].tolist()
            
            anomalies[col] = {
                'count': int(anomaly_mask.sum()),
                'percentage': round(float(anomaly_mask.mean() * 100), 2),
                'values': [round(v, 2) for v in anomaly_values[:5]],  # Top 5 anomalies
                'mean': round(float(data.mean()), 2),
                'std': round(float(data.std()), 2)
            }
    
    logger.info(f"Detected anomalies in {len(anomalies)} columns")
    return anomalies


def compute_correlations(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Compute correlations between numeric columns.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Dictionary of significant correlations
    """
    correlations = {}
    
    numeric_df = df.select_dtypes(include=[np.number])
    
    if numeric_df.shape[1] < 2:
        logger.warning("Not enough numeric columns for correlation analysis")
        return correlations
    
    # Compute correlation matrix
    corr_matrix = numeric_df.corr()
    
    # Find significant correlations (|r| > 0.5)
    significant_pairs = []
    
    for i, col1 in enumerate(corr_matrix.columns):
        for j, col2 in enumerate(corr_matrix.columns):
            if i < j:  # Only upper triangle
                corr_val = corr_matrix.loc[col1, col2]
                if abs(corr_val) > 0.5 and not np.isnan(corr_val):
                    significant_pairs.append({
                        'column1': col1,
                        'column2': col2,
                        'correlation': round(float(corr_val), 4),
                        'strength': 'strong' if abs(corr_val) > 0.7 else 'moderate'
                    })
    
    # Sort by absolute correlation
    significant_pairs.sort(key=lambda x: abs(x['correlation']), reverse=True)
    
    correlations['significant_pairs'] = significant_pairs[:10]  # Top 10
    correlations['total_variables'] = len(corr_matrix.columns)
    
    # Weather-traffic correlation if available
    weather_cols = [col for col in df.columns if 'weather' in col.lower() or 'temp' in col.lower()]
    traffic_cols = [col for col in df.columns if 'traffic' in col.lower() or 'visit' in col.lower()]
    
    if weather_cols and traffic_cols:
        for w_col in weather_cols:
            for t_col in traffic_cols:
                if w_col in numeric_df.columns and t_col in numeric_df.columns:
                    corr = numeric_df[w_col].corr(numeric_df[t_col])
                    if not np.isnan(corr):
                        correlations[f'{w_col}_vs_{t_col}'] = round(float(corr), 4)
    
    logger.info(f"Computed correlations: {len(significant_pairs)} significant pairs found")
    return correlations


def compute_summary_statistics(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Compute summary statistics for the dataset.
    
    Args:
        df: Input DataFrame
        
    Returns:
        Dictionary of summary statistics
    """
    summary = {
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'numeric_columns': len(df.select_dtypes(include=[np.number]).columns),
        'categorical_columns': len(df.select_dtypes(include=['object', 'category']).columns),
        'datetime_columns': len([col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]),
        'missing_values': int(df.isna().sum().sum()),
        'missing_percentage': round(float(df.isna().sum().sum() / (len(df) * len(df.columns)) * 100), 2)
    }
    
    # Date range if available
    datetime_cols = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
    if datetime_cols:
        date_col = datetime_cols[0]
        summary['date_range_start'] = str(df[date_col].min())
        summary['date_range_end'] = str(df[date_col].max())
        summary['total_days'] = (df[date_col].max() - df[date_col].min()).days
    
    logger.info("Computed summary statistics")
    return summary


def compute_kpis(data_path: str) -> Dict[str, Any]:
    """
    Main KPI computation function.
    
    This function computes:
    1. Basic KPIs (CTR, CPC, Conversion Rate, etc.)
    2. Top performing segments
    3. Period-over-period comparisons
    4. Anomaly detection
    5. Correlations (including weather-traffic)
    6. Summary statistics
    
    Args:
        data_path: Path to transformed data file
        
    Returns:
        Dictionary containing all computed KPIs and metrics
    """
    # Load transformed data
    df = pd.read_parquet(data_path)
    logger.info(f"Loaded data for KPI computation: {len(df)} rows")
    
    # Compute all KPIs
    kpis = {
        'basic_metrics': compute_basic_kpis(df),
        'top_performers': identify_top_performers(df),
        'period_comparison': compute_period_comparison(df),
        'anomalies': detect_anomalies(df),
        'correlations': compute_correlations(df),
        'summary': compute_summary_statistics(df)
    }
    
    # Add data snapshot for LLM context
    kpis['data_snapshot'] = {
        'columns': list(df.columns),
        'sample_values': df.head(5).to_dict('records')
    }
    
    logger.info("KPI computation completed successfully")
    
    return kpis

