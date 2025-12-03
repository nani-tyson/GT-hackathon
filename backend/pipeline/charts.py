"""
Chart Generation Pipeline Step
GroundTruth Hackathon | Automated Insight Engine

This module handles:
- Daily performance line graphs
- Bar charts for top categories
- Pie charts for segmentation
- Saving charts as PNG images
"""

import logging
from pathlib import Path
from typing import Dict, Any, List, Optional

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import MaxNLocator

logger = logging.getLogger(__name__)

# Chart styling configuration
CHART_STYLE = {
    'figure.figsize': (12, 6),
    'axes.titlesize': 14,
    'axes.labelsize': 12,
    'xtick.labelsize': 10,
    'ytick.labelsize': 10,
    'legend.fontsize': 10,
    'figure.dpi': 150,
    'axes.grid': True,
    'grid.alpha': 0.3,
    'axes.spines.top': False,
    'axes.spines.right': False
}

# Color palette
COLORS = [
    '#2563EB',  # Blue
    '#10B981',  # Green
    '#F59E0B',  # Amber
    '#EF4444',  # Red
    '#8B5CF6',  # Purple
    '#EC4899',  # Pink
    '#06B6D4',  # Cyan
    '#F97316',  # Orange
]


def apply_chart_style():
    """Apply consistent styling to all charts."""
    plt.rcParams.update(CHART_STYLE)
    plt.style.use('seaborn-v0_8-whitegrid')


def create_daily_performance_chart(
    df: pd.DataFrame,
    output_dir: Path,
    metrics: List[str] = None
) -> Optional[str]:
    """
    Create daily performance line graph.
    
    Args:
        df: Input DataFrame with datetime column
        output_dir: Directory to save chart
        metrics: List of metrics to plot
        
    Returns:
        Path to saved chart or None if failed
    """
    apply_chart_style()
    
    # Find datetime column
    datetime_cols = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
    
    if not datetime_cols:
        logger.warning("No datetime columns found for daily performance chart")
        return None
    
    date_col = datetime_cols[0]
    df = df.copy()
    df['date'] = pd.to_datetime(df[date_col]).dt.date
    
    # Identify metrics to plot
    if metrics is None:
        metrics = ['impressions', 'clicks', 'conversions', 'spend', 'revenue', 
                   'visits', 'foot_traffic', 'ctr', 'conversion_rate']
    
    available_metrics = [m for m in metrics if m in df.columns]
    
    if not available_metrics:
        logger.warning("No metrics available for daily performance chart")
        return None
    
    # Aggregate by date
    daily_data = df.groupby('date')[available_metrics].sum().reset_index()
    daily_data = daily_data.sort_values('date')
    
    # Create figure with multiple subplots
    n_metrics = min(len(available_metrics), 4)  # Max 4 metrics per chart
    fig, axes = plt.subplots(n_metrics, 1, figsize=(12, 4 * n_metrics), sharex=True)
    
    if n_metrics == 1:
        axes = [axes]
    
    for idx, metric in enumerate(available_metrics[:n_metrics]):
        ax = axes[idx]
        ax.plot(daily_data['date'], daily_data[metric], 
                color=COLORS[idx % len(COLORS)], linewidth=2, marker='o', markersize=4)
        ax.fill_between(daily_data['date'], daily_data[metric], alpha=0.1, color=COLORS[idx % len(COLORS)])
        ax.set_ylabel(metric.replace('_', ' ').title())
        ax.yaxis.set_major_locator(MaxNLocator(integer=True, nbins=5))
        
        # Add trend line
        if len(daily_data) > 2:
            z = np.polyfit(range(len(daily_data)), daily_data[metric].values, 1)
            p = np.poly1d(z)
            ax.plot(daily_data['date'], p(range(len(daily_data))), 
                    '--', color=COLORS[idx % len(COLORS)], alpha=0.5, label='Trend')
    
    axes[0].set_title('Daily Performance Overview', fontsize=16, fontweight='bold', pad=20)
    axes[-1].set_xlabel('Date')
    
    # Format x-axis
    plt.gcf().autofmt_xdate()
    
    plt.tight_layout()
    
    # Save chart
    chart_path = output_dir / 'daily_performance.png'
    plt.savefig(chart_path, bbox_inches='tight', facecolor='white', edgecolor='none')
    plt.close()
    
    logger.info(f"Created daily performance chart: {chart_path}")
    return str(chart_path)


def create_top_categories_bar_chart(
    kpis: Dict[str, Any],
    output_dir: Path
) -> Optional[str]:
    """
    Create bar chart for top performing categories.
    
    Args:
        kpis: Dictionary containing top performers data
        output_dir: Directory to save chart
        
    Returns:
        Path to saved chart or None if failed
    """
    apply_chart_style()
    
    top_performers = kpis.get('top_performers', {})
    
    if not top_performers:
        logger.warning("No top performers data available for bar chart")
        return None
    
    # Find a suitable metric to visualize
    chart_data = None
    chart_title = ""
    
    for key, data in top_performers.items():
        if data and len(data) > 0:
            chart_data = data
            # Parse title from key (e.g., 'top_region_by_revenue' -> 'Top Region by Revenue')
            chart_title = key.replace('_', ' ').title()
            break
    
    if not chart_data:
        logger.warning("No suitable data found for bar chart")
        return None
    
    # Create bar chart
    fig, ax = plt.subplots(figsize=(12, 6))
    
    names = [item['name'] for item in chart_data]
    values = [item['value'] for item in chart_data]
    
    bars = ax.barh(names, values, color=COLORS[:len(names)])
    
    # Add value labels
    for bar, value in zip(bars, values):
        ax.text(bar.get_width() + max(values) * 0.01, bar.get_y() + bar.get_height()/2,
                f'{value:,.0f}', va='center', fontsize=10)
    
    ax.set_xlabel('Value')
    ax.set_title(chart_title, fontsize=16, fontweight='bold', pad=20)
    ax.invert_yaxis()  # Highest value at top
    
    plt.tight_layout()
    
    # Save chart
    chart_path = output_dir / 'top_categories.png'
    plt.savefig(chart_path, bbox_inches='tight', facecolor='white', edgecolor='none')
    plt.close()
    
    logger.info(f"Created top categories bar chart: {chart_path}")
    return str(chart_path)


def create_segmentation_pie_chart(
    df: pd.DataFrame,
    output_dir: Path
) -> Optional[str]:
    """
    Create pie chart for segmentation analysis.
    
    Args:
        df: Input DataFrame
        output_dir: Directory to save chart
        
    Returns:
        Path to saved chart or None if failed
    """
    apply_chart_style()
    
    # Find categorical column for segmentation
    categorical_keywords = ['region', 'category', 'segment', 'channel', 'source', 
                           'device', 'platform', 'campaign', 'country']
    
    cat_col = None
    for keyword in categorical_keywords:
        matching = [col for col in df.columns if keyword in col.lower() and df[col].dtype == 'object']
        if matching:
            cat_col = matching[0]
            break
    
    if not cat_col:
        logger.warning("No suitable categorical column found for pie chart")
        return None
    
    # Find metric column
    metric_cols = ['revenue', 'spend', 'conversions', 'clicks', 'impressions', 'visits']
    metric_col = None
    for metric in metric_cols:
        if metric in df.columns:
            metric_col = metric
            break
    
    if not metric_col:
        # Use count if no metric found
        segment_data = df[cat_col].value_counts().head(8)
        metric_label = 'Count'
    else:
        segment_data = df.groupby(cat_col)[metric_col].sum().nlargest(8)
        metric_label = metric_col.replace('_', ' ').title()
    
    # Create pie chart
    fig, ax = plt.subplots(figsize=(10, 8))
    
    # Calculate percentages
    total = segment_data.sum()
    percentages = (segment_data / total * 100).round(1)
    
    # Create labels with percentages
    labels = [f'{name}\n({pct}%)' for name, pct in zip(segment_data.index, percentages)]
    
    wedges, texts, autotexts = ax.pie(
        segment_data.values,
        labels=labels,
        colors=COLORS[:len(segment_data)],
        autopct='',
        startangle=90,
        explode=[0.02] * len(segment_data)
    )
    
    ax.set_title(f'{metric_label} Distribution by {cat_col.replace("_", " ").title()}',
                 fontsize=16, fontweight='bold', pad=20)
    
    plt.tight_layout()
    
    # Save chart
    chart_path = output_dir / 'segmentation_pie.png'
    plt.savefig(chart_path, bbox_inches='tight', facecolor='white', edgecolor='none')
    plt.close()
    
    logger.info(f"Created segmentation pie chart: {chart_path}")
    return str(chart_path)


def create_kpi_comparison_chart(
    kpis: Dict[str, Any],
    output_dir: Path
) -> Optional[str]:
    """
    Create comparison chart for period-over-period metrics.
    
    Args:
        kpis: Dictionary containing period comparison data
        output_dir: Directory to save chart
        
    Returns:
        Path to saved chart or None if failed
    """
    apply_chart_style()
    
    period_data = kpis.get('period_comparison', {})
    
    if not period_data:
        logger.warning("No period comparison data available")
        return None
    
    # Extract metrics with both periods
    metrics = []
    period1_values = []
    period2_values = []
    changes = []
    
    for key, value in period_data.items():
        if key.endswith('_period1_total'):
            metric = key.replace('_period1_total', '')
            p1 = value
            p2 = period_data.get(f'{metric}_period2_total', 0)
            change = period_data.get(f'{metric}_period_change_pct', 0)
            
            if p1 > 0 or p2 > 0:
                metrics.append(metric.replace('_', ' ').title())
                period1_values.append(p1)
                period2_values.append(p2)
                changes.append(change)
    
    if not metrics:
        logger.warning("No metrics found for comparison chart")
        return None
    
    # Limit to top 6 metrics
    if len(metrics) > 6:
        metrics = metrics[:6]
        period1_values = period1_values[:6]
        period2_values = period2_values[:6]
        changes = changes[:6]
    
    # Create grouped bar chart
    fig, ax = plt.subplots(figsize=(12, 6))
    
    x = np.arange(len(metrics))
    width = 0.35
    
    bars1 = ax.bar(x - width/2, period1_values, width, label='Period 1', color=COLORS[0])
    bars2 = ax.bar(x + width/2, period2_values, width, label='Period 2', color=COLORS[1])
    
    # Add change percentages
    for i, (bar, change) in enumerate(zip(bars2, changes)):
        color = COLORS[1] if change >= 0 else COLORS[3]
        symbol = '↑' if change >= 0 else '↓'
        ax.annotate(f'{symbol}{abs(change):.1f}%',
                   xy=(bar.get_x() + bar.get_width()/2, bar.get_height()),
                   ha='center', va='bottom', fontsize=9, color=color, fontweight='bold')
    
    ax.set_ylabel('Value')
    ax.set_title('Period-over-Period Comparison', fontsize=16, fontweight='bold', pad=20)
    ax.set_xticks(x)
    ax.set_xticklabels(metrics, rotation=45, ha='right')
    ax.legend()
    
    plt.tight_layout()
    
    # Save chart
    chart_path = output_dir / 'period_comparison.png'
    plt.savefig(chart_path, bbox_inches='tight', facecolor='white', edgecolor='none')
    plt.close()
    
    logger.info(f"Created period comparison chart: {chart_path}")
    return str(chart_path)


def create_correlation_heatmap(
    df: pd.DataFrame,
    output_dir: Path
) -> Optional[str]:
    """
    Create correlation heatmap for numeric variables.
    
    Args:
        df: Input DataFrame
        output_dir: Directory to save chart
        
    Returns:
        Path to saved chart or None if failed
    """
    apply_chart_style()
    
    numeric_df = df.select_dtypes(include=[np.number])
    
    if numeric_df.shape[1] < 2:
        logger.warning("Not enough numeric columns for correlation heatmap")
        return None
    
    # Limit to top 10 columns by variance
    if numeric_df.shape[1] > 10:
        variances = numeric_df.var().nlargest(10)
        numeric_df = numeric_df[variances.index]
    
    # Compute correlation matrix
    corr_matrix = numeric_df.corr()
    
    # Create heatmap
    fig, ax = plt.subplots(figsize=(10, 8))
    
    im = ax.imshow(corr_matrix, cmap='RdBu_r', aspect='auto', vmin=-1, vmax=1)
    
    # Add colorbar
    cbar = plt.colorbar(im, ax=ax)
    cbar.set_label('Correlation Coefficient')
    
    # Set ticks
    ax.set_xticks(np.arange(len(corr_matrix.columns)))
    ax.set_yticks(np.arange(len(corr_matrix.columns)))
    ax.set_xticklabels([col.replace('_', ' ')[:15] for col in corr_matrix.columns], rotation=45, ha='right')
    ax.set_yticklabels([col.replace('_', ' ')[:15] for col in corr_matrix.columns])
    
    # Add correlation values
    for i in range(len(corr_matrix.columns)):
        for j in range(len(corr_matrix.columns)):
            val = corr_matrix.iloc[i, j]
            color = 'white' if abs(val) > 0.5 else 'black'
            ax.text(j, i, f'{val:.2f}', ha='center', va='center', color=color, fontsize=8)
    
    ax.set_title('Correlation Heatmap', fontsize=16, fontweight='bold', pad=20)
    
    plt.tight_layout()
    
    # Save chart
    chart_path = output_dir / 'correlation_heatmap.png'
    plt.savefig(chart_path, bbox_inches='tight', facecolor='white', edgecolor='none')
    plt.close()
    
    logger.info(f"Created correlation heatmap: {chart_path}")
    return str(chart_path)


def generate_charts(
    data_path: str,
    kpis: Dict[str, Any],
    output_dir: str
) -> Dict[str, Any]:
    """
    Main chart generation function.
    
    This function generates:
    1. Daily performance line graph
    2. Top categories bar chart
    3. Segmentation pie chart
    4. Period comparison chart
    5. Correlation heatmap
    
    Args:
        data_path: Path to transformed data file
        kpis: Dictionary of computed KPIs
        output_dir: Directory to save charts
        
    Returns:
        Dictionary containing paths to generated charts and summary
    """
    # Load data
    df = pd.read_parquet(data_path)
    logger.info(f"Loaded data for chart generation: {len(df)} rows")
    
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Generate all charts
    chart_paths = []
    chart_summary = {}
    
    # 1. Daily performance chart
    daily_chart = create_daily_performance_chart(df, output_path)
    if daily_chart:
        chart_paths.append(daily_chart)
        chart_summary['daily_performance'] = 'Generated'
    
    # 2. Top categories bar chart
    bar_chart = create_top_categories_bar_chart(kpis, output_path)
    if bar_chart:
        chart_paths.append(bar_chart)
        chart_summary['top_categories'] = 'Generated'
    
    # 3. Segmentation pie chart
    pie_chart = create_segmentation_pie_chart(df, output_path)
    if pie_chart:
        chart_paths.append(pie_chart)
        chart_summary['segmentation'] = 'Generated'
    
    # 4. Period comparison chart
    comparison_chart = create_kpi_comparison_chart(kpis, output_path)
    if comparison_chart:
        chart_paths.append(comparison_chart)
        chart_summary['period_comparison'] = 'Generated'
    
    # 5. Correlation heatmap
    heatmap = create_correlation_heatmap(df, output_path)
    if heatmap:
        chart_paths.append(heatmap)
        chart_summary['correlation_heatmap'] = 'Generated'
    
    logger.info(f"Generated {len(chart_paths)} charts")
    
    return {
        'status': 'success',
        'chart_paths': chart_paths,
        'summary': chart_summary,
        'output_dir': str(output_path)
    }

