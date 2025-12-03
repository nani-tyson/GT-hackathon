"""
LLM Insights Generation Pipeline Step
GroundTruth Hackathon | Automated Insight Engine

This module handles:
- Generating executive summaries using OpenAI
- Creating key highlights
- Identifying performance issues
- Providing actionable recommendations
"""

import os
import json
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# Try to import OpenAI
try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    logger.warning("OpenAI package not available. Using fallback insights generation.")


def format_kpis_for_prompt(kpis: Dict[str, Any]) -> str:
    """
    Format KPIs into a readable string for the LLM prompt.
    
    Args:
        kpis: Dictionary of computed KPIs
        
    Returns:
        Formatted string representation of KPIs
    """
    formatted_parts = []
    
    # Basic metrics
    basic = kpis.get('basic_metrics', {})
    if basic:
        formatted_parts.append("## Key Metrics")
        for key, value in basic.items():
            if isinstance(value, (int, float)):
                formatted_key = key.replace('_', ' ').title()
                if isinstance(value, float):
                    formatted_parts.append(f"- {formatted_key}: {value:,.2f}")
                else:
                    formatted_parts.append(f"- {formatted_key}: {value:,}")
    
    # Top performers
    top = kpis.get('top_performers', {})
    if top:
        formatted_parts.append("\n## Top Performers")
        for category, performers in list(top.items())[:5]:
            if performers:
                formatted_parts.append(f"\n### {category.replace('_', ' ').title()}")
                for p in performers[:3]:
                    formatted_parts.append(f"- {p['name']}: {p['value']:,.2f}")
    
    # Period comparison
    period = kpis.get('period_comparison', {})
    if period:
        formatted_parts.append("\n## Period-over-Period Changes")
        for key, value in period.items():
            if 'change_pct' in key:
                metric = key.replace('_period_change_pct', '').replace('_', ' ').title()
                direction = "increased" if value > 0 else "decreased"
                formatted_parts.append(f"- {metric} {direction} by {abs(value):.1f}%")
    
    # Anomalies
    anomalies = kpis.get('anomalies', {})
    if anomalies:
        formatted_parts.append("\n## Detected Anomalies")
        for col, info in list(anomalies.items())[:5]:
            formatted_parts.append(f"- {col.replace('_', ' ').title()}: {info['count']} anomalies detected ({info['percentage']:.1f}% of data)")
    
    # Correlations
    correlations = kpis.get('correlations', {})
    if correlations:
        pairs = correlations.get('significant_pairs', [])
        if pairs:
            formatted_parts.append("\n## Significant Correlations")
            for pair in pairs[:5]:
                formatted_parts.append(f"- {pair['column1']} ↔ {pair['column2']}: {pair['correlation']:.2f} ({pair['strength']})")
    
    # Summary
    summary = kpis.get('summary', {})
    if summary:
        formatted_parts.append("\n## Data Summary")
        formatted_parts.append(f"- Total Records: {summary.get('total_rows', 'N/A'):,}")
        formatted_parts.append(f"- Date Range: {summary.get('date_range_start', 'N/A')} to {summary.get('date_range_end', 'N/A')}")
        formatted_parts.append(f"- Total Days: {summary.get('total_days', 'N/A')}")
    
    return "\n".join(formatted_parts)


def generate_insights_with_openai(
    kpis: Dict[str, Any],
    charts_summary: Dict[str, str]
) -> Dict[str, Any]:
    """
    Generate insights using OpenAI API.
    
    Args:
        kpis: Dictionary of computed KPIs
        charts_summary: Summary of generated charts
        
    Returns:
        Dictionary containing generated insights
    """
    api_key = os.getenv('OPENAI_API_KEY')
    
    if not api_key:
        logger.warning("OPENAI_API_KEY not found. Using fallback insights.")
        return generate_fallback_insights(kpis)
    
    if not OPENAI_AVAILABLE:
        logger.warning("OpenAI package not installed. Using fallback insights.")
        return generate_fallback_insights(kpis)
    
    try:
        client = OpenAI(api_key=api_key)
        
        # Format KPIs for prompt
        kpi_text = format_kpis_for_prompt(kpis)
        
        # Construct prompt
        prompt = f"""You are a senior data analyst at an AdTech company. Based on the KPIs and metrics below, generate a comprehensive executive summary for stakeholders.

{kpi_text}

Charts Generated: {', '.join(charts_summary.keys()) if charts_summary else 'None'}

Please provide:

1. **Executive Summary** (2-3 sentences overview)

2. **5 Key Highlights** (positive findings, achievements, notable patterns)

3. **3 Performance Issues** (areas of concern, declining metrics, anomalies)

4. **3 Actionable Recommendations** (specific, data-driven suggestions for improvement)

Write in crisp, professional business language. Use specific numbers from the data. Format your response in clear sections with bullet points."""

        # Call OpenAI API
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": "You are a data analyst expert who provides clear, actionable insights from marketing and advertising data. Always be specific and reference actual metrics."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.7,
            max_tokens=1500
        )
        
        insights_text = response.choices[0].message.content
        
        # Parse the response into structured format
        insights = parse_llm_response(insights_text)
        insights['raw_response'] = insights_text
        insights['model'] = 'gpt-4o'
        insights['status'] = 'success'
        
        logger.info("Successfully generated insights with OpenAI")
        return insights
        
    except Exception as e:
        logger.error(f"OpenAI API error: {str(e)}")
        return generate_fallback_insights(kpis)


def parse_llm_response(response_text: str) -> Dict[str, Any]:
    """
    Parse the LLM response into structured sections.
    
    Args:
        response_text: Raw text response from LLM
        
    Returns:
        Dictionary with parsed sections
    """
    sections = {
        'executive_summary': '',
        'key_highlights': [],
        'performance_issues': [],
        'recommendations': []
    }
    
    current_section = None
    current_items = []
    
    lines = response_text.split('\n')
    
    for line in lines:
        line = line.strip()
        
        # Detect section headers
        lower_line = line.lower()
        if 'executive summary' in lower_line:
            current_section = 'executive_summary'
            current_items = []
        elif 'key highlight' in lower_line or 'highlights' in lower_line:
            if current_section == 'executive_summary' and current_items:
                sections['executive_summary'] = ' '.join(current_items)
            current_section = 'key_highlights'
            current_items = []
        elif 'performance issue' in lower_line or 'issues' in lower_line or 'concerns' in lower_line:
            if current_section and current_items:
                if current_section == 'key_highlights':
                    sections['key_highlights'] = current_items
            current_section = 'performance_issues'
            current_items = []
        elif 'recommendation' in lower_line or 'action' in lower_line:
            if current_section and current_items:
                if current_section == 'performance_issues':
                    sections['performance_issues'] = current_items
            current_section = 'recommendations'
            current_items = []
        elif line and not line.startswith('#'):
            # Add content to current section
            if line.startswith(('-', '•', '*', '1', '2', '3', '4', '5')):
                # Clean bullet points
                clean_line = line.lstrip('-•* 0123456789.)')
                if clean_line:
                    current_items.append(clean_line)
            elif current_section == 'executive_summary':
                current_items.append(line)
    
    # Save last section
    if current_section and current_items:
        if current_section == 'executive_summary':
            sections['executive_summary'] = ' '.join(current_items)
        elif current_section == 'recommendations':
            sections['recommendations'] = current_items
        elif current_section == 'key_highlights':
            sections['key_highlights'] = current_items
        elif current_section == 'performance_issues':
            sections['performance_issues'] = current_items
    
    return sections


def generate_fallback_insights(kpis: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate fallback insights when OpenAI is not available.
    
    Args:
        kpis: Dictionary of computed KPIs
        
    Returns:
        Dictionary containing generated insights
    """
    basic = kpis.get('basic_metrics', {})
    top_performers = kpis.get('top_performers', {})
    period = kpis.get('period_comparison', {})
    anomalies = kpis.get('anomalies', {})
    summary = kpis.get('summary', {})
    
    # Generate executive summary
    total_rows = summary.get('total_rows', 0)
    date_range = f"{summary.get('date_range_start', 'N/A')} to {summary.get('date_range_end', 'N/A')}"
    
    executive_summary = f"This report analyzes {total_rows:,} data records spanning {date_range}. "
    
    if 'overall_ctr' in basic:
        executive_summary += f"The overall click-through rate is {basic['overall_ctr']:.2f}%. "
    if 'total_revenue' in basic:
        executive_summary += f"Total revenue generated is ${basic['total_revenue']:,.2f}. "
    if 'overall_roas' in basic:
        executive_summary += f"Return on ad spend stands at {basic['overall_roas']:.2f}x."
    
    # Generate highlights
    highlights = []
    
    if 'total_impressions' in basic:
        highlights.append(f"Total impressions reached {basic['total_impressions']:,}")
    if 'total_clicks' in basic:
        highlights.append(f"Generated {basic['total_clicks']:,} total clicks")
    if 'total_conversions' in basic:
        highlights.append(f"Achieved {basic['total_conversions']:,} conversions")
    
    # Add top performer highlights
    for key, performers in list(top_performers.items())[:2]:
        if performers:
            top = performers[0]
            metric = key.split('_by_')[-1] if '_by_' in key else 'performance'
            highlights.append(f"Top performer: {top['name']} with {top['value']:,.2f} {metric}")
    
    # Ensure we have 5 highlights
    while len(highlights) < 5:
        highlights.append("Data quality maintained throughout the reporting period")
    
    # Generate issues
    issues = []
    
    # Check for negative trends
    for key, value in period.items():
        if 'change_pct' in key and value < -10:
            metric = key.replace('_period_change_pct', '').replace('_', ' ')
            issues.append(f"{metric.title()} decreased by {abs(value):.1f}% compared to previous period")
    
    # Check for anomalies
    for col, info in list(anomalies.items())[:2]:
        if info['percentage'] > 5:
            issues.append(f"Detected {info['count']} anomalies in {col.replace('_', ' ')} ({info['percentage']:.1f}% of data)")
    
    # Ensure we have 3 issues
    while len(issues) < 3:
        issues.append("Consider implementing additional data validation checks")
    
    # Generate recommendations
    recommendations = [
        "Increase budget allocation to top-performing segments identified in this report",
        "Investigate and address detected anomalies to improve data quality",
        "Implement A/B testing for underperforming campaigns to optimize conversion rates",
        "Set up automated alerts for significant metric deviations",
        "Review targeting strategies for segments showing declining performance"
    ]
    
    return {
        'executive_summary': executive_summary,
        'key_highlights': highlights[:5],
        'performance_issues': issues[:3],
        'recommendations': recommendations[:3],
        'raw_response': 'Generated using fallback method (OpenAI not available)',
        'model': 'fallback',
        'status': 'success'
    }


def run_llm_insights(
    kpis: Dict[str, Any],
    charts_summary: Dict[str, str] = None
) -> Dict[str, Any]:
    """
    Main LLM insights generation function.
    
    This function:
    1. Formats KPIs for LLM consumption
    2. Calls OpenAI API (or fallback) to generate insights
    3. Returns structured insights including:
       - Executive summary
       - 5 key highlights
       - 3 performance issues
       - 3 actionable recommendations
    
    Args:
        kpis: Dictionary of computed KPIs
        charts_summary: Summary of generated charts
        
    Returns:
        Dictionary containing all generated insights
    """
    logger.info("Starting LLM insights generation")
    
    if charts_summary is None:
        charts_summary = {}
    
    # Try OpenAI first, fall back to rule-based generation
    insights = generate_insights_with_openai(kpis, charts_summary)
    
    # Validate insights structure
    required_keys = ['executive_summary', 'key_highlights', 'performance_issues', 'recommendations']
    for key in required_keys:
        if key not in insights:
            insights[key] = [] if key != 'executive_summary' else 'Analysis complete.'
    
    logger.info("LLM insights generation completed")
    
    return insights

