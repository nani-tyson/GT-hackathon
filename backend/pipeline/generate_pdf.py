"""
PDF Report Generation Pipeline Step
GroundTruth Hackathon | Automated Insight Engine

This module handles:
- Creating professional PDF reports using ReportLab
- Including cover page, KPI tables, charts, and insights
"""

import os
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle,
    PageBreak, ListFlowable, ListItem
)
from reportlab.graphics.shapes import Drawing, Line

logger = logging.getLogger(__name__)

# Brand colors
BRAND_PRIMARY = colors.HexColor('#2563EB')
BRAND_SECONDARY = colors.HexColor('#10B981')
BRAND_DARK = colors.HexColor('#1E293B')
BRAND_LIGHT = colors.HexColor('#F1F5F9')


def create_styles() -> Dict[str, ParagraphStyle]:
    """Create custom paragraph styles for the report."""
    styles = getSampleStyleSheet()
    
    custom_styles = {
        'Title': ParagraphStyle(
            'CustomTitle',
            parent=styles['Title'],
            fontSize=28,
            textColor=BRAND_DARK,
            spaceAfter=30,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        ),
        'Subtitle': ParagraphStyle(
            'CustomSubtitle',
            parent=styles['Normal'],
            fontSize=14,
            textColor=colors.gray,
            spaceAfter=20,
            alignment=TA_CENTER
        ),
        'Heading1': ParagraphStyle(
            'CustomHeading1',
            parent=styles['Heading1'],
            fontSize=18,
            textColor=BRAND_PRIMARY,
            spaceBefore=20,
            spaceAfter=12,
            fontName='Helvetica-Bold'
        ),
        'Heading2': ParagraphStyle(
            'CustomHeading2',
            parent=styles['Heading2'],
            fontSize=14,
            textColor=BRAND_DARK,
            spaceBefore=15,
            spaceAfter=8,
            fontName='Helvetica-Bold'
        ),
        'Body': ParagraphStyle(
            'CustomBody',
            parent=styles['Normal'],
            fontSize=11,
            textColor=BRAND_DARK,
            spaceAfter=8,
            alignment=TA_JUSTIFY,
            leading=14
        ),
        'Bullet': ParagraphStyle(
            'CustomBullet',
            parent=styles['Normal'],
            fontSize=11,
            textColor=BRAND_DARK,
            leftIndent=20,
            spaceAfter=6,
            leading=14
        ),
        'KPIValue': ParagraphStyle(
            'KPIValue',
            parent=styles['Normal'],
            fontSize=24,
            textColor=BRAND_PRIMARY,
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        ),
        'KPILabel': ParagraphStyle(
            'KPILabel',
            parent=styles['Normal'],
            fontSize=10,
            textColor=colors.gray,
            alignment=TA_CENTER
        ),
        'Footer': ParagraphStyle(
            'Footer',
            parent=styles['Normal'],
            fontSize=9,
            textColor=colors.gray,
            alignment=TA_CENTER
        )
    }
    
    return custom_styles


def create_cover_page(
    title: str,
    styles: Dict[str, ParagraphStyle],
    report_id: str
) -> List:
    """Create the cover page elements."""
    elements = []
    
    # Add spacing for cover
    elements.append(Spacer(1, 2 * inch))
    
    # Title
    elements.append(Paragraph(title, styles['Title']))
    
    # Subtitle
    elements.append(Paragraph(
        "Automated Insight Engine Report",
        styles['Subtitle']
    ))
    
    elements.append(Spacer(1, 0.5 * inch))
    
    # Date
    date_str = datetime.now().strftime("%B %d, %Y")
    elements.append(Paragraph(
        f"Generated on {date_str}",
        styles['Subtitle']
    ))
    
    # Report ID
    elements.append(Spacer(1, 2 * inch))
    elements.append(Paragraph(
        f"Report ID: {report_id[:8]}...",
        styles['Footer']
    ))
    
    elements.append(PageBreak())
    
    return elements


def create_executive_summary_section(
    insights: Dict[str, Any],
    styles: Dict[str, ParagraphStyle]
) -> List:
    """Create the executive summary section."""
    elements = []
    
    elements.append(Paragraph("Executive Summary", styles['Heading1']))
    
    summary = insights.get('executive_summary', 'No summary available.')
    elements.append(Paragraph(summary, styles['Body']))
    
    elements.append(Spacer(1, 0.3 * inch))
    
    return elements


def create_kpi_table(
    kpis: Dict[str, Any],
    styles: Dict[str, ParagraphStyle]
) -> List:
    """Create KPI summary table."""
    elements = []
    
    elements.append(Paragraph("Key Performance Indicators", styles['Heading1']))
    
    basic_metrics = kpis.get('basic_metrics', {})
    
    # Select important KPIs
    important_kpis = [
        ('Total Impressions', 'total_impressions', '{:,.0f}'),
        ('Total Clicks', 'total_clicks', '{:,.0f}'),
        ('Overall CTR', 'overall_ctr', '{:.2f}%'),
        ('Total Spend', 'total_spend', '${:,.2f}'),
        ('Total Conversions', 'total_conversions', '{:,.0f}'),
        ('Conversion Rate', 'overall_conversion_rate', '{:.2f}%'),
        ('Cost Per Click', 'overall_cpc', '${:.2f}'),
        ('Total Revenue', 'total_revenue', '${:,.2f}'),
        ('ROAS', 'overall_roas', '{:.2f}x'),
    ]
    
    # Build table data
    table_data = [['Metric', 'Value']]
    
    for label, key, fmt in important_kpis:
        if key in basic_metrics:
            value = basic_metrics[key]
            try:
                formatted_value = fmt.format(value)
            except:
                formatted_value = str(value)
            table_data.append([label, formatted_value])
    
    if len(table_data) > 1:
        table = Table(table_data, colWidths=[3 * inch, 2 * inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), BRAND_PRIMARY),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('TOPPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), BRAND_LIGHT),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 11),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 8),
            ('TOPPADDING', (0, 1), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.white),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [BRAND_LIGHT, colors.white]),
        ]))
        
        elements.append(table)
    
    elements.append(Spacer(1, 0.3 * inch))
    
    return elements


def create_highlights_section(
    insights: Dict[str, Any],
    styles: Dict[str, ParagraphStyle]
) -> List:
    """Create key highlights section."""
    elements = []
    
    elements.append(Paragraph("Key Highlights", styles['Heading1']))
    
    highlights = insights.get('key_highlights', [])
    
    if highlights:
        for highlight in highlights:
            bullet_text = f"• {highlight}"
            elements.append(Paragraph(bullet_text, styles['Bullet']))
    else:
        elements.append(Paragraph("No highlights available.", styles['Body']))
    
    elements.append(Spacer(1, 0.3 * inch))
    
    return elements


def create_issues_section(
    insights: Dict[str, Any],
    styles: Dict[str, ParagraphStyle]
) -> List:
    """Create performance issues section."""
    elements = []
    
    elements.append(Paragraph("Performance Issues", styles['Heading1']))
    
    issues = insights.get('performance_issues', [])
    
    if issues:
        for issue in issues:
            bullet_text = f"• {issue}"
            elements.append(Paragraph(bullet_text, styles['Bullet']))
    else:
        elements.append(Paragraph("No significant issues detected.", styles['Body']))
    
    elements.append(Spacer(1, 0.3 * inch))
    
    return elements


def create_recommendations_section(
    insights: Dict[str, Any],
    styles: Dict[str, ParagraphStyle]
) -> List:
    """Create recommendations section."""
    elements = []
    
    elements.append(Paragraph("Recommendations", styles['Heading1']))
    
    recommendations = insights.get('recommendations', [])
    
    if recommendations:
        for i, rec in enumerate(recommendations, 1):
            bullet_text = f"{i}. {rec}"
            elements.append(Paragraph(bullet_text, styles['Bullet']))
    else:
        elements.append(Paragraph("No recommendations available.", styles['Body']))
    
    elements.append(Spacer(1, 0.3 * inch))
    
    return elements


def create_charts_section(
    charts: Dict[str, Any],
    styles: Dict[str, ParagraphStyle]
) -> List:
    """Create charts section with embedded images."""
    elements = []
    
    elements.append(PageBreak())
    elements.append(Paragraph("Visual Analytics", styles['Heading1']))
    
    chart_paths = charts.get('chart_paths', [])
    
    if not chart_paths:
        elements.append(Paragraph("No charts available.", styles['Body']))
        return elements
    
    for chart_path in chart_paths:
        if os.path.exists(chart_path):
            try:
                # Get chart name from path
                chart_name = Path(chart_path).stem.replace('_', ' ').title()
                elements.append(Paragraph(chart_name, styles['Heading2']))
                
                # Add image
                img = Image(chart_path, width=6.5 * inch, height=4 * inch)
                img.hAlign = 'CENTER'
                elements.append(img)
                elements.append(Spacer(1, 0.3 * inch))
                
            except Exception as e:
                logger.warning(f"Could not add chart {chart_path}: {str(e)}")
    
    return elements


def create_data_summary_section(
    kpis: Dict[str, Any],
    styles: Dict[str, ParagraphStyle]
) -> List:
    """Create data summary section."""
    elements = []
    
    elements.append(Paragraph("Data Summary", styles['Heading1']))
    
    summary = kpis.get('summary', {})
    
    if summary:
        summary_items = [
            f"Total Records: {summary.get('total_rows', 'N/A'):,}",
            f"Total Columns: {summary.get('total_columns', 'N/A')}",
            f"Date Range: {summary.get('date_range_start', 'N/A')} to {summary.get('date_range_end', 'N/A')}",
            f"Total Days: {summary.get('total_days', 'N/A')}",
            f"Missing Values: {summary.get('missing_percentage', 0):.2f}%"
        ]
        
        for item in summary_items:
            elements.append(Paragraph(f"• {item}", styles['Bullet']))
    
    elements.append(Spacer(1, 0.3 * inch))
    
    return elements


def add_page_number(canvas, doc):
    """Add page numbers to the document."""
    page_num = canvas.getPageNumber()
    text = f"Page {page_num}"
    canvas.saveState()
    canvas.setFont('Helvetica', 9)
    canvas.setFillColor(colors.gray)
    canvas.drawCentredString(letter[0] / 2, 0.5 * inch, text)
    canvas.restoreState()


def generate_pdf_report(
    report_id: str,
    report_title: str,
    kpis: Dict[str, Any],
    charts: Dict[str, Any],
    insights: Dict[str, Any],
    output_dir: str
) -> Dict[str, Any]:
    """
    Main PDF report generation function.
    
    This function creates a professional PDF report including:
    1. Cover page
    2. Executive summary
    3. KPI summary table
    4. Key highlights
    5. Performance issues
    6. Recommendations
    7. Charts and visualizations
    8. Data summary
    
    Args:
        report_id: Unique report identifier
        report_title: Title for the report
        kpis: Dictionary of computed KPIs
        charts: Dictionary containing chart paths
        insights: Dictionary containing LLM-generated insights
        output_dir: Directory to save the report
        
    Returns:
        Dictionary with file path and status
    """
    logger.info(f"Generating PDF report: {report_id}")
    
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Define output file path
    file_path = output_path / f"{report_id}.pdf"
    
    # Create document
    doc = SimpleDocTemplate(
        str(file_path),
        pagesize=letter,
        rightMargin=0.75 * inch,
        leftMargin=0.75 * inch,
        topMargin=0.75 * inch,
        bottomMargin=0.75 * inch
    )
    
    # Create styles
    styles = create_styles()
    
    # Build document elements
    elements = []
    
    # Cover page
    elements.extend(create_cover_page(report_title, styles, report_id))
    
    # Executive summary
    elements.extend(create_executive_summary_section(insights, styles))
    
    # KPI table
    elements.extend(create_kpi_table(kpis, styles))
    
    # Key highlights
    elements.extend(create_highlights_section(insights, styles))
    
    # Performance issues
    elements.extend(create_issues_section(insights, styles))
    
    # Recommendations
    elements.extend(create_recommendations_section(insights, styles))
    
    # Charts
    elements.extend(create_charts_section(charts, styles))
    
    # Data summary
    elements.extend(create_data_summary_section(kpis, styles))
    
    # Build PDF
    try:
        doc.build(elements, onFirstPage=add_page_number, onLaterPages=add_page_number)
        logger.info(f"PDF report generated: {file_path}")
        
        return {
            'status': 'success',
            'file_path': str(file_path),
            'format': 'pdf',
            'pages': 'multiple'
        }
        
    except Exception as e:
        logger.error(f"Failed to generate PDF: {str(e)}")
        raise

