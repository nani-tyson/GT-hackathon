"""
Unstructured Data Processing Pipeline
GroundTruth Hackathon | Automated Insight Engine

This module handles unstructured data processing:
- Text files (.txt) - logs, notes, feedback
- PDF files (.pdf) - reports, documents
- Markdown files (.md) - documentation
- Uses LLM to extract structured insights from unstructured text
"""

import os
import re
import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# Try to import PDF libraries
try:
    import PyPDF2
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    logger.warning("PyPDF2 not available. PDF processing disabled.")

# Try to import OpenAI for text extraction
try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False


def read_text_file(file_path: Path) -> str:
    """
    Read a text file with encoding detection.
    
    Args:
        file_path: Path to text file
        
    Returns:
        File contents as string
    """
    encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                content = f.read()
            logger.info(f"Read text file: {file_path.name} ({len(content)} chars)")
            return content
        except UnicodeDecodeError:
            continue
    
    raise ValueError(f"Could not read text file {file_path.name}")


def read_pdf_file(file_path: Path) -> str:
    """
    Extract text from PDF file.
    
    Args:
        file_path: Path to PDF file
        
    Returns:
        Extracted text content
    """
    if not PDF_AVAILABLE:
        raise ImportError("PyPDF2 is required for PDF processing. Install with: pip install PyPDF2")
    
    text_content = []
    
    try:
        with open(file_path, 'rb') as f:
            pdf_reader = PyPDF2.PdfReader(f)
            
            for page_num, page in enumerate(pdf_reader.pages):
                page_text = page.extract_text()
                if page_text:
                    text_content.append(page_text)
        
        full_text = "\n\n".join(text_content)
        logger.info(f"Extracted {len(full_text)} chars from PDF: {file_path.name}")
        return full_text
        
    except Exception as e:
        logger.error(f"Failed to read PDF {file_path.name}: {str(e)}")
        raise


def read_markdown_file(file_path: Path) -> str:
    """
    Read markdown file and extract plain text.
    
    Args:
        file_path: Path to markdown file
        
    Returns:
        Plain text content
    """
    content = read_text_file(file_path)
    
    # Remove markdown formatting for cleaner text extraction
    # Remove headers
    content = re.sub(r'^#+\s*', '', content, flags=re.MULTILINE)
    # Remove bold/italic
    content = re.sub(r'\*+([^*]+)\*+', r'\1', content)
    # Remove links but keep text
    content = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', content)
    # Remove images
    content = re.sub(r'!\[([^\]]*)\]\([^)]+\)', '', content)
    # Remove code blocks
    content = re.sub(r'```[\s\S]*?```', '', content)
    # Remove inline code
    content = re.sub(r'`([^`]+)`', r'\1', content)
    
    return content.strip()


def extract_metrics_from_text(text: str) -> Dict[str, Any]:
    """
    Extract numerical metrics from unstructured text using regex patterns.
    
    Args:
        text: Unstructured text content
        
    Returns:
        Dictionary of extracted metrics
    """
    metrics = {}
    
    # Common patterns for metrics in AdTech/marketing text
    patterns = {
        # Percentages
        'percentages': r'(\d+(?:\.\d+)?)\s*%',
        # Dollar amounts
        'dollar_amounts': r'\$\s*(\d+(?:,\d{3})*(?:\.\d{2})?)',
        # Large numbers with K/M/B suffix
        'large_numbers': r'(\d+(?:\.\d+)?)\s*([KMB])\b',
        # Plain numbers with context
        'impressions': r'(\d+(?:,\d{3})*)\s*(?:impressions?|views?)',
        'clicks': r'(\d+(?:,\d{3})*)\s*(?:clicks?)',
        'conversions': r'(\d+(?:,\d{3})*)\s*(?:conversions?)',
        'visitors': r'(\d+(?:,\d{3})*)\s*(?:visitors?|users?)',
        # CTR patterns
        'ctr': r'(?:CTR|click[- ]?through[- ]?rate)[:\s]*(\d+(?:\.\d+)?)\s*%?',
        # Dates
        'dates': r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}[/-]\d{1,2}[/-]\d{1,2})',
    }
    
    for metric_name, pattern in patterns.items():
        matches = re.findall(pattern, text, re.IGNORECASE)
        if matches:
            # Clean and store matches
            if metric_name == 'large_numbers':
                # Convert K/M/B to actual numbers
                converted = []
                for num, suffix in matches:
                    multiplier = {'K': 1000, 'M': 1000000, 'B': 1000000000}.get(suffix.upper(), 1)
                    converted.append(float(num) * multiplier)
                metrics[metric_name] = converted
            elif metric_name in ['impressions', 'clicks', 'conversions', 'visitors']:
                # Clean comma-separated numbers
                metrics[metric_name] = [int(m.replace(',', '')) for m in matches]
            else:
                metrics[metric_name] = matches
    
    logger.info(f"Extracted {len(metrics)} metric types from text")
    return metrics


def extract_entities_from_text(text: str) -> Dict[str, List[str]]:
    """
    Extract named entities from text (campaigns, regions, products, etc.).
    
    Args:
        text: Unstructured text content
        
    Returns:
        Dictionary of extracted entities
    """
    entities = {
        'campaigns': [],
        'regions': [],
        'products': [],
        'dates': [],
        'companies': []
    }
    
    # Campaign patterns
    campaign_patterns = [
        r'(?:campaign|promo|promotion)[:\s]*["\']?([^"\'.,\n]+)["\']?',
        r'["\']([^"\']+(?:campaign|promo|sale|offer))["\']',
    ]
    for pattern in campaign_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        entities['campaigns'].extend([m.strip() for m in matches if len(m.strip()) > 2])
    
    # Region patterns
    regions = ['Northeast', 'Southeast', 'Midwest', 'West', 'Southwest', 'Northwest',
               'North', 'South', 'East', 'Central', 'Pacific', 'Atlantic',
               'USA', 'US', 'Europe', 'Asia', 'APAC', 'EMEA', 'LATAM']
    for region in regions:
        if re.search(rf'\b{region}\b', text, re.IGNORECASE):
            entities['regions'].append(region)
    
    # Date extraction
    date_patterns = [
        r'\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\b',
        r'\b(\d{4}[/-]\d{1,2}[/-]\d{1,2})\b',
        r'\b((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},?\s+\d{4})\b',
    ]
    for pattern in date_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        entities['dates'].extend(matches)
    
    # Remove duplicates
    for key in entities:
        entities[key] = list(set(entities[key]))
    
    logger.info(f"Extracted entities: {sum(len(v) for v in entities.values())} total")
    return entities


def extract_structured_data_with_llm(
    text: str,
    context: str = "AdTech performance data"
) -> Dict[str, Any]:
    """
    Use LLM to extract structured data from unstructured text.
    
    Args:
        text: Unstructured text content
        context: Context about what kind of data to extract
        
    Returns:
        Structured data extracted by LLM
    """
    api_key = os.getenv('OPENAI_API_KEY')
    
    if not api_key or not OPENAI_AVAILABLE:
        logger.warning("OpenAI not available. Using regex-based extraction.")
        return {
            'metrics': extract_metrics_from_text(text),
            'entities': extract_entities_from_text(text),
            'method': 'regex'
        }
    
    try:
        client = OpenAI(api_key=api_key)
        
        prompt = f"""Analyze the following unstructured text and extract structured data relevant to {context}.

TEXT:
{text[:4000]}  # Limit to 4000 chars for API

Please extract and return a JSON object with the following structure:
{{
    "metrics": {{
        "impressions": <number or null>,
        "clicks": <number or null>,
        "conversions": <number or null>,
        "spend": <number or null>,
        "revenue": <number or null>,
        "ctr": <percentage or null>,
        "conversion_rate": <percentage or null>,
        "other_metrics": {{<any other numerical metrics found>}}
    }},
    "entities": {{
        "campaigns": [<list of campaign names>],
        "regions": [<list of regions/locations>],
        "products": [<list of products/categories>],
        "time_periods": [<list of date ranges or periods mentioned>]
    }},
    "key_findings": [<list of 3-5 key insights from the text>],
    "sentiment": "<positive/negative/neutral>",
    "data_quality": "<high/medium/low based on how much structured info was found>"
}}

Return ONLY valid JSON, no other text."""

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": "You are a data extraction specialist. Extract structured data from unstructured text and return valid JSON only."
                },
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=1000
        )
        
        response_text = response.choices[0].message.content.strip()
        
        # Clean up response (remove markdown code blocks if present)
        if response_text.startswith('```'):
            response_text = re.sub(r'^```(?:json)?\n?', '', response_text)
            response_text = re.sub(r'\n?```$', '', response_text)
        
        extracted_data = json.loads(response_text)
        extracted_data['method'] = 'llm'
        
        logger.info("Successfully extracted structured data using LLM")
        return extracted_data
        
    except Exception as e:
        logger.error(f"LLM extraction failed: {str(e)}. Falling back to regex.")
        return {
            'metrics': extract_metrics_from_text(text),
            'entities': extract_entities_from_text(text),
            'method': 'regex_fallback'
        }


def convert_unstructured_to_dataframe(
    extracted_data: Dict[str, Any],
    source_file: str
) -> pd.DataFrame:
    """
    Convert extracted unstructured data to a DataFrame row.
    
    Args:
        extracted_data: Data extracted from unstructured source
        source_file: Name of source file
        
    Returns:
        DataFrame with extracted data
    """
    row_data = {
        'source_file': source_file,
        'extraction_method': extracted_data.get('method', 'unknown'),
    }
    
    # Add metrics
    metrics = extracted_data.get('metrics', {})
    for key, value in metrics.items():
        if isinstance(value, (int, float)):
            row_data[key] = value
        elif isinstance(value, list) and len(value) > 0:
            # Take first value or sum
            if all(isinstance(v, (int, float)) for v in value):
                row_data[f'{key}_total'] = sum(value)
                row_data[f'{key}_count'] = len(value)
    
    # Add entity counts
    entities = extracted_data.get('entities', {})
    for key, value in entities.items():
        if isinstance(value, list):
            row_data[f'{key}_count'] = len(value)
            if value:
                row_data[f'{key}_list'] = ', '.join(str(v) for v in value[:5])
    
    # Add key findings as text
    findings = extracted_data.get('key_findings', [])
    if findings:
        row_data['key_findings'] = ' | '.join(findings)
    
    # Add sentiment
    if 'sentiment' in extracted_data:
        row_data['sentiment'] = extracted_data['sentiment']
    
    return pd.DataFrame([row_data])


def process_unstructured_files(
    file_paths: List[Path]
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Process multiple unstructured files and combine results.
    
    Args:
        file_paths: List of paths to unstructured files
        
    Returns:
        Tuple of (combined DataFrame, processing summary)
    """
    all_dataframes = []
    all_raw_texts = []
    processing_summary = {
        'files_processed': 0,
        'files_failed': 0,
        'total_chars_extracted': 0,
        'extraction_methods': {},
        'errors': []
    }
    
    for file_path in file_paths:
        try:
            # Read file based on extension
            suffix = file_path.suffix.lower()
            
            if suffix == '.txt':
                text = read_text_file(file_path)
            elif suffix == '.pdf':
                text = read_pdf_file(file_path)
            elif suffix in ['.md', '.markdown']:
                text = read_markdown_file(file_path)
            else:
                logger.warning(f"Unsupported file type: {suffix}")
                continue
            
            all_raw_texts.append({
                'file': file_path.name,
                'text': text
            })
            
            # Extract structured data
            extracted = extract_structured_data_with_llm(text)
            
            # Convert to DataFrame
            df = convert_unstructured_to_dataframe(extracted, file_path.name)
            all_dataframes.append(df)
            
            # Update summary
            processing_summary['files_processed'] += 1
            processing_summary['total_chars_extracted'] += len(text)
            method = extracted.get('method', 'unknown')
            processing_summary['extraction_methods'][method] = \
                processing_summary['extraction_methods'].get(method, 0) + 1
            
        except Exception as e:
            logger.error(f"Failed to process {file_path.name}: {str(e)}")
            processing_summary['files_failed'] += 1
            processing_summary['errors'].append({
                'file': file_path.name,
                'error': str(e)
            })
    
    # Combine all DataFrames
    if all_dataframes:
        combined_df = pd.concat(all_dataframes, ignore_index=True)
    else:
        combined_df = pd.DataFrame()
    
    # Store raw texts for LLM context
    processing_summary['raw_texts'] = all_raw_texts
    
    logger.info(f"Processed {processing_summary['files_processed']} unstructured files")
    
    return combined_df, processing_summary


def merge_structured_and_unstructured(
    structured_df: pd.DataFrame,
    unstructured_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Merge structured and unstructured data into a single DataFrame.
    
    Args:
        structured_df: DataFrame from CSV/JSON sources
        unstructured_df: DataFrame from unstructured sources
        
    Returns:
        Combined DataFrame
    """
    if unstructured_df.empty:
        return structured_df
    
    if structured_df.empty:
        return unstructured_df
    
    # Add source type indicator
    structured_df = structured_df.copy()
    structured_df['data_source_type'] = 'structured'
    
    unstructured_df = unstructured_df.copy()
    unstructured_df['data_source_type'] = 'unstructured'
    
    # Concatenate (columns may differ)
    combined = pd.concat([structured_df, unstructured_df], ignore_index=True)
    
    logger.info(f"Merged data: {len(structured_df)} structured + {len(unstructured_df)} unstructured rows")
    
    return combined

