# 🚀 TrendSpotter: The Automated Insight Engine

> **GroundTruth Mini AI Hackathon | Track: Data Engineering & Analytics**

![Python](https://img.shields.io/badge/Python-3.11+-blue?style=flat&logo=python)
![FastAPI](https://img.shields.io/badge/FastAPI-0.109-green?style=flat&logo=fastapi)
![OpenAI](https://img.shields.io/badge/OpenAI-GPT--4o-orange?style=flat&logo=openai)
![License](https://img.shields.io/badge/License-MIT-yellow?style=flat)

**Tagline:** *An event-driven data pipeline that converts raw CSV/JSON data into executive-ready PDF/PPTX reports with AI-generated narratives in under 60 seconds.*

---

## 📋 Table of Contents

- [The Problem](#1-the-problem-real-world-scenario)
- [The Solution](#2-the-solution)
- [Demo & Results](#3-demo--expected-results)
- [Technical Architecture](#4-technical-architecture)
- [Tech Stack](#5-tech-stack)
- [Features](#6-key-features)
- [How to Run](#7-how-to-run)
- [API Documentation](#8-api-documentation)
- [Challenges & Learnings](#9-challenges--learnings)

---

## 1. The Problem (Real World Scenario)

### Context
In the AdTech world, **terabytes of data are generated daily** — foot traffic logs, ad clickstreams, and weather reports. Currently, Account Managers manually:
- Download CSVs from multiple sources
- Take screenshots of dashboards
- Copy-paste data into PowerPoint slides
- Write analysis summaries by hand

### The Pain Point
This manual process takes **4-6 hours per week per account manager**. It's:
- ⏰ **Slow** — Reports are always late
- 😴 **Boring** — Repetitive, soul-crushing work  
- ❌ **Error-prone** — Human mistakes in data interpretation
- 💸 **Costly** — If a campaign is wasting budget, clients don't know for days

---

## 2. The Solution

I built **TrendSpotter**, an automated insight engine that:

```
📁 Raw Data (CSV/JSON) → 🔄 Pipeline → 📊 Charts + 🤖 AI Insights → 📄 PDF/PPTX Report
```

### How It Works
1. **Upload** raw data files via API
2. **Wait** ~60 seconds
3. **Download** a professionally formatted report containing:
   - 📈 Auto-generated performance charts
   - 📊 KPI summary tables (CTR, CPC, ROAS, etc.)
   - 🔍 Anomaly detection alerts
   - ✍️ AI-written executive summary & recommendations

---

## 3. Demo & Expected Results

### 🎨 Beautiful Web UI

TrendSpotter includes a modern, animated web interface:

![UI Screenshot](visual_outputs/ui_screenshot.png)

**Features:**
- 🎯 Drag & drop file upload
- ⚡ Real-time progress tracking (6-step pipeline visualization)
- 📊 Live console output
- 🎉 One-click report download
- 📋 Recent reports gallery

**Access:** Simply visit `http://localhost:8000` after starting the server!

### Input
```
POST /upload
Files: sample_adtech_data.csv
```

### Output (PDF Report Contains)

| Section | Description |
|---------|-------------|
| **Cover Page** | Branded title page with report metadata |
| **Executive Summary** | AI-generated 2-3 sentence overview |
| **KPI Dashboard** | CTR, CPC, CPA, ROAS metrics in visual cards |
| **Performance Charts** | Daily trends, category comparisons, pie charts |
| **Key Highlights** | Top 5 positive findings |
| **Issues Detected** | Anomalies and declining metrics |
| **Recommendations** | 3 actionable, data-driven suggestions |

### Sample Output
```
📊 Total Impressions: 4,567,000
📈 Overall CTR: 3.12%
💰 Total Revenue: $128,500
🎯 ROAS: 2.81x

⚠️ Anomaly Detected: Traffic dropped 40% in Midwest region
💡 AI Insight: "The traffic decline correlates with severe weather 
   conditions. Recommend increasing mobile bid adjustments by 15%."
```

---

## 4. Technical Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        CLIENT REQUEST                            │
│                    (CSV/JSON/TXT/PDF Upload)                     │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                         FASTAPI SERVER                           │
│                      (REST API Endpoints)                        │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    BACKGROUND PIPELINE                           │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────┐   ┌───────────┐   ┌──────────┐   ┌─────────────┐  │
│  │  INGEST  │ → │ TRANSFORM │ → │   KPIs   │ → │   CHARTS    │  │
│  │  Data    │   │   Clean   │   │  Compute │   │  Generate   │  │
│  └──────────┘   └───────────┘   └──────────┘   └─────────────┘  │
│        │                                              │          │
│        ▼                                              ▼          │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │                    LLM INSIGHTS                           │   │
│  │               (OpenAI GPT-4o Analysis)                    │   │
│  └──────────────────────────────────────────────────────────┘   │
│                              │                                   │
│                              ▼                                   │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │              REPORT GENERATION                            │   │
│  │           (ReportLab PDF / python-pptx)                   │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                      DOWNLOADABLE REPORT                         │
│                       (PDF or PPTX)                              │
└─────────────────────────────────────────────────────────────────┘
```

### Pipeline Steps

| Step | Module | Description |
|------|--------|-------------|
| 1 | `ingest.py` | Read CSV/JSON, merge sources, normalize columns |
| 2 | `transform.py` | Parse dates, handle missing values, compute derived metrics |
| 3 | `kpis.py` | Calculate CTR, CPC, ROAS, detect anomalies, find correlations |
| 4 | `charts.py` | Generate Matplotlib visualizations (line, bar, pie, heatmap) |
| 5 | `insights.py` | Call OpenAI GPT-4o for executive summary |
| 6 | `generate_pdf.py` | Build professional PDF with ReportLab |
| 7 | `generate_ppt.py` | Build PowerPoint deck with python-pptx |

---

## 5. Tech Stack

| Category | Technology | Why? |
|----------|------------|------|
| **Backend** | FastAPI | Async, fast, auto-docs |
| **Data Processing** | Pandas + NumPy | Industry standard for data manipulation |
| **Anomaly Detection** | SciPy (Z-score) | Statistical outlier detection |
| **Visualization** | Matplotlib | Publication-quality charts |
| **AI/LLM** | OpenAI GPT-4o | Best-in-class text generation |
| **PDF Generation** | ReportLab | Programmatic PDF creation |
| **PPTX Generation** | python-pptx | Native PowerPoint support |
| **Unstructured Data** | PyPDF2 + Regex + LLM | Extract insights from text/PDFs |

---

## 6. Key Features

### ✅ Multi-Source Data Ingestion
- CSV, JSON (structured)
- TXT, PDF, Markdown (unstructured)
- Auto-merge multiple files
- Smart column normalization

### ✅ Intelligent KPI Computation
- CTR, CPC, CPM, CPA, ROAS
- Period-over-period comparisons
- Anomaly detection (Z-score based)
- Correlation analysis

### ✅ AI-Powered Insights
- Executive summary generation
- Key highlights extraction
- Issue identification
- Actionable recommendations

### ✅ Professional Reports
- Branded PDF reports
- PowerPoint presentations
- Embedded charts and tables
- Clean, executive-ready formatting

---

## 7. How to Run

### Prerequisites
- Python 3.10+
- OpenAI API Key (optional, for AI insights)

### Quick Start

```bash
# 1. Clone Repository
git clone https://github.com/nani-tyson/GT-hackathon.git
cd GT-hackathon/problem-1/backend

# 2. Create Virtual Environment
python -m venv venv

# Windows (Git Bash)
source venv/Scripts/activate

# Windows (PowerShell)
.\venv\Scripts\Activate.ps1

# macOS/Linux
source venv/bin/activate

# 3. Install Dependencies
pip install -r requirements.txt

# 4. Set OpenAI API Key (Optional - for AI insights)
export OPENAI_API_KEY="your-api-key-here"   # Linux/Mac
set OPENAI_API_KEY=your-api-key-here        # Windows CMD

# 5. Run the Server
uvicorn main_simple:app --reload --port 8000

# 6. Open API Docs
# Visit: http://localhost:8000/docs
```

### Test the Pipeline

```bash
# Step 1: Upload sample data
# Use Swagger UI at http://localhost:8000/docs
# Or use the provided sample files in /data folder

# Step 2: Generate report
POST /generate-report?upload_id=YOUR_UPLOAD_ID&report_format=pdf

# Step 3: Download when ready
GET /download/YOUR_REPORT_ID
```

---

## 8. API Documentation

### Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | Health check |
| `POST` | `/upload` | Upload CSV/JSON/TXT/PDF files |
| `POST` | `/generate-report` | Trigger report generation |
| `GET` | `/status/{report_id}` | Check generation status |
| `GET` | `/download/{report_id}` | Download completed report |
| `GET` | `/reports` | List all reports |

### Example Request

```bash
# Upload file
curl -X POST "http://localhost:8000/upload" \
  -F "files=@sample_adtech_data.csv"

# Response
{
  "status": "success",
  "upload_id": "abc123-def456",
  "message": "Successfully uploaded 1 file(s)"
}

# Generate report
curl -X POST "http://localhost:8000/generate-report?upload_id=abc123-def456&report_format=pdf"

# Response
{
  "status": "processing",
  "report_id": "xyz789",
  "download_url": "/download/xyz789"
}
```

---

## 9. Challenges & Learnings

### Challenge 1: AI Hallucinations
**Issue:** The AI would invent statistics that weren't in the data.

**Solution:** Implemented a "Strict Context" system prompt with validation:
```python
prompt = """Only use metrics from the provided JSON context.
If data is unavailable, say 'Data not available'.
Never invent or estimate numbers."""
```

### Challenge 2: Unstructured Data Processing
**Issue:** How to extract meaningful metrics from free-form text reports?

**Solution:** Two-layer extraction:
1. **Regex patterns** for known metrics (percentages, dollar amounts)
2. **LLM extraction** for semantic understanding with JSON output

### Challenge 3: Package Compatibility
**Issue:** `python-pptx` changed their color API between versions.

**Solution:** Used `RGBColor` instead of `RgbColor` for newer versions.

---

## 📁 Project Structure

```
problem-1/
├── backend/
│   ├── main_simple.py          # FastAPI application
│   ├── requirements.txt        # Python dependencies
│   ├── pipeline/
│   │   ├── ingest.py          # Data ingestion
│   │   ├── transform.py       # Data transformation
│   │   ├── kpis.py            # KPI computation
│   │   ├── charts.py          # Chart generation
│   │   ├── insights.py        # LLM insights
│   │   ├── generate_pdf.py    # PDF report
│   │   └── generate_ppt.py    # PowerPoint report
│   ├── data/                   # Sample data files
│   └── reports/               # Generated reports
└── README.md
```

---

## 🏆 Why This Solution Stands Out

1. **Production-Ready Architecture** — Not a Jupyter notebook, but a deployable API
2. **Handles Both Structured & Unstructured Data** — Goes beyond CSV parsing
3. **AI-Powered Insights** — Not just charts, but narrative analysis
4. **Multiple Output Formats** — PDF for executives, PPTX for presentations
5. **Extensible Pipeline** — Easy to add new data sources or report types

---

## 📄 License

MIT License - Feel free to use and modify for your own projects.

---

## 👤 Author

**GroundTruth Hackathon Submission**

- GitHub: [@nani-tyson](https://github.com/nani-tyson)
- Repository: [GT-hackathon](https://github.com/nani-tyson/GT-hackathon)

---

<p align="center">
  <b>Built with ❤️ for the GroundTruth Mini AI Hackathon</b>
</p>
