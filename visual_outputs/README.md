# 📸 Visual Outputs - TrendSpotter

This folder contains visual proof of the Automated Insight Engine in action.

## 📁 Folder Structure

```
visual_outputs/
├── screenshots/          # UI and process screenshots
│   ├── 01_ui_dashboard.png
│   ├── 02_file_upload.png
│   ├── 03_processing.png
│   ├── 04_report_ready.png
│   └── ...
├── output/               # Generated reports
│   ├── sample_report.pdf
│   ├── sample_report.pptx
│   └── charts/
└── README.md
```

## 📷 Screenshots to Capture

| # | Screenshot | Description |
|---|------------|-------------|
| 1 | `ui_dashboard.png` | Initial TrendSpotter UI |
| 2 | `file_upload.png` | After selecting/dragging files |
| 3 | `upload_success.png` | Toast showing upload success |
| 4 | `processing.png` | Pipeline progress steps |
| 5 | `report_ready.png` | Completed with download button |
| 6 | `pdf_report.png` | Generated PDF opened |
| 7 | `pptx_report.png` | Generated PPTX opened |

## 📊 Output Files

- `sample_report.pdf` - Generated PDF report from sample data
- `sample_report.pptx` - Generated PowerPoint presentation
- `charts/` - Individual chart images (daily_performance.png, etc.)

## 🎯 How to Capture

1. **Start server:** `uvicorn main_simple:app --port 8000`
2. **Open:** `http://localhost:8000`
3. **Use Windows Snipping Tool** or `Win + Shift + S`
4. **Save screenshots** with descriptive names
5. **Copy generated reports** from `backend/reports/` folder

## 📝 Notes

- Screenshots should be clear and show the full UI
- Include the browser URL bar if possible
- Generated reports demonstrate actual pipeline output
