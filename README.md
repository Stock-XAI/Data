# 📊 KOSPI & NASDAQ Stock Data Collector & Formatter

A Python-based pipeline to collect KOSPI and NASDAQ stock data, upload it to MongoDB Atlas, and convert it into FinMA-compatible training datasets in both regression and classification formats.

---

## 📁 Project Structure

project-directory/
├── multiclass/
│ └── \[Multiclass dataset generation notebooks]
├── regression/
│ └── \[Regression dataset generation notebooks]
├── (No DB) kospi_daily_jsonl.ipynb
├── (No DB) kospi_weekly_jsonl.ipynb
├── (No DB) kospi_monthly_jsonl.ipynb
├── (No DB) nasdaq_jsonl_parser.ipynb
├── (No DB) regression_kospi_daily_jsonl.ipynb
├── (No DB) regression_kospi_jsonl.ipynb
├── (No DB) regression_nasdaq_jsonl_parser.ipynb
├── KOSPI to Mongo.py
├── NASDAQ to Mongo.py
├── NASDAQ to Mongo use Ticker.py
├── kospi_daily_jsonl_5_days.py
├── kospi_daily_jsonl_10_days(with overlap).py
├── kospi_monthly_jsonl_10_months.py
├── kospi_weekly_jsonl_10_weeks.py
├── 0528_FastSHAP.ipynb
├── generate_masked_prompts.ipynb
├── .gitignore
├── requirements.txt
└── README.md

````

---

## ⚙️ Features

- ✅ Collect KOSPI & NASDAQ stock data (Top 50)
- ✅ Upload structured data to MongoDB Atlas
- ✅ Generate training data in `.jsonl` format for:
  - Multi-class classification
  - Regression prediction (e.g., % change)
- ✅ Support various time frames (daily, weekly, monthly)
- ✅ Implement SHAP-based feature importance analysis

---

## 🛠️ Main Scripts Overview

### 🗃 MongoDB Ingestion
- `KOSPI to Mongo.py`: Fetch and upload KOSPI top 50 stock data
- `NASDAQ to Mongo.py`: Fetch NASDAQ data using hardcoded tickers
- `NASDAQ to Mongo use Ticker.py`: Fetch NASDAQ data using tickers from MongoDB

### 📄 Dataset Generation
- `(No DB) *.ipynb`: Generate `.jsonl` data directly from raw FinanceDataReader or yfinance output (no MongoDB dependency)
- `kospi_daily_jsonl_5_days.py`: Create 5-day window JSONL files
- `kospi_daily_jsonl_10_days(with overlap).py`: Create overlapping 10-day JSONL dataset
- `kospi_monthly_jsonl_10_months.py`: Monthly overlapping dataset
- `kospi_weekly_jsonl_10_weeks.py`: Weekly overlapping dataset

### 🧠 Feature Explanation & Prompting
- `0528_FastSHAP.ipynb`: FastSHAP implementation for regression feature importance
- `generate_masked_prompts.ipynb`: Prompt generation for language model training

---

## 🔧 Setup

1. Clone the repository:

```bash
git clone https://github.com/yourusername/your-repo-name.git
cd your-repo-name
````

2. Set up environment variables:

Create a `.env` file with:

```env
MONGO_PASSWORD=your_mongodb_password
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

---

## 📚 Requirements

All dependencies are listed in `requirements.txt`. Major packages:

- `FinanceDataReader`
- `yfinance`
- `pandas`
- `pymongo`
- `tqdm`
- `python-dotenv`

(You can auto-generate the list using `pip freeze > requirements.txt`.)

---

## 💬 Contact

For questions or contributions, please contact: **\[Your Name or GitHub handle here]**

```

---

필요 시, 프로젝트 목적 및 기능 설명도 추가 가능합니다. GitHub에 업로드하시려면 이 내용을 `README.md` 파일로 덮어쓰기 하시면 됩니다. 수정이나 보완이 필요하면 말씀해주세요!
```
