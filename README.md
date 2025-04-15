# 📊 KOSPI & NASDAQ Stock Data Collector

A Python-based pipeline to collect stock data from KOSPI and NASDAQ markets and upload it to MongoDB Atlas, as well as convert it to FinMA-compatible training data format.

---

## 📁 Project Structure

project-directory/
├── KOSPI to Mongo.py                     # Collects KOSPI top 50 data and uploads to MongoDB
├── NASDAQ to Mongo.py                   # Collects NASDAQ top 50 data using hard-coded tickers
├── NASDAQ to Mongo use Ticker.py       # Collects NASDAQ data using tickers stored in MongoDB
├── kospi_daily_jsonl_5_days.py         # Converts KOSPI MongoDB data into FinMA training format
├── kospi_daily_output_5_days.jsonl     # Generated KOSPI training data
├── nasdaq_daily_5_days.py              # Converts NASDAQ MongoDB data into FinMA training format
├── nasdaq_daily_jsonl_5_days.jsonl     # Generated NASDAQ training data
├── .env                                 # MongoDB password (excluded from GitHub)
├── .gitignore
├── requirements.txt
└── README.md

---

## ⚙️ Features

- ✅ Fetch and upload KOSPI top 50 stock data to MongoDB Atlas  
- ✅ Fetch and upload NASDAQ stock data (using either MongoDB-stored tickers or hard-coded tickers)  
- ✅ Transform stock data stored in MongoDB into FinMA-compatible training dataset (.jsonl format)  
- ✅ Secure MongoDB credential handling using dotenv and .env  
- ✅ Supports both data ingestion and preprocessing for training

---

## 📦 How the Code Works

### KOSPI to Mongo.py
- Uses FinanceDataReader to fetch KOSPI top 50 stocks
- Uploads structured data to MongoDB Atlas

### NASDAQ to Mongo use Ticker.py
- Reads NASDAQ tickers stored in MongoDB
- Uses yfinance to fetch corresponding stock data
- Uploads to MongoDB

### NASDAQ to Mongo.py
- Uses hard-coded top 50 NASDAQ tickers
- Fetches data using yfinance and uploads to MongoDB

### kospi_daily_jsonl_5_days.py
- Fetches KOSPI data from MongoDB
- Converts the last 5 days of data into FinMA .jsonl format

### nasdaq_daily_5_days.py
- Same as above, but for NASDAQ

---

## 🔧 Setup

1. Clone the repository

```bash
git clone https://github.com/yourusername/stock-data-uploader.git
cd stock-data-uploader

2. Create a .env file and add your MongoDB password:

MONGO_PASSWORD=your_mongodb_password

3. Install dependencies:

pip install -r requirements.txt

---

📚 Requirements

The following libraries are required and included in requirements.txt:

FinanceDataReader
yfinance
pandas
pymongo
tqdm
python-dotenv
(자동으로 생성하려면 pip freeze > requirements.txt 명령어를 사용할 수도 있어요.)

---

💬 Contact
TBD
