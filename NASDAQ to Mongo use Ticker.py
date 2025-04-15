#!/usr/bin/env python
# coding: utf-8

# In[20]:


import yfinance as yf
import pandas as pd
from tqdm import tqdm
from pymongo import MongoClient
from dotenv import load_dotenv 
import os

load_dotenv() # .env 파일을 불러와 환경변수로 등록 db_password = os.getenv("MONGO_PASSWORD")
db_password = os.getenv("MONGO_PASSWORD")
mongo_url = f"mongodb+srv://skkucapstone:{db_password}@stock.iz5b97b.mongodb.net/?retryWrites=true&w=majority&appName=XXX"


# MongoDB 연결
client = MongoClient(mongo_url)

# ✅ 티커 목록 불러오기 (from DB 'db', collection 'tickers')
tickers_db = client["db"]
tickers_collection = tickers_db["tickers"]

# ✅ 주가 데이터 저장할 곳 (to DB 'stock', collection 'nasdaq_all_2024')
stock_db = client["stock"]
output_collection = stock_db["nasdaq_all_2024"]


# In[21]:


# 1. NASDAQ ticker 목록 가져오기
tickers_cursor = tickers_collection.find({})
nasdaq_tickers = [doc['ticker'] for doc in tickers_cursor if 'ticker' in doc]

print(f"✅ 총 {len(nasdaq_tickers)}개 티커 불러옴")


# In[22]:


# 2. 주가 데이터 수집
all_df = pd.DataFrame()

for ticker in tqdm(nasdaq_tickers):
    try:
        df = yf.download(ticker, start="2024-01-01", end="2024-12-31")

        if df.empty:
            print(f"⚠️ {ticker} - 데이터 없음 (상장 폐지 또는 비활성)")
            continue

        # 컬럼 정리
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)
        df.columns.name = None
        df = df.reset_index()

        # 수익률, 메타 정보 추가
        df['Change'] = df['Close'].pct_change().fillna(0)
        df['Code'] = ticker
        df['Name'] = yf.Ticker(ticker).info.get("shortName", ticker)

        # 컬럼 순서 정리
        df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Change', 'Code', 'Name']]
        df['Date'] = df['Date'].astype(str)

        all_df = pd.concat([all_df, df])

    except Exception as e:
        print(f"❌ {ticker} 오류 발생: {e}")




# In[25]:


# 3. MongoDB에 업로드
if not all_df.empty:
    output_collection.insert_many(all_df.to_dict("records"))
    print("✅ NASDAQ 전체 데이터 MongoDB 업로드 완료!")
else:
    print("⚠️ 업로드할 데이터 없음")


# In[ ]:




