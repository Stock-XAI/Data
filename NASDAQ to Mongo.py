#!/usr/bin/env python
# coding: utf-8

# In[1]:


import yfinance as yf
import pandas as pd
from tqdm import tqdm
from pymongo import MongoClient
from dotenv import load_dotenv 
import os

load_dotenv() # .env 파일을 불러와 환경변수로 등록 db_password = os.getenv("MONGO_PASSWORD")
db_password = os.getenv("MONGO_PASSWORD")
mongo_url = f"mongodb+srv://skkucapstone:{db_password}@stock.iz5b97b.mongodb.net/?retryWrites=true&w=majority&appName=XXX"

nasdaq_top50 = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'PEP', 'AVGO', 'COST',
    'CSCO', 'ADBE', 'TXN', 'AMAT', 'QCOM', 'INTC', 'AMD', 'INTU', 'ISRG', 'VRTX',
    'SBUX', 'ADP', 'REGN', 'GILD', 'PYPL', 'MDLZ', 'MU', 'BKNG', 'LRCX', 'PANW',
    'KDP', 'MAR', 'MNST', 'FTNT', 'ROST', 'ATVI', 'CTAS', 'BIIB', 'KLAC', 'EXC',
    'WBD', 'EA', 'IDXX', 'CDNS', 'DLTR', 'CHTR', 'ANSS', 'ILMN', 'EBAY', 'WBA'
]


# In[2]:


# 📦 MongoDB 연결

client = MongoClient(mongo_url)
db = client['stock']
collection = db['nasdaq_top50_2024']

# 📌 결과를 모을 DataFrame
all_df = pd.DataFrame()


# In[14]:


# 📈 반복 수집
for ticker in tqdm(nasdaq_top50):
    try:
        df = yf.download(ticker, start="2024-01-01", end="2024-12-31")

        # 멀티 인덱스 컬럼 정리
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)
        df.columns.name = None

        # 인덱스 정리
        df = df.reset_index()
        df['Change'] = df['Close'].pct_change().fillna(0)
        df['Code'] = ticker
        df['Name'] = yf.Ticker(ticker).info.get("shortName", ticker)  # 기업 이름 불러오기
        df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Change', 'Code', 'Name']]
        df = df.set_index(keys='Date')

        all_df = pd.concat([all_df, df])

    except Exception as e:
        print(f"❌ {ticker} 오류 발생: {e}")

# 📤 MongoDB에 업로드
all_df = all_df.reset_index()  # MongoDB에는 인덱스가 문자열로 있어야 좋음
all_df['Date'] = all_df['Date'].astype(str)


# In[22]:


all_df = all_df.reset_index()


# In[24]:


collection.insert_many(all_df.to_dict('records'))

print("✅ NASDAQ Top 50 데이터 MongoDB 업로드 완료!")


# In[ ]:




