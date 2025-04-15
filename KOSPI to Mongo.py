#!/usr/bin/env python
# coding: utf-8

# In[3]:


import FinanceDataReader as fdr
import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm
from dotenv import load_dotenv 
import os

load_dotenv() # .env 파일을 불러와 환경변수로 등록 db_password = os.getenv("MONGO_PASSWORD")
db_password = os.getenv("MONGO_PASSWORD")
mongo_url = f"mongodb+srv://skkucapstone:{db_password}@stock.iz5b97b.mongodb.net/?retryWrites=true&w=majority&appName=XXX"



# 1. MongoDB 연결
client = MongoClient(mongo_url)
db = client['stock']
collection = db['kospi_top50_2024']  # 실제용 컬렉션


# In[5]:


# 2. KOSPI 시가총액 상위 50개 종목 가져오기
kospi = fdr.StockListing('KOSPI')
top50 = kospi.sort_values(by='Marcap', ascending=False).head(50)
codes = top50[['Code', 'Name']].reset_index(drop=True)


# In[7]:


# 3. 모든 데이터 수집 및 업로드
all_df = pd.DataFrame()

for i, row in tqdm(codes.iterrows(), total=len(codes)):
    try:
        df = fdr.DataReader(row['Code'], start='2024-01-01', end='2024-12-31')
        df['Code'] = row['Code']
        df['Name'] = row['Name']
        df = df.reset_index()
        df['Date'] = df['Date'].astype(str)  # 날짜는 문자열로 변환
        all_df = pd.concat([all_df, df])
    except Exception as e:
        print(f"❌ Error loading {row['Name']} ({row['Code']}): {e}")


# In[8]:


# 4. MongoDB에 업로드
collection.insert_many(all_df.to_dict('records'))

print("✅ KOSPI Top 50 종목 MongoDB 업로드 완료!")

