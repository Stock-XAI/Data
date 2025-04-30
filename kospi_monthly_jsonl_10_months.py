from pymongo import MongoClient
import pandas as pd
import json
from dotenv import load_dotenv 
import os

# 환경변수 로드
load_dotenv()
db_password = os.getenv("MONGO_PASSWORD")
mongo_url = f"mongodb+srv://skkucapstone:{db_password}@stock.iz5b97b.mongodb.net/?retryWrites=true&w=majority&appName=XXX"

def classify_change(rate):
    if rate >= 0.05:
        return "Strong Rise (≥ 5%)"
    elif rate >= 0.02:
        return "Rise (2%–4.99%)"
    elif rate >= 0.0:
        return "Slight Rise (0%–1.99%)"
    elif rate > -0.02:
        return "Slight Fall (–1.99% to 0%)"
    elif rate > -0.05:
        return "Fall (–4.99% to –2%)"
    else:
        return "Strong Fall (≤ –5%)"

# MongoDB 연결
client = MongoClient(mongo_url)
db = client["stock"]
collection = db["kospi_top50_2024"]

# 종목별 데이터 분류
cursor = collection.find({})
data_by_company = {}

for doc in cursor:
    name = doc.get("Name")
    if not name or not doc.get("Date"):
        continue
    if name not in data_by_company:
        data_by_company[name] = []
    data_by_company[name].append(doc)

# 월간 JSONL 생성
jsonl_str = ""

for name, docs in data_by_company.items():
    # 날짜 오름차순 정렬
    docs.sort(key=lambda x: x["Date"])

    # 일간 데이터 → DataFrame 변환
    df = pd.DataFrame(docs)
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.set_index('Date')

    # 월간 데이터 리샘플링
    monthly_df = df.resample('M').agg({
        'Open': 'first',
        'High': 'max',
        'Low': 'min',
        'Close': 'last',
        'Volume': 'sum'
    }).dropna()

    # 월간 Change 계산 (Open 대비 Close 수익률)
    monthly_df['Change'] = (monthly_df['Close'] - monthly_df['Open']) / monthly_df['Open']
    monthly_df = monthly_df.reset_index()

    # 슬라이딩 윈도우 적용 (1달씩 이동)
    for i in range(len(monthly_df) - 10):
        try:
            context_lines = []

            # 최근 10개월 데이터 수집
            for j in range(i, i + 10):
                row = monthly_df.iloc[j]
                line = f"{row['Date'].strftime('%Y-%m-%d')}, {row['Open']}, {row['High']}, {row['Low']}, {row['Close']}, {row['Volume']}, {round(row['Change'], 6)}"
                context_lines.append(line)

            # 예측 대상: 11번째 달 Close
            target_row = monthly_df.iloc[i + 10]
            target_close = target_row['Close']

            # 전 달 대비 등락률
            prev_close = monthly_df.iloc[i + 9]['Close']
            rate = (target_close - prev_close) / prev_close
            output_label = classify_change(rate)

            instruction = f"""Assess the data to estimate how the closing price of {name} will change on {target_row['Date'].strftime('%Y-%m-%d')}. 
Respond with one of the following levels based on the rate of change: 
Strong Rise (≥ 5%), Rise (2%–4.99%), Slight Rise (0%–1.99%), Slight Fall (–1.99% to 0%), 
Fall (–4.99% to –2%), or Strong Fall (≤ –5%).

Context: date, open, high, low, close, volume, change.
{chr(10).join(context_lines)}
Answer:"""

            json_obj = {
                "instruction": instruction,
                "output": output_label
            }
            jsonl_str += json.dumps(json_obj, ensure_ascii=False) + "\n"

        except Exception as e:
            print(f"❗ {name}에서 오류 발생: {e}")
            continue

# 파일 저장
with open("kospi_monthly_output_10_months.jsonl", "w", encoding="utf-8") as f:
    f.write(jsonl_str)

print("✅ 월간 JSONL 생성 완료: kospi_monthly_output_10_months.jsonl")
