from pymongo import MongoClient
import json
from dotenv import load_dotenv 
import os

load_dotenv()  # .env 파일을 불러와 환경변수로 등록
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

# 종목별로 데이터 분류
cursor = collection.find({})
data_by_company = {}

for doc in cursor:
    name = doc.get("Name")
    if not name or not doc.get("Date"):
        continue
    if name not in data_by_company:
        data_by_company[name] = []
    data_by_company[name].append(doc)

# 날짜순 정렬 및 JSONL 생성

jsonl_str = ""




for name, docs in data_by_company.items():
    docs.sort(key=lambda x: x["Date"])

    for i in range(len(docs) - 11):  # 10일 context + 1일 예측
        try:
            context_lines = []

            # 최근 10일 데이터 수집
            for j in range(i, i + 10):
                d = docs[j]
                if None in [d.get("Open"), d.get("High"), d.get("Low"), d.get("Close"), d.get("Volume"), d.get("Change")]:
                    raise ValueError("데이터 누락")
                line = f"{d['Date']}, {d['Open']}, {d['High']}, {d['Low']}, {d['Close']}, {d['Volume']}, {round(d['Change'], 6)}"
                context_lines.append(line)

            # 예측 대상: 11일째의 등락률
            target_doc = docs[i + 10]
            if "Change" not in target_doc or target_doc["Change"] is None:
                i += 10
                continue

            change = float(target_doc["Change"])
            output_label = classify_change(change)

            instruction = f"""Assess the data to estimate how the closing price of {name} will change on {target_doc['Date']}. 
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
with open("kospi_daily_output_10_days_with_overlap.jsonl", "w", encoding="utf-8") as f:
    f.write(jsonl_str)

print("✅ 수정된 일간 JSONL 생성 완료: kospi_daily_output_10_days_with_overlap.jsonl")
