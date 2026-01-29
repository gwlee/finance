import os
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, BulkWriteError
from curl_cffi import requests
import ssl, certifi
import time
import random
import numpy as np

MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "finance_db"

HEADERS = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/136.0.0.0 Safari/537.36'
}

# SSL 인증서 문제 해결용
ssl_context = ssl.create_default_context(cafile=certifi.where())

# ✅ 전역 세션 1회 생성 (모든 Ticker가 공유)
session = requests.Session(impersonate="chrome", headers=HEADERS, verify=False)

def update_metadata(symbol: str, collection_name):
    today = datetime.now()
    collection = db[collection_name]
    try:
        collection.update_one(
            {"symbol":symbol},
            {"$set":{"updated_at":today}}
        )
        print(f"[{symbol}]'s metadata 업데이트 완료")
    except Exception as e:
        print(f"[{symbol}]'s metadata 업데이트 오류: {e}")
        

def fetch_and_insert_ticker(yf_symbol: str, symbol: str, collection, start_date):
    """다운로드 받을 날짜(start_date)를 기준으로 스크립트를 작성하기 전 날 데이터를 수집"""
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    collection = db[collection_name]

    try:
        stock = yf.Ticker(yf_symbol, session=session)
        df = stock.history(start=start_date, end=yesterday.strftime("%Y-%m-%d"))
    except Exception as e:
        print (f"{symbol} 다운로드 실패: {e}")
        return

    if df.empty:
        print (f"[{symbol}] 데이터 없음 또는 잘못된 심볼")

    df = df.reset_index()
    documents = []
    
    for _, row in df.iterrows():
        try:
            date_obj = pd.to_datetime(row['Date']).to_pydatetime()
        except Exception:
            continue
            
        volume_value = int(row['Volume']) if not pd.isna(row['Volume']) else 0
        doc = {
            "symbol": symbol,
            "date": date_obj,
            "open": float(row["Open"]),
            "high": float(row["High"]),
            "low": float(row["Low"]),
            "close": float(row["Close"]),
            "volume": volume_value,
        }

        if "Dividends" in df.columns:
            doc["dividends"] = float(row["Dividends"])
        if "Stock Splits" in df.columns:
            doc["stock_splits"] = float(row["Stock Splits"])

        documents.append(doc)

    if not documents:
        print (f"[{symbol}] 삽입할 데이터 없음")
        return

    try:
        collection.insert_many(documents, ordered=False)
        print(f"[{symbol}] {len(documents)}건 삽입 완료")
    except BulkWriteError as bwe:
        duplicates = len([e for e in bwe.details.get("writeErrors", []) if e.get("code") == 11000])
        inserted = len(documents) - duplicates
        print(f"[{symbol}] {inserted}건 삽입, {duplicates}건 중복")
    except Exception as e:
        print(f"[{symbol}] DB 삽입 오류: {e}")
      
def get_latest_dates_from_mongo(collection_name, symbol):
    """mongodb에서 각 심볼별 마지막 업데이트 되어 있는 날짜 조회"""
    latest_dates = {}
    collection = db[collection_name]
    doc = collection.find_one({"symbol":symbol},sort=[("date",-1)])
    if not doc:
        latest_dates = datetime(1900, 1, 1)
    else:
        latest_dates = doc["date"].date()+timedelta(days=1)

    return latest_dates    

"""mongoDB 접속, ticker/symbol 정보를 가지고 있는 metadata에서 리스트 확보"""
collection_meta = "metadata"
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = client[DATABASE_NAME]
collection = db[collection_meta]

i=0
for doc in collection.find():
    symbol = doc["symbol"]
    if doc["type"] == "stock": #주식정보(한국,미국)
        if doc["country"] == "KR":
            collection_name = "kr_stocks"
            if doc["market"] == "KOSPI":
                print ("KS","KOSPI",doc)
                start_date = get_latest_dates_from_mongo(collection_name,symbol)
                print (f"{symbol}.KS",start_date)
                fetch_and_insert_ticker(f"{symbol}.KS",symbol,collection_name,start_date)
                update_metadata(symbol,collection_meta)
            else:
                print ("KQ","KOSDAQ",doc)
                start_date = get_latest_dates_from_mongo(collection_name,symbol)
                print (f"{symbol}.KQ",start_date)
                fetch_and_insert_ticker(f"{symbol}.KQ",symbol,collection_name,start_date)
                update_metadata(symbol,collection_meta)
        else:
            collection_name = "us_stocks"
            print ("US","STOCK",doc)
            start_date = get_latest_dates_from_mongo(collection_name,symbol)
            print (f"{symbol}",start_date)
            fetch_and_insert_ticker(symbol,symbol,collection_name,start_date)
            update_metadata(symbol,collection_meta)
            
    elif doc["type"] == "ETF": #ETF정보(한국,미국)
        if doc["country"] == "KR":
            collection_name = "kr_stocks"
            print ("KR","ETF",doc)
            start_date = get_latest_dates_from_mongo(collection_name,symbol)
            print (f"{symbol}.KS",start_date)
            fetch_and_insert_ticker(f"{symbol}.KS",symbol,collection_name,start_date)
            update_metadata(symbol,collection_meta)
        else:
            collection_name = "us_stocks"
            print ("US","ETF",doc)
            start_date = get_latest_dates_from_mongo(collection_name,symbol)
            print (f"{symbol}",start_date)
            fetch_and_insert_ticker(symbol,symbol,collection_name,start_date)
            update_metadata(symbol,collection_meta)
            
    elif doc["type"] == "indices": #지수정보(한국,미국 및 각 국가별 주요 지수)
        collection_name = "indices"
        print ("indices",doc)
        start_date = get_latest_dates_from_mongo(collection_name,symbol)
        print (f"{symbol}",start_date)
        fetch_and_insert_ticker(symbol,symbol,collection_name,start_date)
        update_metadata(symbol,collection_meta)
        
    elif doc["type"] == "currencies": #통화정보 (달라,원,엔 등 주요 통화)
        collection_name = "currencies"
        print ("currencies",doc)
        start_date = get_latest_dates_from_mongo(collection_name,symbol)
        print (f"{symbol}",start_date)
        fetch_and_insert_ticker(symbol,symbol,collection_name,start_date)
        update_metadata(symbol,collection_meta)
        
    else:
        pass

    i+=1

    if i%100 == 0:
        time.sleep(random.random())
