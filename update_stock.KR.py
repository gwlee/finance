# -*- coding:utf-8 -*-
import sqlite3
import yfinance as yf
from datetime import datetime, timedelta
import time

# SQLite 데이터베이스 연결
conn = sqlite3.connect('stocks.db')
cursor = conn.cursor()

# stocks 테이블이 존재하지 않으면 생성
cursor.execute('''
CREATE TABLE IF NOT EXISTS stocks (
    symbol TEXT,
    date DATE,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume INTEGER,
    dividends REAL,
    stock_splits TEXT
)
''')


# 현재 날짜와 어제 날짜 계산
today = datetime.now().date()
yesterday = today - timedelta(days=1)

# 가장 최신의 주식 정보 날짜를 얻는 함수
def get_latest_date(symbol):
    cursor.execute("SELECT MAX(date) FROM stocks WHERE symbol=?", (symbol,))
    result = cursor.fetchone()
    if result[0]:
        return datetime.strptime(result[0], "%Y-%m-%d").date()
    return None

# 주식 데이터를 가져오는 함수
def fetch_stock_data(symbol, start_date):
    stock = yf.Ticker(symbol)
    # 주식 데이터를 start_date부터 어제까지 불러옴
    hist = stock.history(start=start_date, end=today)
    return hist

# 주식 데이터를 테이블에 삽입하는 함수
def insert_stock_data(symbol, data):
    for idx, row in data.iterrows():
        date = idx.date()
        # 중복 여부 확인 쿼리
        cursor.execute('''
        SELECT COUNT(*) FROM stocks WHERE symbol = ? AND date = ?
        ''', (symbol, date))
        count = cursor.fetchone()[0]

        if count == 0:
            # 중복이 없을 때만 INSERT
            cursor.execute('''
            INSERT INTO stocks (symbol, date, open, high, low, close, volume, dividends, stock_splits)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (symbol, date, row['Open'], row['High'], row['Low'], row['Close'], row['Volume'], row['Dividends'], row['Stock Splits']))
            print(f"{symbol} {date} 데이터가 업데이트 되었습니다.")
        else:
            print(f"{symbol} {date} 데이터가 이미 존재합니다. 업데이트되지 않았습니다.")        

# 업데이트 함수
def update_stock_data(symbol):
    # DB에 기록된 가장 최근 날짜 확인
    latest_date = get_latest_date(symbol)
    
    if latest_date is None:
        # 데이터가 없으면 처음부터 데이터를 가져옴
        print("DB에 저장된 데이터가 없습니다. 전체 데이터를 가져옵니다.")
        try:
            stock_data = fetch_stock_data('{}.KS'.format(symbol), "1900-01-01")
        except:
            stock_data = fetch_stock_data('{}.KQ'.format(symbol), "1900-01-01")
    elif latest_date < yesterday:
        # 가장 최근 날짜 이후로 데이터가 있으면 그 이후부터 데이터를 가져옴
        print(f"{latest_date} 이후로 데이터를 업데이트합니다.")
        start_date = latest_date + timedelta(days=1)
        try:
            stock_data = fetch_stock_data('{}.KS'.format(symbol), start_date)
        except:
            stock_data = fetch_stock_data('{}.KQ'.format(symbol), start_date)
    else:
        print("DB가 이미 최신 상태입니다.")
        return

    # 데이터를 DB에 삽입
    insert_stock_data(symbol, stock_data)



# 유한양행 주식 업데이트 실행
symbol = '000100'
update_stock_data(symbol)

# DB 저장 및 연결 해제
conn.commit()
conn.close()


# SQLite 연결 종료
conn.close()
