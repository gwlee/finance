import sqlite3
import pandas as pd
from datetime import timedelta
from dateutil.relativedelta import relativedelta

# 데이터베이스 연결 및 데이터 조회
def fetch_data(symbol, db_path):
    conn = sqlite3.connect(db_path)
    query = f"""
        SELECT date, close 
        FROM stocks 
        WHERE symbol = '{symbol}' 
        ORDER BY date
    """
    data = pd.read_sql(query, conn)
    conn.close()
    
    if data.empty:
        raise ValueError(f"{symbol}의 데이터가 없습니다.")
    
    # 날짜 형식 변환
    data['date'] = pd.to_datetime(data['date'])
    return data

# 투자 기간별 수익률 계산
def calculate_moving_returns(data, period):
    results = []

    for i in range(len(data)):
        start_row = data.iloc[i]
        start_date = start_row['date']
        start_close = start_row['close']

        # 종료일 계산
        end_date = start_date + period
        end_data = data[data['date'] == end_date]

        # 종료일에 정확히 해당하는 데이터가 없으면 무시
        if end_data.empty:
            continue

        end_row = end_data.iloc[0]
        end_close = end_row['close']
        end_date = end_row['date']

        # 수익률 계산
        return_rate = (end_close - start_close) / start_close * 100

        # 결과 저장
        results.append({
            "시작날짜": start_date.strftime('%Y-%m-%d'),
            "종료날짜": end_date.strftime('%Y-%m-%d'),
            "시작종가": start_close,
            "종료종가": end_close,
            "수익률(%)": return_rate
        })
    return pd.DataFrame(results)

# 결과를 엑셀 파일로 저장 (다중 시트)
def save_to_excel(period_data, period_labels, filename):
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            for period_label, data in zip(period_labels, period_data):
                if not data.empty:
                    data.to_excel(writer, sheet_name=period_label, index=False)
        print(f"결과가 '{filename}' 파일로 저장되었습니다.")
    except Exception as e:
        print(f"엑셀 저장 중 오류가 발생했습니다: {e}")

# 실행
def main(symbol, db_path, output_file):
    try:
        # 데이터 가져오기
        data = fetch_data(symbol, db_path)

        # 투자 기간 설정 및 레이블 정의 (1개월, 3개월, 6개월, 1년, 5년, 10년)
        periods = [
            relativedelta(months=1),      # 1개월
            relativedelta(months=3),      # 3개월
            relativedelta(months=6),      # 6개월
            relativedelta(years=1),       # 1년
            relativedelta(years=5),       # 5년
            relativedelta(years=10)       # 10년
        ]
        period_labels = ["1개월", "3개월", "6개월", "1년", "5년", "10년"]

        # 투자 기간별 수익률 계산
        period_data = [calculate_moving_returns(data, period) for period in periods]

        # 엑셀로 저장
        save_to_excel(period_data, period_labels, output_file)
        
    except Exception as e:
        print("오류:", e)

# 데이터베이스 경로 및 심볼 설정
db_path = "finance_stock.db"  # 데이터베이스 파일 경로
symbol = "AAPL"  # 주식 심볼
output_file = "investment_returns_{}.xlsx".format(symbol)  # 저장할 엑셀 파일 이름

main(symbol, db_path, output_file)
