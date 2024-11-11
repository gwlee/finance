import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 데이터베이스 파일 및 테이블 설정
db_file = 'finance_stock.db'  # 데이터베이스 파일 이름을 설정합니다.
symbol = 'AAPL'  # 분석할 종목의 심볼

# 데이터베이스 연결
conn = sqlite3.connect(db_file)

# SQL 쿼리 실행 - 특정 심볼(AAPL)의 월별 최고가와 최저가를 구하여 변동 비율을 계산
query = f"""
SELECT 
    strftime('%Y', date) AS year,
    strftime('%m', date) AS month,
    MAX(close) AS max_close,
    MIN(close) AS min_close
FROM 
    stocks
WHERE 
    symbol = ?
GROUP BY 
    strftime('%Y-%m', date)
ORDER BY 
    year, month;
"""

# 쿼리 결과를 Pandas DataFrame으로 가져오기
monthly_data = pd.read_sql_query(query, conn, params=(symbol,))

# 연결 닫기
conn.close()

# 'month'와 'year'를 숫자 형식으로 변환
monthly_data['year'] = monthly_data['year'].astype(int)
monthly_data['month'] = monthly_data['month'].astype(int)

# 변동 비율 계산 (최소값/최대값 비율)
monthly_data['min_max_ratio'] = monthly_data['min_close'] / monthly_data['max_close']

# 각 월별로 데이터 프레임 분리
monthly_ratios = monthly_data[['month', 'min_max_ratio']]

# Box Plot 그리기
plt.figure(figsize=(12, 6))
sns.boxplot(x='month', y='min_max_ratio', data=monthly_ratios, order=range(1, 13))
plt.title(f"Monthly Min/Max Ratio Box Plot for {symbol}")
plt.xlabel("Month")
plt.ylabel("Min/Max Ratio (Min Close / Max Close)")
plt.xticks(ticks=range(12), labels=[
    'January', 'February', 'March', 'April', 'May', 'June', 
    'July', 'August', 'September', 'October', 'November', 'December'
])
plt.show()
