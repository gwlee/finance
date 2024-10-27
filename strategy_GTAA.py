import sqlite3
import pandas as pd
import numpy as np

# GTAA 자산 목록 정의
gtaa_assets = ['SPY', 'EFA', 'IEF', 'DBC', 'VNQ']  # GTAA 자산군
cash_asset = 'BIL'  # 현금 자산

# SQLite3에서 월말 종가 데이터를 불러오는 함수
def load_monthly_data(symbols):
    conn = sqlite3.connect('C:\\WORK\\jupyter\\finance_stock.db')
    query = f"""
    SELECT symbol, date, close FROM stocks
    WHERE symbol IN ({','.join('?' for _ in symbols)})
    """
    params = symbols
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    
    df['date'] = pd.to_datetime(df['date'])
    df.set_index(['date', 'symbol'], inplace=True)
    df = df['close'].unstack()  # 자산별 종가 데이터로 변환
    df = df.resample('ME').last()  # 월말 종가 추출

    # 현재 날짜 확인
    current_date = pd.Timestamp.today()
    last_valid_date = current_date - pd.DateOffset(months=1)
    last_valid_date = last_valid_date.replace(day=1) + pd.DateOffset(months=1) - pd.DateOffset(days=1)
    df = df[df.index <= last_valid_date]
    
    return df

# 각 자산의 10개월 이동평균 스코어 계산 함수
def calculate_sma(asset_data, current_index):
    if current_index < 10:  # 10개월 이동평균을 계산할 수 없는 경우
        return None

    sma_scores = {}
    for asset in asset_data.columns:
        # 10개월 이동평균 계산
        sma_10m = asset_data[asset].iloc[current_index - 9 : current_index + 1].mean()
        # 종가가 10개월 이동평균보다 높은 경우 매수, 그렇지 않으면 매도
        sma_scores[asset] = 1 if asset_data[asset].iloc[current_index] > sma_10m else 0
    
    return sma_scores

# GTAA 포트폴리오 결정 함수
def gtaa_portfolio(sma_scores):
    buy_assets = [asset for asset, score in sma_scores.items() if score == 1]
    
    if buy_assets:
        # 매수 자산 비중 균등 할당
        weights = [1 / len(buy_assets)] * len(buy_assets)
        portfolio = pd.Series(weights, index=buy_assets)
    else:
        # 모든 자산이 매도일 경우 현금 자산 100%
        portfolio = pd.Series([1], index=[cash_asset])

    return portfolio

# 재귀적으로 GTAA 전략을 수행하는 함수
def recursive_gtaa_strategy(asset_data, start_index):
    # 현재 시점에서 10개월 이동평균 스코어 계산
    sma_scores = calculate_sma(asset_data, start_index)

    if sma_scores is None:
        return  # 이동평균을 계산할 수 없는 경우 종료

    # 포트폴리오 결정
    portfolio = gtaa_portfolio(sma_scores)

    # 결과 출력
    current_date = asset_data.index[start_index]
    port_list = [f"{asset} ({weight * 100:.2f}%)" for asset, weight in portfolio.items()]

    # 데이터 저장
    as_list = [current_date.strftime('%Y-%m')] + list(sma_scores.values()) + port_list
    data_list.append(as_list)

    # 다음 달로 재귀 호출
    if start_index < len(asset_data) - 1:
        recursive_gtaa_strategy(asset_data, start_index + 1)

# 메인 함수: GTAA 전략 수행
def gtaa_strategy():
    all_assets = gtaa_assets + [cash_asset]
    asset_data = load_monthly_data(all_assets)

    # 시작 인덱스 설정
    start_index = 10  # 최소 10개월의 데이터가 필요하므로 10번째 인덱스부터 시작

    # 재귀적으로 전략 수행
    recursive_gtaa_strategy(asset_data, start_index)

# 실행 예시
data_list = []
gtaa_strategy()

# 결과를 엑셀 파일로 저장
output = pd.DataFrame.from_records(data_list)
#output.to_excel('GTAA.xlsx', sheet_name="GTAA", index=False, header=['date'] + gtaa_assets + ['Port1', 'Port2', 'Port3', 'Port4', 'Port5'])
output.to_excel('GTAA.xlsx', sheet_name="GTAA", index=False)
