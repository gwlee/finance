import sqlite3
import pandas as pd
import numpy as np

# 자산군 정의
attack_assets = ['QQQ', 'VWO', 'VEA', 'BND']  # 공격 자산
defensive_assets = ['TIP', 'DBC', 'BIL', 'IEF', 'TLT', 'LQD', 'BND']  # 방어 자산
canary_assets = ['SPY', 'VWO', 'VEA', 'BND']  # 카나리아 자산

# SQLite3에서 월말 종가 데이터를 불러오는 함수
def load_monthly_data(symbols):
    conn = sqlite3.connect('finance_stock.db')
    query = f"""
    SELECT symbol, date, close FROM stocks
    WHERE symbol IN ({','.join('?' for _ in symbols)})
    """
    df = pd.read_sql_query(query, conn, params=symbols)
    conn.close()

    df['date'] = pd.to_datetime(df['date'])
    df = df.groupby(['date', 'symbol']).mean().reset_index()
    df.set_index(['date', 'symbol'], inplace=True)
    df = df['close'].unstack()
    df = df.resample('ME').last()

    last_valid_date = pd.Timestamp.today() - pd.DateOffset(months=1)
    last_valid_date = last_valid_date.replace(day=1) + pd.DateOffset(months=1) - pd.DateOffset(days=1)
    df = df[df.index <= last_valid_date]
    
    return df

# 가장 오래된 12개월 모멘텀을 계산할 수 있는 날짜 탐색
def find_earliest_date_for_momentum(asset_data):
    # 첫 번째 유효한 날짜 찾기
    first_valid_date = asset_data.dropna().index[0]  # 첫 번째 유효한 날짜
    # 12개월 모멘텀을 계산할 수 있는 최소 날짜 계산
    earliest_date = first_valid_date + pd.DateOffset(months=12)
    
    return earliest_date

# 모멘텀 스코어 계산 함수
def calculate_momentum(asset_data, index):
    if index < 13:  # 12개월 모멘텀 계산 불가 시
        return None
    momentum_1m = asset_data.iloc[index] / asset_data.iloc[index - 1] - 1
    momentum_3m = asset_data.iloc[index] / asset_data.iloc[index - 3] - 1
    momentum_6m = asset_data.iloc[index] / asset_data.iloc[index - 6] - 1
    momentum_12m = asset_data.iloc[index] / asset_data.iloc[index - 12] - 1
    return (momentum_1m * 12) + (momentum_3m * 4) + (momentum_6m * 2) + (momentum_12m * 1)

# SMA(단순이동평균) 계산 함수
def calculate_sma(asset_data, index, window=12):
    return asset_data.rolling(window=window).mean().iloc[index]

# 포트폴리오 결정 함수
def abaa_portfolio(attack_scores, defensive_scores, canary_scores, sma_data, bil_sma):
    if all(score > 0 for score in canary_scores):  # 카나리아 자산이 모두 양수일 때
        selected_attack = attack_scores.idxmax() if attack_scores.max() > 0 else None
        portfolio = {selected_attack: 1} if selected_attack else None
    else:
        top_defensive = defensive_scores.nlargest(3)
        portfolio = {asset: 1/3 for asset in top_defensive.index}
        
        # 방어 자산 내에서 BIL보다 모멘텀이 낮은 자산 교체
        portfolio_copy = portfolio.copy()  # 포트폴리오 복사
        for asset in portfolio_copy:
            if sma_data[asset] < bil_sma:
                del portfolio[asset]
                portfolio['BIL'] = portfolio.get('BIL', 0) + 1/3  # BIL의 비중을 추가
                
    return portfolio

# 재귀적으로 ABAA 전략 실행 함수
def recursive_abaa_strategy(asset_data, index):
    global data_list
    # 카나리아, 공격, 방어 자산의 모멘텀 점수 계산
    canary_scores = calculate_momentum(asset_data[canary_assets], index)
    attack_scores = calculate_momentum(asset_data[attack_assets], index)
    defensive_scores = calculate_momentum(asset_data[defensive_assets], index)

    # NaN 값이 포함된 경우, 즉 모멘텀 스코어를 계산할 수 없는 경우 종료
    if any(score.isnull().any() for score in [canary_scores, attack_scores, defensive_scores]):
        return
    
    # 각 자산의 SMA 계산
    sma_data = {asset: calculate_sma(asset_data[asset], index) for asset in attack_assets + defensive_assets}
    bil_sma = calculate_sma(asset_data['BIL'], index)
    
    # 포트폴리오 구성
    portfolio = abaa_portfolio(attack_scores, defensive_scores, canary_scores, sma_data, bil_sma)

    # 결과 저장
    current_date = asset_data.index[index]
    as_list = [current_date.strftime('%Y-%m')]
    as_list += canary_scores.tolist() + attack_scores.tolist() + defensive_scores.tolist()
    as_list += [f"{asset} ({weight * 100:.2f}%)" for asset, weight in portfolio.items()]

    data_list.append(as_list)
    # 다음 달로 재귀 호출
    if index < len(asset_data) - 1:
        recursive_abaa_strategy(asset_data, index + 1)

# 메인 함수
def abaa_strategy():
    all_assets = attack_assets + defensive_assets + canary_assets
    asset_data = load_monthly_data(all_assets)

    # 가장 오래된 모멘텀 계산 가능 날짜 찾기
    earliest_momentum_date = find_earliest_date_for_momentum(asset_data)
    start_index = pd.Index(asset_data.index).get_indexer([earliest_momentum_date], method='nearest')[0]
    
    # 전략 실행
    recursive_abaa_strategy(asset_data, start_index)

# 실행
data_list = []
abaa_strategy()

# 결과 출력
output = pd.DataFrame.from_records(data_list)
output.to_excel('ABAA.xlsx', sheet_name="ABAA", index=False, header=['Date', 'SPY', 'VWO', 'VEA', 'BND', 'QQQ', 'VWO', 'VEA', 'BND', 'TIP', 'DBC', 'BIL', 'IEF', 'TLT', 'LQD', 'BND', 'Portfolio1', 'Portfolio2', 'Portfolio3'])
