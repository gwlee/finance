import pandas as pd
import numpy as np
import sqlite3

# 자산 목록 정의
canary_assets = ['SPY', 'VWO', 'VEA', 'BND']  # 카나리아 자산
offensive_assets = ['SPY', 'QQQ', 'IWM', 'VGK', 'EWJ', 'VWO', 'VNQ', 'DBC', 'GLD', 'TLT', 'HYG', 'LQD']  # 공격 자산
defensive_assets = ['TIP', 'DBC', 'BIL', 'IEF', 'TLT', 'LQD', 'BND']  # 방어 자산

# SQLite3에서 월말 종가 데이터를 불러오는 함수
def load_monthly_data(symbols):
    conn = sqlite3.connect('finance_stock.db')
    query = f"""
    SELECT symbol, date, close FROM stocks
    WHERE symbol IN ({','.join('?' for _ in symbols)})
    """
    params = symbols
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    
    df['date'] = pd.to_datetime(df['date'])
    df = df.groupby(['date', 'symbol']).mean().reset_index()
    df.set_index(['date', 'symbol'], inplace=True)
    df = df['close'].unstack()  # 각 자산의 종가를 컬럼으로 피벗
    df = df.resample('ME').last()  # 월말 종가 추출

    # 현재 날짜 확인
    current_date = pd.Timestamp.today()
    last_valid_date = current_date - pd.DateOffset(months=1)
    last_valid_date = last_valid_date.replace(day=1) + pd.DateOffset(months=1) - pd.DateOffset(days=1)
    df = df[df.index <= last_valid_date]
    
    return df

# 13612W 모멘텀 계산 함수
def calculate_13612_momentum(asset_data, current_index):
    if current_index < 12:
        return None
    momentum_1m = asset_data.iloc[current_index] / asset_data.iloc[current_index - 1] - 1
    momentum_3m = asset_data.iloc[current_index] / asset_data.iloc[current_index - 3] - 1
    momentum_6m = asset_data.iloc[current_index] / asset_data.iloc[current_index - 6] - 1
    momentum_12m = asset_data.iloc[current_index] / asset_data.iloc[current_index - 12] - 1

    momentum_score = (momentum_1m * 12) + (momentum_3m * 4) + (momentum_6m * 2) + (momentum_12m * 1)
    return momentum_score

# SMA12 모멘텀 계산 함수
def calculate_sma12(asset_data, current_index):
    if current_index < 12:
        return None
    sma_12 = asset_data.iloc[current_index - 12:current_index].mean()
    momentum_score = (asset_data.iloc[current_index] / sma_12) - 1
    return momentum_score

# 카나리아 자산 모멘텀 확인 함수
def check_canary_momentum(asset_data, current_index):
    canary_scores = {asset: calculate_13612_momentum(asset_data[asset], current_index) for asset in canary_assets}
    tmp_list.extend([canary_scores[asset] for asset in canary_assets])

    # 모든 카나리아 자산의 모멘텀 스코어가 존재하는지 확인
    if all(score is not None for score in canary_scores.values()):
        # 마이너스 모멘텀이 하나라도 있는 경우 방어 자산만 사용
        if any(score < 0 for score in canary_scores.values()):
            return False
        return True
    else:
        return None  # 모든 자산의 모멘텀 스코어가 존재하지 않으면 None 반환

# BAA 포트폴리오 구성 함수
def baa_portfolio(asset_data, current_index):
    # 카나리아 자산 모멘텀 확인
    use_offensive = check_canary_momentum(asset_data, current_index)
    
    if use_offensive is None:
        return None  # 포트폴리오를 구성하지 않음 (모든 모멘텀 스코어가 존재하지 않음)

    offensive_scores = {asset: calculate_sma12(asset_data[asset], current_index) for asset in offensive_assets}
    defensive_scores = {asset: calculate_sma12(asset_data[asset], current_index) for asset in defensive_assets}
    
    # 모든 공격 및 방어 자산의 모멘텀 스코어가 존재하는지 확인
    if not all(score is not None for score in offensive_scores.values()) or \
       not all(score is not None for score in defensive_scores.values()):
        return None  # 포트폴리오를 구성하지 않음 (모든 모멘텀 스코어가 존재하지 않음)

    tmp_list.extend([offensive_scores[asset] for asset in offensive_assets])
    tmp_list.extend([defensive_scores[asset] for asset in defensive_assets])

    if use_offensive:
        selected_offensive = sorted(offensive_scores, key=offensive_scores.get, reverse=True)[:6]
        portfolio = {asset: 1/6 for asset in selected_offensive}
    else:
        selected_defensive = sorted(defensive_scores, key=defensive_scores.get, reverse=True)[:3]
        
        bil_momentum = defensive_scores['BIL']
        portfolio = {}
        for asset in selected_defensive:
            if defensive_scores[asset] < bil_momentum:
                portfolio['BIL'] = portfolio.get('BIL', 0) + 1/3
            else:
                portfolio[asset] = 1/3

    # 포트폴리오 정보를 tmp_list에 추가
    portfolio_info = [f"{asset}:{weight:.2f}" for asset, weight in portfolio.items()]
    for item in portfolio_info:
        tmp_list.append(item)
    return portfolio

# BAA 전략 실행 함수
def run_baa_strategy():
    all_assets = canary_assets + offensive_assets + defensive_assets
    asset_data = load_monthly_data(all_assets)
    
    data_list = []
    
    for i in range(12, len(asset_data)):
        tmp_list.clear()
        portfolio = baa_portfolio(asset_data, i)
        
        if portfolio is not None:
            row = [asset_data.index[i].strftime('%Y-%m')] + tmp_list
            data_list.append(row)

    # 데이터 프레임 생성 및 엑셀 저장
    df = pd.DataFrame(data_list)
    df.to_excel("BAA.xlsx",sheet_name="BAA",  index=False, header=['date','SPY', 'VWO', 'VEA', 'BND', 'SPY', 'QQQ', 'IWM', 'VGK', 'EWJ', 'VWO', 'VNQ', 'DBC', 'GLD', 'TLT', 'HYG', 'LQD', 'TIP', 'DBC', 'BIL', 'IEF', 'TLT', 'LQD', 'BND', 'Port1','Port2','Port3','Port4','Port5','Port6'])
    print("엑셀 파일이 성공적으로 저장되었습니다.")

# 실행 예시
run_baa_strategy()
