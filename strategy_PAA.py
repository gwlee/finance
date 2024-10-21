import sqlite3
import pandas as pd
import numpy as np

# PAA 자산 목록 정의
paa_assets = ['SPY', 'QQQ', 'IWM', 'VGK', 'EWJ', 'EEM', 'VNQ', 'DBC', 'GLD', 'HYG', 'LQD', 'TLT']  # 12개 자산
cash_asset = ['IEF']  # 현금성 자산

# SQLite3에서 월말 종가 데이터를 불러오는 함수
def load_monthly_data(symbols):
    conn = sqlite3.connect('stock.db')
    query = f"""
    SELECT symbol, date, close FROM stocks
    WHERE symbol IN ({','.join('?' for _ in symbols)})
    """
    params = symbols
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    
    df['date'] = pd.to_datetime(df['date'])
    
    # 중복된 날짜 제거: 같은 날짜가 여러 번 나올 경우 평균을 취함
    df = df.groupby(['date', 'symbol']).mean().reset_index()

    # 날짜와 심볼을 인덱스로 설정하고, 이를 피벗 테이블로 변환하여 각 자산의 종가가 컬럼으로 나열되도록 처리
    df.set_index(['date', 'symbol'], inplace=True)
    df = df['close'].unstack()  # 날짜를 기준으로 자산별 종가 데이터로 변환

    # 월말 종가 추출 (이전에는 'M'을 사용했으나, 'ME'(Month-End)를 사용)
    df = df.resample('ME').last()

    # 현재 날짜 기준으로 가장 최근에 완성된 달의 마지막 날짜를 계산
    current_date = pd.Timestamp.today()
    last_valid_date = current_date - pd.DateOffset(months=1)
    last_valid_date = last_valid_date.replace(day=1) + pd.DateOffset(months=1) - pd.DateOffset(days=1)
    df = df[df.index <= last_valid_date]
    
    return df

# 가장 오래된 12개월 모멘텀을 계산할 수 있는 날짜 탐색
def find_earliest_date_for_momentum(asset_data):
    first_valid_date = asset_data.dropna().index[0]  # 첫 번째 유효한 날짜
    earliest_date = first_valid_date + pd.DateOffset(months=12)
    return earliest_date

# PAA 모멘텀 스코어 계산 함수 (12개월 이동평균 사용)
def calculate_paa_momentum(asset_data, current_index):
    if current_index < 12:
        return None
    sma_12 = asset_data.iloc[current_index - 12:current_index].mean()  # 12개월 평균
    momentum_score = (asset_data.iloc[current_index] / sma_12) - 1  # 모멘텀 스코어 계산
    return momentum_score

# PAA 포트폴리오 결정 함수
def paa_portfolio(momentum_scores):
    # 모멘텀 스코어가 양수인 자산 수에 따라 현금 비중 조정
    negative_momentum_count = (momentum_scores < 0).sum()
    if negative_momentum_count >= 6:
        cash_weight = 100
        asset_weight = 0

    else:
        cash_weight = 16.7 * negative_momentum_count / 100
        asset_weight = (1 - cash_weight) / 6  # 나머지 자산 6개를 고르게 분배

    # 모멘텀 상위 6개 자산 선택
    top_6_assets = momentum_scores.nlargest(6).index

    return top_6_assets, [asset_weight] * 6, cash_weight

# 재귀적으로 PAA 전략을 수행하는 함수
def recursive_paa_strategy(asset_data, start_index):
    momentum_scores = pd.Series(index=paa_assets)
    
    for asset in paa_assets:
        momentum_score = calculate_paa_momentum(asset_data[asset], start_index)
        if momentum_score is not None:
            momentum_scores[asset] = momentum_score

    # 모멘텀 스코어를 계산할 수 없는 경우 종료
    if momentum_scores.isnull().any():
        return

    # 포트폴리오 결정
    selected_assets, weights, cash_weight = paa_portfolio(momentum_scores)

    # 결과 출력
    current_date = asset_data.index[start_index]
    port_list = list()
    for asset, weight in zip(selected_assets, weights):
        port_list.append(f"{asset} ({weight * 100:.2f}%)")
    
    as_list = [current_date.strftime('%Y-%m')]
    as_dict = momentum_scores.to_dict()
    
    # 모멘텀 스코어 기록
    for rec in as_dict:
        as_list.append(as_dict[rec])

    as_list.append(calculate_paa_momentum(asset_data[cash_asset[0]], start_index))

    # 포트폴리오 기록
    as_list.append(f"Cash ({cash_weight * 100:.2f}%)")
    for por in port_list:
        as_list.append(por)

    data_list.append(as_list)

    # 다음 달로 재귀 호출
    if start_index < len(asset_data) - 1:
        recursive_paa_strategy(asset_data, start_index + 1)

# 메인 함수: PAA 전략 수행
def paa_strategy():
    all_assets = paa_assets + cash_asset
    asset_data = load_monthly_data(all_assets)

    # 가장 오래된 모멘텀을 계산할 수 있는 날짜를 탐색
    earliest_momentum_date = find_earliest_date_for_momentum(asset_data)

    # 시작 인덱스 찾기: closest to the earliest momentum date
    start_index = pd.Index(asset_data.index).get_indexer([earliest_momentum_date], method='nearest')[0]

    # 재귀적으로 가장 오래된 시점부터 현재까지 계산
    recursive_paa_strategy(asset_data, start_index)

# 실행 예시
data_list = list()
paa_strategy()

output = pd.DataFrame.from_records(data_list)
output.to_excel('PAA.xlsx', sheet_name="PAA", index=False, header=['date','SPY', 'QQQ', 'IWM', 'VGK', 'EWJ', 'EEM', 'VNQ', 'DBC', 'GLD', 'HYG', 'LQD', 'TLT', 'IEF', 'Cash', 'Port1', 'Prot2', 'Port3', 'Port4', 'Port5', 'Port6'])
