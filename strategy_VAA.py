import sqlite3
import pandas as pd
import numpy as np

# VAA 자산 목록 정의
attack_assets = ['SPY', 'VEA', 'VWO', 'BND']  # 공격 자산
defensive_assets = ['LQD', 'IEF', 'SHY']  # 방어 자산

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
    
    # 중복된 날짜 제거: 같은 날짜가 여러 번 나올 경우 평균을 취함
    df = df.groupby(['date', 'symbol']).mean().reset_index()

    # 날짜와 심볼을 인덱스로 설정하고, 이를 피벗 테이블로 변환하여 각 자산의 종가가 컬럼으로 나열되도록 처리
    df.set_index(['date', 'symbol'], inplace=True)
    df = df['close'].unstack()  # 날짜를 기준으로 자산별 종가 데이터로 변환

    # 월말 종가 추출 (이전에는 'M'을 사용했으나, 'ME'(Month-End)를 사용)
    df = df.resample('ME').last()
    
    return df

# 가장 오래된 12개월 모멘텀을 계산할 수 있는 날짜 탐색
def find_earliest_date_for_momentum(asset_data):
    # 데이터가 시작된 가장 오래된 월을 탐색
    first_valid_date = asset_data.dropna().index[0]  # 첫 번째 유효한 날짜
    # 12개월 모멘텀을 계산하려면 첫 유효 날짜로부터 최소 12개월 후부터 가능
    earliest_date = first_valid_date + pd.DateOffset(months=12)
    
    return earliest_date

# 모멘텀 스코어 계산 함수 (1개월, 3개월, 6개월, 12개월 모멘텀)
def calculate_momentum(asset_data, current_index):
    # current_index 기준으로 1개월, 3개월, 6개월, 12개월 전의 종가를 사용
    if current_index < 13:  # 12개월 모멘텀을 계산할 수 없는 경우
        return None

    momentum_1m = asset_data.iloc[current_index] / asset_data.iloc[current_index - 1] - 1  # 1개월 모멘텀
    momentum_3m = asset_data.iloc[current_index] / asset_data.iloc[current_index - 3] - 1  # 3개월 모멘텀
    momentum_6m = asset_data.iloc[current_index] / asset_data.iloc[current_index - 6] - 1  # 6개월 모멘텀
    momentum_12m = asset_data.iloc[current_index] / asset_data.iloc[current_index - 12] - 1  # 12개월 모멘텀

    # 가중합으로 모멘텀 스코어 계산
    momentum_score = (momentum_1m * 12) + (momentum_3m * 4) + (momentum_6m * 2) + (momentum_12m * 1)
    return momentum_score

# VAA 포트폴리오 결정 함수
def vaa_portfolio(attack_scores, defensive_scores):
    attack_spy, attack_vea, attack_vwo, attack_bnd = attack_scores['SPY'], attack_scores['VEA'], attack_scores['VWO'], attack_scores['BND']
    
    # 공격 자산 모멘텀이 모두 양수일 경우만 공격 자산
    if attack_spy > 0 and attack_vea > 0 and attack_vwo > 0 and attack_bnd > 0:
        portfolio = attack_scores.nlargest(1)  # 공격 자산 1종목 선정
        weights = [1]  # 공격 자산 100% 비중
    else:
        portfolio = defensive_scores.nlargest(1)  # 방어 자산 1종목 선정
        weights = [1]  # 방어 자산 100% 비중

    return portfolio.index, weights

# 재귀적으로 DAA 전략을 수행하는 함수
def recursive_vaa_strategy(asset_data, start_index):
    # 현재 시점에서 모멘텀 스코어 계산
    attack_scores = calculate_momentum(asset_data[attack_assets], start_index)
    defensive_scores = calculate_momentum(asset_data[defensive_assets], start_index)

    if attack_scores is None or defensive_scores is None:
        return  # 모멘텀 스코어를 계산할 수 없는 경우 종료

    # 포트폴리오 결정
    selected_assets, weights = vaa_portfolio(attack_scores, defensive_scores)

    # 결과 출력
    current_date = asset_data.index[start_index]
    port_list = list()
    for asset, weight in zip(selected_assets, weights):
        port_list.append(f"{asset} ({weight * 100:.2f}%)")
    

    as_list = list()
    as_list.append(current_date.strftime('%Y-%m'))
    as_dict = attack_scores.to_dict()
    for rec in as_dict:
        as_list.append(as_dict[rec])

    as_dict = defensive_scores.to_dict()
    for rec in as_dict:
        as_list.append(as_dict[rec])

    for por in port_list:
        as_list.append(por)

    data_list.append(as_list)

    # 다음 달로 재귀 호출
    if start_index < len(asset_data) - 1:
        recursive_vaa_strategy(asset_data, start_index + 1)

# 메인 함수: vaa 전략 수행
def vaa_strategy():
    all_assets = attack_assets + defensive_assets
    asset_data = load_monthly_data(all_assets)

    # 가장 오래된 모멘텀을 계산할 수 있는 날짜를 탐색
    earliest_momentum_date = find_earliest_date_for_momentum(asset_data)

    # 시작 인덱스 찾기: closest to the earliest momentum date
    start_index = pd.Index(asset_data.index).get_indexer([earliest_momentum_date], method='nearest')[0]

    # 재귀적으로 가장 오래된 시점부터 현재까지 계산
    recursive_vaa_strategy(asset_data, start_index)

# 실행 예시
data_list = list()
vaa_strategy()

output = pd.DataFrame.from_records(data_list)
output.to_excel('VAA.xlsx', sheet_name="DAA", index=False, header=['date','SPY', 'VEA', 'VWO', 'BND', 'LQD', 'IEF', 'SHY', 'Port1'])
