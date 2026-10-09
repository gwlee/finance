import FinanceDataReader as fdr
import pandas as pd

print("FinanceDataReader 버전:", fdr.__version__)
print("=" * 60)

# ============================================================
# 0. 한국 ETF Category 코드 → 한글 분류 매핑
# ============================================================
category_map = {
    1: "국내 시장지수",
    2: "국내 업종/테마",
    3: "국내 파생",
    4: "해외 주식",
    5: "원자재",
    6: "채권/금리",
    7: "단기자금",
}

# ============================================
# 1. 한국 주식 (코스피 / 코스닥)
# ============================================

print("\n[코스피 종목 리스트]")
kospi = fdr.StockListing('KOSPI')
print(f"종목 수: {len(kospi)}")
print(kospi.head())
kospi.to_excel("리스트_KOSPI.xlsx", index=False, engine='openpyxl')
print(f"    → 리트스_KOSPI.xlsx 저장 완료 ({len(kospi):,} 종목)")

print("\n[코스닥 종목 리스트]")
kosdaq = fdr.StockListing('KOSDAQ')
print(f"종목 수: {len(kosdaq)}")
print(kosdaq.head())
kosdaq.to_excel("리스트_KOSDAQ.xlsx", index=False, engine='openpyxl')
print(f"    → 리트스_KOSDAQ.xlsx 저장 완료 ({len(kosdaq):,} 종목)")


# ============================================
# 2. 한국 ETF
# ============================================
print("\n[한국 ETF 리스트]")
kr_etf = fdr.StockListing('ETF/KR')
print(f"종목 수: {len(kr_etf)}")
print(kr_etf.head())
# Category 코드 → 한글 분류명 컬럼 추가
kr_etf['CategoryName'] = kr_etf['Category'].map(category_map)
# 컬럼 순서 정리 (Category 바로 옆에 CategoryName 배치)
cols = list(kr_etf.columns)
if 'Category' in cols and 'CategoryName' in cols:
    cols.remove('CategoryName')
    cat_idx = cols.index('Category')
    cols.insert(cat_idx + 1, 'CategoryName')
    kr_etf = kr_etf[cols]

kr_etf.to_excel("리스트_KR_ETF.xlsx", index=False, engine='openpyxl')
print(f"    → 리트스_KR_ETF.xlsx 저장 완료 ({len(kr_etf):,} 종목)")


# ============================================
# 3. 미국 주식
# ============================================
print("\n[나스닥 종목 리스트]")
nasdaq = fdr.StockListing('NASDAQ')
print(f"종목 수: {len(nasdaq)}")
print(nasdaq.head())
nasdaq.to_excel("리스트_NASDAQ.xlsx", index=False, engine='openpyxl')
print(f"    → 리트스_NASDAQ.xlsx 저장 완료 ({len(kosdaq):,} 종목)")

print("\n[NYSE 종목 리스트]")
nyse = fdr.StockListing('NYSE')
print(f"종목 수: {len(nyse)}")
print(nyse.head())
nyse.to_excel("리스트_NYSE.xlsx", index=False, engine='openpyxl')
print(f"    → 리트스_NYSE.xlsx 저장 완료 ({len(kosdaq):,} 종목)")

print("\n[AMEX 종목 리스트]")
amex = fdr.StockListing('AMEX')
print(f"종목 수: {len(amex)}")
print(amex.head())
amex.to_excel("리스트_AMEX.xlsx", index=False, engine='openpyxl')
print(f"    → 리트스_AMEX.xlsx 저장 완료 ({len(kosdaq):,} 종목)")

# ============================================
# 4. 미국 ETF
# ============================================
print("\n[미국 ETF 리스트]")
try:
    us_etf = fdr.StockListing('ETF/US')
    print ('ETF/US')
except Exception:
    us_etf = fdr.EtfListing('US')   # 구버전 호환
    print ('US')

print(f"종목 수: {len(us_etf)}")
print(us_etf.head())
us_etf.to_excel("리스트_US_ETF.xlsx", index=False, engine='openpyxl')
print(f"    → 리트스_US_ETD.xlsx 저장 완료 ({len(kosdaq):,} 종목)")

print("\n완료!")
