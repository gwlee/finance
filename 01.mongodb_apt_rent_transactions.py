# apt_rent_to_mongo_final.py
import csv
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

# ==================== 설정 ====================
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "real_estate_db"
COLLECTION_NAME = "apartment_rent_transactions"

# 컬럼 매핑 (모든 연도 호환)
FIELD_MAPPING = {
    "시군구": "sigungu",
    "번지": "bunji",
    "본번": "bonbun",
    "부번": "bubun",
    "단지명": "complex_name",
    "전월세구분": "rent_type",                # 전세 / 월세
    "전용면적(㎡)": "exclusive_area",
    "계약년월": "contract_year_month",
    "계약일": "contract_day",
    "보증금(만원)": "deposit",
    "월세금(만원)": "monthly_rent",            # 구버전
    "월세(만원)": "monthly_rent",              # 신버전
    "층": "floor",
    "건축년도": "construction_year",
    "도로명": "road_name",
    "계약기간": "contract_period",
    "계약구분": "contract_category",
    "갱신요구권 사용": "renewal_right_used",
    "종전계약 보증금(만원)": "previous_deposit",
    "종전계약 월세(만원)": "previous_monthly_rent",
    "주택유형": "housing_type",
}

# 숫자로 변환할 필드들
NUMERIC_FIELDS = [
    "exclusive_area", "deposit", "monthly_rent",
    "floor", "construction_year",
    "previous_deposit", "previous_monthly_rent"
]

# 전월세는 해제사유발생일이 없으므로 모든 필드로 유니크 인덱스
UNIQUE_FIELDS = list(FIELD_MAPPING.values())
# ===============================================

def parse_rent_csv_to_mongo(csv_file_path: str):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # 복합 유니크 인덱스 생성 (한 번만 실행됨)
    index_keys = [(field, 1) for field in UNIQUE_FIELDS]
    try:
        collection.create_index(index_keys, unique=True, name="unique_rent_transaction")
        print("유니크 인덱스 생성 완료")
    except Exception:
        pass  # 이미 있으면 무시

    # 파일 열기 (newline='' 로 Windows 줄바꿈 문제 해결)
    #with open(csv_file_path, mode="r", encoding="utf-8", newline='') as f:
    with open(csv_file_path) as f:
        #reader = csv.reader(f, delimiter="\t")   # 국토교통부 CSV는 탭 구분
        reader = csv.reader(f)   # 국토교통부 CSV는 탭 구분

        header = None
        for row in reader:
            if row and row[0].strip() == "NO":    # 정확히 "NO" 로 시작하는 행 찾기
                header = [col.strip() for col in row]
                break

        if header is None:
            client.close()
            raise ValueError(f"[{csv_file_path}] 파일에서 'NO'로 시작하는 헤더 행을 찾을 수 없습니다.")

        # 이제 파일 포인터는 헤더 다음 행에 있음 → DictReader로 나머지 읽기
        csv_reader = csv.DictReader(f, fieldnames=header)

        inserted = 0
        skipped = 0

        for row in csv_reader:
            # NO 컬럼 무시
            if 'NO' in row:
                del row['NO']

            # 문서 생성
            doc = {}
            for csv_col, mongo_field in FIELD_MAPPING.items():
                value = row.get(csv_col, "").strip()

                if mongo_field in NUMERIC_FIELDS and value:
                    value = value.replace(",", "")
                    try:
                        doc[mongo_field] = float(value) if "." in value else int(value)
                    except ValueError:
                        doc[mongo_field] = value
                else:
                    doc[mongo_field] = value if value else None

            # 삽입 시도 (중복은 유니크 인덱스에서 차단)
            try:
                collection.insert_one(doc)
                inserted += 1
            except DuplicateKeyError:
                skipped += 1
            except Exception as e:
                print(f"삽입 실패 (예외): {e}")
                print(f"문서: {doc}")

    client.close()
    print(f"[{csv_file_path}] 처리 완료 → 삽입: {inserted}건, 중복 스킵: {skipped}건")


# ==================== 실행 ====================
if __name__ == "__main__":
    # 단일 파일 처리
    parse_rent_csv_to_mongo("data/서울특별시_아파트_전월세_2011.csv")
    
    # 여러 파일 일괄 처리 예시
    # import glob
    # for path in glob.glob("*.csv"):
    #     if "전월세" in path:
    #         parse_rent_csv_to_mongo(path)
