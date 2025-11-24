# rent_apartment_to_mongo.py
import csv
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

# ==================== 설정 영역 ====================
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "real_estate_db"
COLLECTION_NAME = "apartment_rent_transactions"   # 전월세 전용 컬렉션

# 실제 CSV에 등장할 수 있는 컬럼 → MongoDB 필드 매핑
FIELD_MAPPING = {
    "시군구": "sigungu",
    "번지": "bunji",
    "본번": "bonbun",
    "부번": "bubun",
    "단지명": "complex_name",
    "전월세구분": "rent_type",            # 전세 / 월세
    "전용면적(㎡)": "exclusive_area",
    "계약년월": "contract_year_month",
    "계약일": "contract_day",
    "보증금(만원)": "deposit",
    "월세금(만원)": "monthly_rent",        # 오래된 파일
    "월세(만원)": "monthly_rent",          # 최신 파일
    "층": "floor",
    "건축년도": "construction_year",
    "도로명": "road_name",
    "계약기간": "contract_period",
    "계약구분": "contract_category",       # 신규/갱신 등
    "갱신요구권 사용": "renewal_right_used",
    "종전계약 보증금(만원)": "previous_deposit",
    "종전계약 월세(만원)": "previous_monthly_rent",
    "주택유형": "housing_type",
}

# 숫자로 변환할 필드
NUMERIC_FIELDS = {
    "exclusive_area", "deposit", "monthly_rent",
    "floor", "construction_year",
    "previous_deposit", "previous_monthly_rent"
}

# 전월세는 해제사유발생일이 없으므로 모든 필드를 고유키로 사용
UNIQUE_FIELDS = list(FIELD_MAPPING.values())
# ===================================================

def parse_rent_csv_to_mongo(csv_file_path: str):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    col = db[COLLECTION_NAME]

    # 모든 필드로 복합 유니크 인덱스 생성 (이미 있으면 무시)
    index_keys = [(field, 1) for field in UNIQUE_FIELDS]
    col.create_index(index_keys, unique=True, background=True)

    with open(csv_file_path, mode="r", encoding="utf-8") as f:
        lines = f.readlines()

    # 1. 헤더 행 찾기 ("NO" 로 시작하는 행)
    header = None
    header_idx = -1
    for idx, line in enumerate(lines):
        if line.strip().startswith("NO"):
            # 탭으로 분리된 실제 헤더 리스트
            header = [h.strip() for h in line.strip().split("\t")]
            header_idx = idx
            break
    if header is None:
        raise ValueError(f"[{csv_file_path}] 파일에서 헤더 행(NO로 시작)을 찾을 수 없습니다.")

    # 2. 실제 데이터 행들만 추출 (header 다음 행부터 끝까지)
    data_lines = lines[header_idx + 1:]

    # 3. DictReader로 파싱 (탭 구분)
    reader = csv.DictReader(
        data_lines,
        fieldnames=header,
        delimiter="\t",
        quoting=csv.QUOTE_NONE
    )

    inserted = 0
    skipped = 0

    for row in reader:
        # NO 컬럼 제거
        row.pop("NO", None)

        # 문서 생성
        doc = {}
        for csv_col, mongo_field in FIELD_MAPPING.items():
            value = row.get(csv_col, "").strip()

            if mongo_field in NUMERIC_FIELDS and value:
                value = value.replace(",", "")
                try:
                    doc[mongo_field] = float(value) if "." in value else int(value)
                except ValueError:
                    doc[mongo_field] = None
            else:
                doc[mongo_field] = value if value else None

        # 전월세는 해제 컬럼이 없으므로 바로 insert (중복은 인덱스에서 차단)
        try:
            col.insert_one(doc)
            inserted += 1
        except DuplicateKeyError:
            # 완전히 동일한 거래건은 스킵 (이미 존재)
            skipped += 1

    client.close()
    print(f"[{csv_file_path}] 처리 완료 → 삽입: {inserted}건, 중복 스킵: {skipped}건")


# ==================== 실행 예시 ====================
if __name__ == "__main__":
    # 단일 파일 처리
    parse_rent_csv_to_mongo("data/서울특별시_아파트_전월세_2011.csv")

    # 여러 파일 일괄 처리 예시
    # import glob, os
    # for file in glob.glob("data/*전월세*.csv"):
    #     if os.path.getsize(file) > 0:
    #         parse_rent_csv_to_mongo(file)
