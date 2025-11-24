import csv
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

# MongoDB 연결 설정
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "real_estate_db"
COLLECTION_NAME = "apartment_transactions"

# CSV 헤더 → MongoDB 필드 매핑
# NO는 제외 (식별자 역할 없음)
FIELD_MAPPING = {
    "시군구": "sigungu",
    "번지": "bunji",
    "본번": "bonbun",
    "부번": "bubun",
    "단지명": "complex_name",
    "전용면적(㎡)": "exclusive_area",
    "계약년월": "contract_year_month",
    "계약일": "contract_day",
    "거래금액(만원)": "transaction_amount",
    "동": "dong",
    "층": "floor",
    "매수자": "buyer",
    "매도자": "seller",
    "건축년도": "construction_year",
    "도로명": "road_name",
    "해제사유발생일": "cancellation_date",
    "거래유형": "transaction_type",
    "중개사소재지": "broker_location",
    "등기일자": "registration_date"
}

# 중복 판단에 사용될 주요 필드 목록 (해제사유발생일 제외)
KEY_FIELDS = [FIELD_MAPPING[col] for col in FIELD_MAPPING if col != "해제사유발생일"]


def parse_csv_to_mongo(csv_file_path):
    """CSV 파일을 읽어 MongoDB 컬렉션에 삽입 또는 업데이트하는 함수"""

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # 유니크 인덱스 생성 - 동일 거래 데이터 중복 방지
    index_keys = [(field, 1) for field in KEY_FIELDS]
    try:
        collection.create_index(index_keys, unique=True)
    except DuplicateKeyError:
        pass  # 이미 생성된 경우 무시

    # 결과 카운트 변수
    inserted = 0
    skipped = 0
    updated = 0

    # 파일 읽기 (UTF-8로 열기)
    #with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
    with open(csv_file_path) as csv_file:
        reader = csv.reader(csv_file)

        header = None
        # CSV 내에서 "NO"가 포함된 행을 찾아 헤더로 인식
        for row in reader:
            if row and row[0] == "NO":
                header = row
                break

        if header is None:
            raise ValueError("헤더 행을 찾을 수 없습니다. 'NO'로 시작하는 헤더가 필요합니다.")

        # DictReader는 현재 위치(헤더 이후)부터 데이터 읽음
        csv_reader = csv.DictReader(csv_file, fieldnames=header)

        # CSV 한 행씩 처리
        for row in csv_reader:
            # "NO" 필드는 사용하지 않음 → 제거
            if 'NO' in row:
                del row['NO']

            # MongoDB에 저장할 문서 생성
            doc = {}
            for csv_col, mongo_field in FIELD_MAPPING.items():
                value = row.get(csv_col, "").strip()

                # 숫자형 데이터 처리 (정수/실수 변환)
                if mongo_field in ["exclusive_area", "transaction_amount", "floor", "construction_year"]:
                    try:
                        value = value.replace(',', '')
                        value = float(value) if '.' in value else int(value)
                    except ValueError:
                        pass  # 숫자 변환 실패 시 문자열 그대로 유지

                doc[mongo_field] = value

            # 중복 체크를 위한 검색 조건
            query = {field: doc.get(field) for field in KEY_FIELDS}

            # 해제사유발생일만 업데이트 대상
            update = {}
            if doc.get("cancellation_date"):
                update["$set"] = {"cancellation_date": doc["cancellation_date"]}

            # 이미 존재하는 문서는 업데이트 or 신규 문서 삽입
            if update:
                result = collection.update_one(query, update, upsert=False)
                if result.matched_count > 0:
                    # 기존 문서가 존재했고 업데이트됨
                    updated += 1
                else:
                    # 기존 문서가 없으므로 새로 삽입
                    try:
                        collection.insert_one(doc)
                        inserted += 1
                    except DuplicateKeyError:
                        skipped += 1
            else:
                # 해제사유 정보 없으면 insert 시도
                try:
                    collection.insert_one(doc)
                    inserted += 1
                except DuplicateKeyError:
                    # 동일 거래 이미 존재하므로 스킵
                    skipped += 1

    # DB 연결 종료
    client.close()

    # 결과 출력
    print(f"[{csv_file_path}] 처리 완료 → 삽입: {inserted}건, 업데이트: {updated}건, 스킵: {skipped}건")


# 사용 예시: CSV 파일 경로를 인자로 호출
# parse_csv_to_mongo("path/to/your/csv/file.csv")
#parse_csv_to_mongo("아파트(매매)_실거래가_경기도_2005.csv")

# 2005~2006년 경기·서울 CSV 처리 실행
for r in range(2005, 2007):
    parse_csv_to_mongo(f"아파트(매매)_실거래가_경기도_{r}.csv")
    parse_csv_to_mongo(f"아파트(매매)_실거래가_서울특별시_{r}.csv")
