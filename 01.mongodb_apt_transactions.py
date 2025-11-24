import csv
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

# MongoDB 연결 설정 (사용자 환경에 맞게 수정)
MONGO_URI = "mongodb://localhost:27017/"  # 예시 URI, 실제 URI로 변경
DB_NAME = "real_estate_db"
COLLECTION_NAME = "apartment_transactions"

# 컬럼 매핑 (CSV 헤더와 MongoDB 필드 이름 매칭, NO 제외)
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

# 해제사유발생일을 제외한 키 필드 목록
KEY_FIELDS = [FIELD_MAPPING[col] for col in FIELD_MAPPING if col != "해제사유발생일"]

def parse_csv_to_mongo(csv_file_path):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # 컬렉션에 인덱스 생성 (중복 체크를 위한 유니크 인덱스, 해제사유발생일 제외)
    index_keys = [(field, 1) for field in KEY_FIELDS]
    try:
        collection.create_index(index_keys, unique=True)
    except DuplicateKeyError:
        pass  # 이미 인덱스가 존재하면 무시

    with open(csv_file_path) as csv_file:
    #with open(csv_file_path, mode='r', encoding='utf-8') as csv_file:
        reader = csv.reader(csv_file)
        
        header = None
        for row in reader:
            if row and row[0] == "NO":  # 헤더 행 식별: 첫 컬럼이 "NO"인 행
                header = row
                break
        
        if header is None:
            raise ValueError("헤더 행을 찾을 수 없습니다.")
        
        # 이제 남은 행을 DictReader로 처리 (헤더 다음 행부터 데이터)
        csv_reader = csv.DictReader(csv_file, fieldnames=header)
        
        for row in csv_reader:
            # NO 컬럼 무시
            if 'NO' in row:
                del row['NO']
            
            # 딕셔너리 생성 및 필드 이름 매핑
            doc = {}
            for csv_col, mongo_field in FIELD_MAPPING.items():
                value = row.get(csv_col, "").strip()
                # 숫자 필드 변환 시도 (예: 전용면적, 거래금액 등)
                if mongo_field in ["exclusive_area", "transaction_amount", "floor", "construction_year"]:
                    try:
                        doc[mongo_field] = float(value.replace(',', '')) if '.' in value else int(value.replace(',', ''))
                    except ValueError:
                        doc[mongo_field] = value  # 변환 실패 시 문자열로 유지
                else:
                    doc[mongo_field] = value
            
            # 키 필드로 쿼리 생성
            query = {field: doc.get(field) for field in KEY_FIELDS}
            
            # 업데이트할 값: 해제사유발생일만 (만약 값이 있으면)
            update = {}
            if doc["cancellation_date"]:
                update["$set"] = {"cancellation_date": doc["cancellation_date"]}
            
            # 전체 문서 삽입을 위한 upsert (기존 문서가 없으면 삽입, 있으면 업데이트)
            if update:
                result = collection.update_one(query, update, upsert=False)
                if result.matched_count == 0:
                    # 기존 문서가 없으면 전체 삽입
                    collection.insert_one(doc)
            else:
                # 해제사유발생일이 없으면 그냥 삽입 (중복 시 인덱스 에러 발생 방지)
                try:
                    collection.insert_one(doc)
                except DuplicateKeyError:
                    # 이미 존재하면 무시 (또는 필요 시 로깅)
                    pass

    client.close()

# 사용 예시: CSV 파일 경로를 인자로 호출
# parse_csv_to_mongo("path/to/your/csv/file.csv")
parse_csv_to_mongo("아파트(매매)_실거래가_경기도_2005.csv")
