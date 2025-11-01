# mongodb_setup.py (수정됨)

from pymongo import MongoClient
# ❌ 기존: from pymongo.errors import ConnectionError
# ✅ 수정: ServerSelectionTimeoutError를 사용하여 연결 오류를 처리합니다.
from pymongo.errors import ServerSelectionTimeoutError 

# MongoDB 연결 정보 (사용자 요청에 따라 localhost:27017)
MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "finance_db"

# 데이터 유형별 컬렉션 이름 정의
COLLECTION_NAMES = {
    "us_stocks": "us_stocks",
    "korean_stocks": "korean_stocks",
    "indices": "indices",
    "currencies": "currencies",
}

def setup_mongodb():
    """MongoDB에 연결하고, 필요한 컬렉션과 인덱스를 설정합니다."""
    try:
        # 1. MongoDB 클라이언트 연결
        # 2초 동안 연결을 시도하고 실패하면 ServerSelectionTimeoutError 발생
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000) 
        
        # 연결 확인 (실패 시 예외 발생)
        client.admin.command('ping')
        print(f"✅ MongoDB에 성공적으로 연결되었습니다: {MONGO_URI}")
        
        # 2. 데이터베이스 선택 또는 생성
        db = client[DATABASE_NAME]
        print(f"📊 데이터베이스 '{DATABASE_NAME}'를 선택/생성합니다.")

        # 3. 컬렉션 및 고유 인덱스 설정
        for type_name, collection_name in COLLECTION_NAMES.items():
            collection = db[collection_name]
            
            index_name = collection.create_index(
                [("symbol", 1), ("date", 1)],
                unique=True,
                name="symbol_date_unique_index"
            )
            print(f"   - 컬렉션 '{collection_name}'이 준비되었으며, 고유 인덱스 '{index_name}'이 설정되었습니다.")

        client.close()
        print("🎉 MongoDB 설정이 완료되었습니다.")

    # ✅ 수정된 부분: ServerSelectionTimeoutError를 처리합니다.
    except ServerSelectionTimeoutError as e: 
        print(f"❌ MongoDB 연결에 실패했습니다. 서버가 실행 중인지 확인하세요 (localhost:27017). 오류: {e}")
    except Exception as e:
        print(f"❌ 설정 중 오류가 발생했습니다: {e}")

if __name__ == "__main__":
    setup_mongodb()
