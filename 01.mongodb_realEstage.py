import time
import requests
import xml.etree.ElementTree as ET
from pymongo import MongoClient, UpdateOne

# ------------------------
# MongoDB 설정
# ------------------------
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "realestate"
COLLECTION = "apt_trade"

client = MongoClient(MONGO_URI)
collection = client[DB_NAME][COLLECTION]


# ------------------------
# XML 아이템 파싱
# ------------------------
def parse_items(xml_text):
    try:
        root = ET.fromstring(xml_text)
    except Exception as e:
        print("❌ XML 파싱 오류:", e)
        print("원본:", xml_text[:300])
        return [], 0

    body = root.find("body")
    if body is None:
        print("❌ body 없음")
        return [], 0

    # totalCount 확인
    total_count_elem = root.find(".//totalCount")
    total_count = int(total_count_elem.text) if total_count_elem is not None else 0

    items_node = body.find("items")
    if items_node is None:
        return [], total_count

    items = []
    for item in items_node.findall("item"):
        row = {}
        for el in item:
            row[el.tag] = el.text.strip() if el.text else None
        items.append(row)

    return items, total_count


# ------------------------
# MongoDB Insert/Update(upsert)
# ------------------------
def save_to_mongodb(items):
    ops = []

    for doc in items:
        unique_filter = {
            "sggCd": doc.get("sggCd"),
            "aptNm": doc.get("aptNm"),
            "excluUseAr": doc.get("excluUseAr"),
            "floor": doc.get("floor"),
            "dealYear": doc.get("dealYear"),
            "dealMonth": doc.get("dealMonth"),
            "dealDay": doc.get("dealDay"),
        }

        update_data = {k: v for k, v in doc.items() if v is not None}

        ops.append(
            UpdateOne(unique_filter, {"$set": update_data}, upsert=True)
        )

    if ops:
        result = collection.bulk_write(ops)
        print(f"Inserted={result.upserted_count}, Updated={result.modified_count}")


# ------------------------
# 전체 실행
# ------------------------
def run(gu_code, deal_ymd, service_key, num_of_rows=200):
    base_url = (
        "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade?"
    )

    page_no = 1
    total_fetched = 0

    while True:
        # 🔥 파라미터 문자열 조합 방식 (요청하신 방식 그대로)
        payload = (
            f"LAWD_CD={gu_code}&"
            f"DEAL_YMD={deal_ymd}&"
            f"serviceKey={service_key}&"
            f"pageNo={page_no}&"
            f"numOfRows={num_of_rows}"
        )

        print(f"▶ API 호출: page {page_no}")

        response = requests.get(base_url + payload)
        xml_text = response.text.strip()

        # XML이 아닐 경우 오류 출력
        if not xml_text.startswith("<"):
            print("❌ XML이 아닌 응답 발생!", xml_text[:300])
            break

        items, total_count = parse_items(xml_text)

        if not items:
            print("▶ 데이터 없음 → 종료")
            break

        save_to_mongodb(items)

        total_fetched += len(items)

        # 마지막 페이지 도달 시 종료
        if page_no * num_of_rows >= total_count:
            break

        page_no += 1

    print(f"▶ 총 저장된 데이터: {total_fetched} 건")


# ------------------------
# 실행 예시
# ------------------------
"""
service_key = "<인증키>"
run("11215", "202001", service_key)
"""



