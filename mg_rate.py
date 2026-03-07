import requests
from bs4 import BeautifulSoup
import time
import re
import os
import sys
import random
import csv


BASE = "https://www.kfcc.co.kr"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": BASE + "/map/main.do"
}

session = requests.Session()
session.headers.update(HEADERS)

# -------------------------
# main.do 소스 기반 전국 지역
# -------------------------

REGIONS = {
    "서울": ["도봉구","마포구","관악구","강북구","용산구","서초구","노원구","성동구","강남구","성북구",
           "광진구","송파구","은평구","강서구","강동구","종로구","양천구","중랑구","영등포구",
           "서대문구","구로구","동대문구","동작구","중구","금천구"],

    "인천": ["강화군","서구","동구","중구","미추홀구","연수구","계양구","부평구","남동구","옹진군"],

    "경기": ["김포시","파주시","연천군","고양시","양주시","동두천시","포천시","의정부시",
           "남양주시","구리시","가평군","하남시","부천시","광명시","시흥시","안산시",
           "안양시","과천시","군포시","의왕시","성남시","광주시","양평군","화성시",
           "수원시","오산시","용인시","이천시","여주시","평택시","안성시"],

    "강원": ["철원군","화천군","양구군","춘천시","인제군","고성군","속초시","양양군",
           "홍천군","강릉시","원주시","횡성군","평창군","영월군","정선군","동해시",
           "삼척시","태백시"],

    "충북": ["청주시","진천군","음성군","충주시","제천시","괴산군","단양군","보은군","옥천군","영동군","증평군"],

    "충남": ["태안군","서산시","당진시","홍성군","예산군","아산시","천안시","보령시","청양군",
           "공주시","서천군","부여군","논산시","금산군","계룡시"],

    "대전": ["유성구","대덕구","서구","중구","동구"],

    "경북": ["문경시","예천군","영주시","봉화군","울진군","상주시","의성군","안동시",
           "영양군","김천시","구미시","청송군","영덕군","성주군","칠곡군","영천시",
           "포항시","고령군","경산시","경주시","청도군","울릉군"],

    "경남": ["함양군","거창군","산청군","합천군","하동군","진주시","의령군","함안군",
           "창녕군","남해군","사천시","고성군","창원시","밀양시","통영시",
           "거제시","김해시","양산시"],

    "대구": ["서구","북구","동구","달서구","중구","남구","수성구","달성군","군위군"],

    "부산": ["강서구","북구","금정구","기장군","사상구","부산진구","연제구","동래구",
           "사하구","서구","중구","동구","남구","수영구","해운대구","영도구"],

    "울산": ["울주군","북구","중구","남구","동구"],

    "전북": ["군산시","익산시","부안군","김제시","완주군","전주시","고창군","정읍시",
           "순창군","임실군","진안군","무주군","남원시","장수군"],

    "전남": ["영광군","장성군","담양군","함평군","신안군","무안군","나주시","화순군",
           "곡성군","구례군","목포시","영암군","진도군","해남군","강진군","장흥군",
           "보성군","순천시","완도군","고흥군","여수시","광양시"],

    "광주": ["광산구","북구","서구","남구","동구"],

    "제주": ["제주시","서귀포시"],

    "세종": [""]
}

# -------------------------
# list.do → 금고 목록
# -------------------------

def get_branch_rows(r1, r2):
    r = requests.get(
        f"{BASE}/map/list.do",
        params={"r1": r1, "r2": r2},
        headers=HEADERS
    )

    soup = BeautifulSoup(r.text, "html.parser")
    rows = soup.select("table.rowTbl2 tbody tr")

    results = []

    for row in rows:

        def span(title):
            t = row.find("span", {"title": title})
            return t.text.strip() if t else ""

        results.append({
            "gmgoCd": span("gmgoCd"),
            "name": span("name"),
            "gmgoNm": span("gmgoNm"),            
            "divCd": span("divCd"),
            "divNm": span("divNm"),
            "gmgoType": span("gmgoType"),
            "telephone": span("telephone"),
            "fax": span("fax"),
            "addr": span("addr"),
            "code1": span("code1"),
            "code2": span("code2"),
            
            "r1": span("r1"),
            "r2": span("r2")
        })

    return results



# ----------------------------
# 금리 페이지 진입 → OPEN_TRMID 추출
# ----------------------------

def get_open_trmid(branch):

    params = {
        "gmgoCd": branch["gmgoCd"],
        "name": branch["name"],
        "gmgoNm": branch["gmgoNm"],
        "divCd": branch["divCd"],
        "divNm": branch["divNm"],
        "gmgoType": branch["gmgoType"],
        "telephone": branch["telephone"],
        "fax": branch["fax"],
        "addr": branch["addr"],
        "r1": branch["r1"],
        "r2": branch["r2"],
        "code1": branch["code1"],
        "code2": branch["code2"],
        "sel": "",
        "pageNo": "1",
        "tab": "sub_tab_rate"
    }

    r = session.get(BASE + "/map/view.do", params=params)

    soup = BeautifulSoup(r.text, "html.parser")

    inp = soup.find("input", {"name":"gumgoCode"})
    if inp:
        return inp["value"]

    return g["gmgoCd"]


# ----------------------------
# 상품 테이블 파싱
# ----------------------------

def parse_tables(html):

    soup = BeautifulSoup(html, "html.parser")
    items = []

    for wrap in soup.select(".tblWrap"):

        title = wrap.select_one(".tbl-tit")
        if not title:
            continue

        product = title.get_text(strip=True)
        table = wrap.find("table")
        if not table:
            continue

        for tr in table.select("tbody tr"):
            cols = [td.get_text(strip=True) for td in tr.find_all(["td","th"])]
            if len(cols) < 2:
                continue

            items.append((product, cols))

    return items


# ----------------------------
# 금리 수집 (요구불, 적립식 금리는 제외)
# ----------------------------

def get_rates(open_trmid):

    result = []

    for code, label in [
        ("13","거치식")
    ]:

        r = session.post(
            BASE + "/map/goods_19.do",
            data={
                "OPEN_TRMID": open_trmid,
                "gubuncode": code
            }
        )

        items = parse_tables(r.text)

        for product, cols in items:
            if str(product).strip() == cols[0].strip():
                line = " | ".join(cols[1:])

            else:
                line = " | ".join(cols)

            result.append((label, product, line))

        time.sleep(0.3)

    return result

# -------------------------
# 전체 실행
# -------------------------
ith open("mg_rates.csv", "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.writer(f)
    
    for r1, districts in REGIONS.items():
        for r2 in districts:
            branches = get_branch_rows(r1,r2)
            for b in branches:
                open_trmid = get_open_trmid(b)
                rates = get_rates(open_trmid)

                print(
                        f"[{b['r1']} {b['r2']}] "
                    )
                
                for label, product, line in rates:
                    writer.writerow([
                        b['r1'],
                        b['r2'],
                        b['name'],
                        b['divNm'],
                        label,
                        product,
                        line
                    ])                    

                    '''
                    print(
                        f"[{b['r1']} {b['r2']}] "
                        f"{b['name']} | {label} | {product} | {line}"
                    )
                    '''
                    
                time.sleep(random.random())
    
            time.sleep(random.random()*2)
    
        time.sleep(random.random()*5)
