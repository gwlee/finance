from flask import Flask, render_template, request, jsonify
import sqlite3
import pandas as pd
import os

app = Flask(__name__)

# DB 파일 경로 설정
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DB_PATHS = {
    "currency": os.path.join(BASE_DIR, "data", "finance_currency.db"),
    "index":    os.path.join(BASE_DIR, "data", "finance_index.db"),
    "stock_kr": os.path.join(BASE_DIR, "data", "finance_stock_kr.db"),
    "stock_us": os.path.join(BASE_DIR, "data", "finance_stock.db"), # finance_stock.db로 변경
}

# 로딩된 심볼 목록을 저장할 변수
LOADED_SYMBOLS = {}

def get_unique_symbols_from_db(db_key, db_path):
    """지정된 DB 파일에서 고유한 symbol 목록을 조회."""
    try:
        conn = sqlite3.connect(db_path)
        # 예: currency 테이블 or stocks 테이블
        table = "currency" if db_key == "currency" else "stocks"
        sql = f"SELECT DISTINCT symbol FROM {table} ORDER BY symbol"
        df = pd.read_sql(sql, conn)
        conn.close()
        return df["symbol"].tolist()
    except Exception as e:
        print(f"Error loading symbols from {db_key}: {e}")
        return []

def load_all_symbols():
    """모든 DB에서 심볼 목록을 로드."""
    print("Loading symbols from all databases...")
    for db_key, db_path in DB_PATHS.items():
        LOADED_SYMBOLS[db_key] = get_unique_symbols_from_db(db_key, db_path)
    print("Symbol loading complete.")

# Flask 애플리케이션 시작 전에 심볼 로드
load_all_symbols()

def query_time_series(symbol, db_key, start_date, end_date):
    """심볼(symbol)과 DB 키(db_key)를 받아 해당 시계열 close 값을 조회."""
    db_path = DB_PATHS.get(db_key)
    if not db_path:
        return pd.DataFrame({'date': [], 'close': []})

    conn = sqlite3.connect(db_path)
    # 예: currency 테이블 or stocks 테이블
    table = "currency" if db_key == "currency" else "stocks"
    sql = f"""
        SELECT date, close
        FROM {table}
        WHERE symbol = ?
          AND date BETWEEN ? AND ?
        ORDER BY date
    """
    df = pd.read_sql(sql, conn, params=(symbol, start_date, end_date), parse_dates=["date"])
    conn.close()
    return df

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/symbols", methods=["GET"])
def get_symbols():
    """로드된 심볼 목록을 JSON 형태로 반환."""
    return jsonify(LOADED_SYMBOLS)

@app.route("/api/series", methods=["GET"])
def get_series():
    # 파라미터: symbols (콤마구분), dbkeys (콤마구분 동일 순서), start_date, end_date
    symbols = request.args.get("symbols", "").split(",")
    dbkeys  = request.args.get("dbkeys", "").split(",")
    start   = request.args.get("start_date", "")
    end     = request.args.get("end_date", "")
    result = {}
    
    # 길이가 다른 경우 처리
    if len(symbols) != len(dbkeys):
        return jsonify({"error": "Symbols and DB keys count mismatch"}), 400

    for sym, dbk in zip(symbols, dbkeys):
        if sym and dbk: # 빈 문자열 방지
            df = query_time_series(sym, dbk, start, end)
            # JSON 직렬화: 날짜 리스트 + 종가 리스트
            result[sym] = {
                "dates": df["date"].dt.strftime("%Y-%m-%d").tolist(),
                "close": df["close"].tolist()
            }
    return jsonify(result)

if __name__ == "__main__":
    # HOST를 0.0.0.0으로 설정하여 외부 접속 가능하도록 함 (필요시)
    app.run(debug=True, host='127.0.0.1')