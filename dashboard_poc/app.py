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
    "stock_us": os.path.join(BASE_DIR, "data", "finance_stock.db"),
}

def query_time_series(symbol, db_key, start_date, end_date):
    """심볼(symbol)과 DB 키(db_key)를 받아 해당 시계열 close 값을 조회."""
    db_path = DB_PATHS[db_key]
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

@app.route("/api/series", methods=["GET"])
def get_series():
    # 파라미터: symbols (콤마구분), dbkeys (콤마구분 동일 순서), start_date, end_date
    symbols = request.args.get("symbols", "").split(",")
    dbkeys  = request.args.get("dbkeys", "").split(",")
    start   = request.args.get("start_date", "")
    end     = request.args.get("end_date", "")
    result = {}
    for sym, dbk in zip(symbols, dbkeys):
        df = query_time_series(sym, dbk, start, end)
        # JSON 직렬화: 날짜 리스트 + 종가 리스트
        result[sym] = {
            "dates": df["date"].dt.strftime("%Y-%m-%d").tolist(),
            "close": df["close"].tolist()
        }
    return jsonify(result)

if __name__ == "__main__":
    app.run(debug=True)
