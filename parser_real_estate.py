import csv,glob
import sqlite3

# SQLite 데이터베이스 파일 경로
sqlite_file_path = "real_estate.db"

for csv_file_path in glob.glob("아파트(매매)_*.csv"):
    print (csv_file_path)
    num = 0
    with open(csv_file_path, newline="") as csvfile:
        reader = csv.reader(csvfile, delimiter=",")
        cursor = connection.cursor()
        for row in reader:
            if num < 16:
                pass

            else:
                tmp = list()
                for r in row[1:]:
                    tmp.append(r.strip())

                cursor.execute("""INSERT OR IGNORE INTO sale ("시군구","번지","본번","부번","단지명","전용면적","계약년월","계약일","거래금액","동","층","매수자","매도자","건축년도","도로명","해제사유발생일","거래유형","중개사소재지","등기일자") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (tmp),)

            num+=1

    connection.commit()

# 연결 닫기
connection.close()
