import csv,glob,time
import sqlite3

# SQLite 데이터베이스 파일 경로
sqlite_file_path = "real_estate.db"

for csv_file_path in glob.glob("아파트(전월세)_실거래가_*.csv"):
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

                cursor.execute("""INSERT OR IGNORE INTO rent ("시군구","번지","본번","부번","단지명","구분","전용면적","계약년월","계약일","보증금","월세금","층","건축년도","도로명","계약기간","계약구분","갱신요구권","종전계약보증금","종전계약월세","주택유형") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (tmp),)

            num+=1

      connection.commit()
      
# 연결 닫기
connection.close()

