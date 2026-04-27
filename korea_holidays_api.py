"""
Korea Public Holidays API  v2.0
=================================
Source  : data.go.kr > 한국천문연구원_특일 정보
Endpoint: https://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService/getRestDeInfo
Format  : XML (자동 파싱)

설치 (이미 했으면 생략):
    pip install fastapi uvicorn requests python-dotenv

.env 파일:
    DATA_GO_KR_KEY=발급받은_인증키_전체

실행 순서:
    python korea_holidays_api.py --test    # Step 1: 2026년 1월 테스트
    python korea_holidays_api.py --sync    # Step 2: 2024~2027 전체 저장
    python korea_holidays_api.py           # Step 3: API 서버 시작
"""

import os, sys, sqlite3, requests, argparse
import xml.etree.ElementTree as ET
from datetime import datetime
from fastapi import FastAPI, HTTPException, Query
import uvicorn
from dotenv import load_dotenv

load_dotenv()

SERVICE_KEY = os.getenv("DATA_GO_KR_KEY", "")
ENDPOINT    = "https://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService/getRestDeInfo"
DB_FILE     = "holidays_data.db"

# ── 한글 공휴일명 → 영문 변환 ─────────────────────────────────────────
HOLIDAY_NAMES = {
    "신정":          "New Year's Day",
    "설날":          "Lunar New Year's Day",
    "설날 전날":      "Lunar New Year Eve",
    "설날 다음날":    "Lunar New Year Holiday",
    "삼일절":        "Independence Movement Day",
    "어린이날":      "Children's Day",
    "부처님오신날":   "Buddha's Birthday",
    "석가탄신일":    "Buddha's Birthday",
    "현충일":        "Memorial Day",
    "광복절":        "Liberation Day",
    "추석":          "Chuseok (Korean Thanksgiving)",
    "추석 전날":     "Chuseok Eve",
    "추석 다음날":   "Chuseok Holiday",
    "개천절":        "National Foundation Day",
    "한글날":        "Hangul Proclamation Day",
    "기독탄신일":    "Christmas Day",
    "크리스마스":    "Christmas Day",
    "대체공휴일":    "Substitute Holiday",
    "1월1일":        "New Year's Day",
}

def translate(name_ko: str) -> str:
    if name_ko in HOLIDAY_NAMES:
        return HOLIDAY_NAMES[name_ko]
    for ko, en in HOLIDAY_NAMES.items():
        if ko in name_ko:
            return en
    return name_ko


# ── XML 파싱 ───────────────────────────────────────────────────────────
def parse_xml(text: str) -> list:
    """XML 응답을 파싱해서 item 리스트 반환"""
    root = ET.fromstring(text)
    items = []
    for item in root.findall(".//item"):
        items.append({
            "locdate":   item.findtext("locdate", ""),
            "dateName":  item.findtext("dateName", ""),
            "isHoliday": item.findtext("isHoliday", "N"),
            "dateKind":  item.findtext("dateKind", ""),
            "seq":       item.findtext("seq", ""),
        })
    return items


# ── 단일 테스트 ────────────────────────────────────────────────────────
def test():
    if not SERVICE_KEY:
        print("❌ .env 에 DATA_GO_KR_KEY 없음"); return
    print("🔍 2026년 1월 공휴일 테스트 중...")
    params = {
        "serviceKey": SERVICE_KEY,
        "solYear":    2026,
        "solMonth":   "01",
        "numOfRows":  10,
        "pageNo":     1,
    }
    r = requests.get(ENDPOINT, params=params, timeout=20)
    items = parse_xml(r.text)
    if items:
        print(f"✅ {len(items)}건 수신:")
        for it in items:
            print(f"  {it['locdate']} | {it['dateName']} → {translate(it['dateName'])} | 공휴일:{it['isHoliday']}")
    else:
        print("⚠️ 데이터 없음")
        print(r.text[:300])


# ── DB 초기화 ──────────────────────────────────────────────────────────
def init_db():
    con = sqlite3.connect(DB_FILE)
    con.execute("""
        CREATE TABLE IF NOT EXISTS holidays (
            date_id    TEXT PRIMARY KEY,
            year       INTEGER,
            month      INTEGER,
            day        INTEGER,
            name_ko    TEXT,
            name_en    TEXT,
            is_holiday TEXT
        )
    """)
    con.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
    con.commit(); con.close()


# ── 전체 동기화 ────────────────────────────────────────────────────────
def sync(years=None):
    if not SERVICE_KEY:
        print("❌ .env 에 DATA_GO_KR_KEY 없음"); return False
    if years is None:
        years = [2024, 2025, 2026, 2027]

    print(f"🔄 {years}년 공휴일 수집 중...")
    con = sqlite3.connect(DB_FILE)
    saved = 0

    for year in years:
        for month in range(1, 13):
            params = {
                "serviceKey": SERVICE_KEY,
                "solYear":    year,
                "solMonth":   f"{month:02d}",
                "numOfRows":  20,
                "pageNo":     1,
            }
            try:
                r = requests.get(ENDPOINT, params=params, timeout=20)
                items = parse_xml(r.text)
            except Exception as e:
                print(f"  ⚠️ {year}-{month:02d} 오류: {e}"); continue

            for item in items:
                date_id = str(item["locdate"]).strip()
                if not date_id or len(date_id) != 8:
                    continue
                name_ko = item["dateName"].strip()
                con.execute("""
                    INSERT OR REPLACE INTO holidays
                    (date_id, year, month, day, name_ko, name_en, is_holiday)
                    VALUES (?,?,?,?,?,?,?)
                """, (
                    date_id,
                    int(date_id[:4]),
                    int(date_id[4:6]),
                    int(date_id[6:8]),
                    name_ko,
                    translate(name_ko),
                    item["isHoliday"],
                ))
                saved += 1

        print(f"  ✅ {year}년 완료")

    now = datetime.utcnow().isoformat()
    con.execute("INSERT OR REPLACE INTO meta VALUES ('last_sync',?)", (now,))
    con.commit(); con.close()
    print(f"✅ 동기화 완료 — {saved}건 저장")
    return True


# ── FastAPI ────────────────────────────────────────────────────────────
app = FastAPI(
    title="Korea Public Holidays API",
    description=(
        "Official South Korea public holidays.\n\n"
        "Source: Korea Astronomy and Space Science Institute (한국천문연구원) via data.go.kr\n\n"
        "Covers 2024–2027. Includes substitute holidays (대체공휴일)."
    ),
    version="2.0.0",
)

def _db():
    con = sqlite3.connect(DB_FILE)
    con.row_factory = sqlite3.Row
    return con

def _fmt(row) -> dict:
    return {
        "date":       row["date_id"],
        "year":       row["year"],
        "month":      row["month"],
        "day":        row["day"],
        "name":       row["name_en"],
        "name_ko":    row["name_ko"],
        "is_holiday": row["is_holiday"] == "Y",
    }

@app.get("/", include_in_schema=False)
def root():
    return {"api": "Korea Public Holidays API", "docs": "/docs", "status": "/status"}

@app.get("/holidays/{year}", summary="All holidays in a year", tags=["Holidays"])
def get_year(year: int):
    """
    All public holidays in South Korea for a given year.

    **year**: 4-digit year (e.g. `2026`)
    """
    rows = _db().execute(
        "SELECT * FROM holidays WHERE year=? AND is_holiday='Y' ORDER BY date_id",
        (year,)
    ).fetchall()
    if not rows:
        raise HTTPException(404, f"No data for {year}. Available: 2024–2027.")
    return {"year": year, "count": len(rows), "holidays": [_fmt(r) for r in rows]}

@app.get("/holidays/{year}/{month}", summary="Holidays in a specific month", tags=["Holidays"])
def get_month(year: int, month: int):
    """
    Public holidays for a specific month.

    **year**: 4-digit year | **month**: 1–12
    """
    if not 1 <= month <= 12:
        raise HTTPException(400, "Month must be 1–12.")
    rows = _db().execute(
        "SELECT * FROM holidays WHERE year=? AND month=? AND is_holiday='Y' ORDER BY date_id",
        (year, month)
    ).fetchall()
    return {"year": year, "month": month, "count": len(rows), "holidays": [_fmt(r) for r in rows]}

@app.get("/holidays/check/{date}", summary="Check if a date is a holiday", tags=["Holidays"])
def check_date(date: str):
    """
    Check if a specific date is a public holiday.

    **date**: YYYYMMDD (e.g. `20260101`)
    """
    row = _db().execute("SELECT * FROM holidays WHERE date_id=?", (date,)).fetchone()
    if not row:
        return {"date": date, "is_holiday": False, "name": None}
    return {"date": date, "is_holiday": row["is_holiday"] == "Y", "name": row["name_en"]}

@app.get("/status", summary="Health check", tags=["Meta"])
def status():
    con   = _db()
    count = con.execute("SELECT COUNT(*) FROM holidays WHERE is_holiday='Y'").fetchone()[0]
    last  = con.execute("SELECT value FROM meta WHERE key='last_sync'").fetchone()
    years = con.execute("SELECT DISTINCT year FROM holidays ORDER BY year").fetchall()
    return {
        "status":          "ok",
        "total_holidays":  count,
        "available_years": [r["year"] for r in years],
        "last_sync":       last[0] if last else "never",
    }


# ── 진입점 ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--test", action="store_true", help="2026년 1월 테스트 후 종료")
    p.add_argument("--sync", action="store_true", help="2024~2027 데이터 수집 후 종료")
    args = p.parse_args()

    init_db()

    if args.test:
        test(); sys.exit(0)

    if args.sync:
        sys.exit(0 if sync() else 1)

    cnt = sqlite3.connect(DB_FILE).execute("SELECT COUNT(*) FROM holidays").fetchone()[0]
    if cnt == 0:
        print("❌ DB 비어있음. 먼저: python korea_holidays_api.py --sync")
        sys.exit(1)

    print("\n🚀 서버 시작: http://localhost:8000")
    print("📖 문서:     http://localhost:8000/docs\n")
    uvicorn.run(app, host="0.0.0.0", port=8000)
