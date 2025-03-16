import requests
import pymysql
from config import db, cursor, API_KEY

PROPOSER_API_URL = f"https://open.assembly.go.kr/portal/openapi/nwvrqwxyaytdsfvhu?KEY={API_KEY}&Type=json&pIndex=1&pSize=300"


def fetch_proposer_data():
    response = requests.get(PROPOSER_API_URL)

    if response.status_code != 200:
        print("API 요청 실패:", response.status_code)
        return []

    try:
        data = response.json()
        if "nwvrqwxyaytdsfvhu" not in data:
            print("응답 데이터에 'nwvrqwxyaytdsfvhu' 키 없음")
            return []
        
        rows = data["nwvrqwxyaytdsfvhu"]
        if len(rows) < 2 or "row" not in rows[1]:
            print("데이터 없음")
            return []
        
        return rows[1]["row"]
    
    except Exception as e:
        print("JSON 변환 오류:", str(e))
        return []


def save_proposer_data():
    proposer_list = fetch_proposer_data()

    if not proposer_list:
        print("국회의원 데이터를 가져오지 못함")
        return

    for proposer in proposer_list:
        proposer_name = proposer.get("HG_NM", "").strip()
        bth = proposer.get("BTH_DATE", None)
        job = proposer.get("JOB_RES_NM", "")
        poly = proposer.get("POLY_NM", "")
        orig = proposer.get("ORIG_NM", "")
        cmits = proposer.get("CMITS", "")
        mem_title = proposer.get("MEM_TITLE", "")

        job = job.strip() if job else ""
        poly = poly.strip() if poly else ""
        orig = orig.strip() if orig else ""
        cmits = cmits.strip() if cmits else ""
        mem_title = mem_title.strip() if mem_title else ""

        cursor.execute("SELECT proposerId FROM BillProposer WHERE proposerName = %s", (proposer_name,))
        existing_data = cursor.fetchone()

        if existing_data:
            print(f"{proposer_name} 이미 존재, 건너뜀")
            continue

        query = """
        INSERT INTO BillProposer (proposerName, bth, job, poly, orig, cmits, memTitle)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (proposer_name, bth, job, poly, orig, cmits, mem_title))
    
    db.commit()
    print("모든 국회의원 데이터 저장 완료")


if __name__ == "__main__":
    save_proposer_data()
