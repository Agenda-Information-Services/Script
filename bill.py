import requests
import time
from bs4 import BeautifulSoup
from config import db, cursor, client, API_URL


def fetch_law_data(size=100):
    response = requests.get(f"{API_URL}&pIndex=1&pSize={size}")
    if response.status_code != 200:
        print("API 요청 실패:", response.status_code)
        return []
    try:
        data = response.json()
        rows = data.get("nzmimeepazxkubdpn", [])
        if len(rows) < 2 or "row" not in rows[1]:
            print("데이터 없음")
            return []
        bills = rows[1]["row"]
        return sorted(bills, key=lambda x: int(x["BILL_NO"]), reverse=False)
    except Exception as e:
        print("JSON 변환 오류:", str(e))
        return []


def scrape_law_details(url):
    if not url:
        return "상세 링크 없음"
    try:
        response = requests.get(url)
        if response.status_code != 200:
            return "크롤링 실패"
        soup = BeautifulSoup(response.content, "html.parser")
        content_div = soup.select_one("div#summaryContentDiv")
        return content_div.text.strip() if content_div else "내용 없음"
    except Exception as e:
        return "크롤링 오류"


def summarize_text(text):
    if not text or text in ["내용 없음", "크롤링 실패", "크롤링 오류"]:
        return "요약 불가", "영향 예측 불가"

    try:
        summary_response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "이 법안을 누구나 이해하기 쉽게 요약해줘."},
                {"role": "user", "content": text}
            ],
            temperature=0.5,
            max_tokens=800,
        )
        summary = summary_response.choices[0].message.content.strip()

        prediction_response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "이 법안이 통과되면 어떤 영향이 있을지 긍정적 영향과 부정적 영향으로 나눠서 설명해줘."},
                {"role": "user", "content": text}
            ],
            temperature=0.5,
            max_tokens=800,
        )
        prediction = prediction_response.choices[0].message.content.strip()

        return summary, prediction
    except Exception as e:
        return "GPT 요약 실패", "GPT 예측 실패"

def save_to_db(apiId, billNumber, billTitle, billProposer, proposerId, committee, billStatus, billDate, detail, summary, prediction):
    print(f"DB 저장 시도: {apiId} - {billTitle}")

    cursor.execute("SELECT * FROM Bill WHERE apiId = %s", (apiId,))
    existing_bill = cursor.fetchone()

    if existing_bill:
        query = """
        UPDATE Bill 
        SET billNumber = %s, billTitle = %s, billProposer = %s, proposerId = %s, committee = %s, 
            billStatus = %s, billDate = %s, detail = %s, summary = %s, prediction = %s
        WHERE apiId = %s
        """
        cursor.execute(query, (billNumber, billTitle, billProposer, proposerId, committee, billStatus, billDate, detail, summary, prediction, apiId))
    else:
        query = """
        INSERT INTO Bill (apiId, billNumber, billTitle, billProposer, proposerId, committee, billStatus, billDate, detail, summary, prediction)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (apiId, billNumber, billTitle, billProposer, proposerId, committee, billStatus, billDate, detail, summary, prediction))

    db.commit()


def initial_data_load():
    law_list = fetch_law_data(size=100)

    if not law_list:
        print("API에서 데이터를 가져오지 못함")
        return

    for law in law_list:
        apiId = law.get("BILL_ID", "")
        billNumber = law.get("BILL_NO", "")
        billTitle = law.get("BILL_NAME", "")
        billProposer = law.get("PROPOSER", "").split(" ")[0].replace("의원", "").strip()

        cursor.execute("SELECT proposerId FROM BillProposer WHERE proposerName = %s", (billProposer,))
        proposer_result = cursor.fetchone()
        proposerId = proposer_result[0] if proposer_result else None

        committee = law.get("COMMITTEE") or "미정"
        billStatus = law.get("PROC_RESULT") or "미정"
        billDate = law.get("PROPOSE_DT") or "2000-01-01"
        detail = scrape_law_details(law.get("DETAIL_LINK", ""))

        summary, prediction = summarize_text(detail)

        save_to_db(apiId, billNumber, billTitle, billProposer, proposerId, committee, billStatus, billDate, detail, summary, prediction)

def update_latest_laws():
    law_list = fetch_law_data(size=10)

    if not law_list:
        print("최신 법안을 가져오지 못함")
        return

    for law in law_list:
        apiId = law.get("BILL_ID", "")
        billNumber = law.get("BILL_NO", "")
        billTitle = law.get("BILL_NAME", "")
        billProposer = law.get("PROPOSER", "").split(" ")[0].replace("의원", "").strip()

        cursor.execute("SELECT proposerId FROM BillProposer WHERE proposerName = %s", (billProposer,))
        proposer_result = cursor.fetchone()
        proposerId = proposer_result[0] if proposer_result else None

        committee = law.get("COMMITTEE") or "미정"
        billStatus = law.get("PROC_RESULT") or "미정"
        billDate = law.get("PROPOSE_DT") or "2000-01-01"
        detail = scrape_law_details(law.get("DETAIL_LINK", ""))

        summary, prediction = summarize_text(detail)

        save_to_db(apiId, billNumber, billTitle, billProposer, proposerId, committee, billStatus, billDate, detail, summary, prediction)


if __name__ == "__main__":
    initial_data_load()
    while True:
        time.sleep(43200) 
        update_latest_laws() 
