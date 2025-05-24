import os
import time
import requests
from bs4 import BeautifulSoup
import openai
from config import db, cursor, client, API_URL


EMBED_MODEL = "text-embedding-3-small"
EMBED_DIM   = 1536


def fetch_law_data(size=100):
    r = requests.get(f"{API_URL}&pIndex=1&pSize={size}", timeout=20)
    if r.status_code != 200:
        print("API 요청 실패:", r.status_code)
        return []
    try:
        rows = r.json().get("nzmimeepazxkubdpn", [])
        bills = rows[1]["row"] if len(rows) > 1 and "row" in rows[1] else []
        return sorted(bills, key=lambda x: int(x["BILL_NO"]))
    except Exception as e:
        print("JSON 변환 오류:", e)
        return []


def scrape_law_details(url: str) -> str:
    if not url:
        return "상세 링크 없음"
    try:
        r = requests.get(url, timeout=20)
        if r.status_code != 200:
            return "크롤링 실패"
        soup = BeautifulSoup(r.content, "html.parser")
        div = soup.select_one("div#summaryContentDiv")
        return div.get_text(strip=True) if div else "내용 없음"
    except Exception:
        return "크롤링 오류"


def summarize_text(text: str):
    if not text or text in ["내용 없음", "크롤링 실패", "크롤링 오류"]:
        return "요약 불가", "영향 예측 불가", "용어 설명 불가"

    try:
        # 요약
        summary_prompt = (
            "당신은 법률 전문가입니다. 아래 법안 내용을 일반 시민이 쉽게 이해할 수 있도록 간결하게 요약하세요.\n"
            "다음 형식을 따라 작성해주세요:\n\n"
            "[주요 내용 요약]\n내용을 간결하고 명확하게 작성해주세요.\n\n"
            "[제정 목적]\n이 법안이 왜 만들어졌는지를 설명해주세요."
        )
        summary_response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": summary_prompt},
                {"role": "user", "content": text}
            ],
            temperature=0.5,
            max_tokens=800
        )
        summary = summary_response.choices[0].message.content.strip()

        # 영향 예측
        prediction_prompt = (
            "당신은 정책 분석가입니다. 아래 법안이 통과될 경우 어떤 영향이 있을지 분석하세요.\n"
            "다음 형식을 따라 작성해주세요:\n\n"
            "[긍정적 영향]\n1. ...\n2. ...\n\n"
            "[부정적 영향]\n1. ...\n2. ..."
        )
        prediction_response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": prediction_prompt},
                {"role": "user", "content": text}
            ],
            temperature=0.5,
            max_tokens=800
        )
        prediction = prediction_response.choices[0].message.content.strip()

        # 법률 용어 설명
        term_prompt = (
            "당신은 법률 교육자입니다. 아래 법안 내용 중 일반인이 이해하기 어려울 법률 용어 3개를 선택하고, "
            "각각에 대해 한 문장으로 쉽게 설명해 주세요.\n"
            "다음 형식을 따라 주세요:\n\n"
            "1. 용어: 설명\n2. 용어: 설명\n3. 용어: 설명"
        )
        term_response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": term_prompt},
                {"role": "user", "content": text}
            ],
            temperature=0.5,
            max_tokens=500
        )
        term = term_response.choices[0].message.content.strip()

        return summary, prediction, term

    except Exception as e:
        print("GPT 처리 중 오류:", e)
        return "GPT 요약 실패", "GPT 예측 실패", "GPT 용어 설명 실패"


def generate_embedding(text: str) -> str:
    """정규화된 embedding → '[v1,v2,...]' 문자열(1536차원)"""
    rsp = client.embeddings.create(model=EMBED_MODEL, input=text)
    vec = rsp.data[0].embedding

    # 정규화 진행
    norm = sum(x * x for x in vec) ** 0.5
    if norm == 0:
        norm = 1e-10  # 0으로 안 나누게 처리함

    vec = [x / norm for x in vec]

    return "[" + ",".join(f"{x:.6f}" for x in vec) + "]"


def save_to_db(apiId, billNumber, billTitle, billProposer,
               proposerId, committee, billStatus, billDate,
               detail, summary, prediction, term, embedding_literal):
    cursor.execute("SELECT 1 FROM Bill WHERE apiId = %s", (apiId,))
    exists = cursor.fetchone()

    if exists:
        print(f"[기존] {apiId} - 상태/일자만 업데이트")
        cursor.execute("""
            UPDATE Bill
            SET billStatus = %s,
                billDate   = %s
            WHERE apiId = %s
        """, (billStatus, billDate, apiId))
    else:
        print(f"[신규] {apiId} - GPT 처리 및 INSERT")
        cursor.execute("""
            INSERT INTO Bill (
                apiId, billNumber, billTitle, billProposer, proposerId,
                committee, billStatus, billDate,
                detail, summary, prediction, term, embedding
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, STRING_TO_VECTOR(%s))
        """, (
            apiId, billNumber, billTitle, billProposer, proposerId,
            committee, billStatus, billDate,
            detail, summary, prediction, term, embedding_literal
        ))
    db.commit()


def process_rows(law_list):
    if not law_list:
        print("API에서 데이터를 가져오지 못함")
        return

    for law in law_list:
        apiId = law.get("BILL_ID", "")
        billNumber = law.get("BILL_NO", "")
        billTitle = law.get("BILL_NAME", "").strip()
        billProposer = law.get("PROPOSER", "").split(" ")[0].replace("의원", "").strip()

        DEFAULT_PROPOSER_NAME = "기타"

        cursor.execute("SELECT proposerId FROM BillProposer WHERE proposerName = %s", (billProposer,))
        proposer_result = cursor.fetchone()

        if proposer_result:
            proposerId = proposer_result[0]
        else:
            print(f"제안자 '{billProposer}' 없음 → 기본 제안자 '{DEFAULT_PROPOSER_NAME}' 사용")

            cursor.execute("SELECT proposerId FROM BillProposer WHERE proposerName = %s", (DEFAULT_PROPOSER_NAME,))
            default_result = cursor.fetchone()

            if default_result:
                proposerId = default_result[0]
            else:
                cursor.execute("INSERT INTO BillProposer (proposerName) VALUES (%s)", (DEFAULT_PROPOSER_NAME,))
                db.commit()
                cursor.execute("SELECT LAST_INSERT_ID()")
                proposerId = cursor.fetchone()[0]


        committee = law.get("COMMITTEE") or "미정"
        billStatus = law.get("PROC_RESULT") or "미정"
        billDate = law.get("PROPOSE_DT") or "2000-01-01"

        print(f"\n처리 중: [{billNumber}] {billTitle}")

        cursor.execute("SELECT 1 FROM Bill WHERE apiId = %s", (apiId,))
        exists = cursor.fetchone()

        if exists:
            print(" → 기존 항목: 상태/일자만 갱신")
            save_to_db(apiId, billNumber, billTitle, billProposer, proposerId,
                       committee, billStatus, billDate,
                       None, None, None, None, None)
        else:
            print(" → 신규 항목: GPT 요약 및 벡터 생성")
            detail = scrape_law_details(law.get("DETAIL_LINK", ""))
            summary, prediction, term = summarize_text(detail)
            combined = f"{billTitle}\n{billTitle}\n{billTitle}\n{summary}\n{detail}"
            embedding_literal = generate_embedding(combined)

            save_to_db(apiId, billNumber, billTitle, billProposer, proposerId,
                       committee, billStatus, billDate,
                       detail, summary, prediction, term, embedding_literal)


def initial_data_load():
    process_rows(fetch_law_data(size=200))

def update_latest_laws():
    process_rows(fetch_law_data(size=10))



if __name__ == "__main__":
    initial_data_load()
    while True:
        time.sleep(10800)
        update_latest_laws()



