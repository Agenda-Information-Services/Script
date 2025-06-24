import os
import time
import requests
from bs4 import BeautifulSoup
import openai
from config import db, cursor, client, API_URL, RECOMMEND_API_URL


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
            "당신은 법률 전문가입니다. 아래 법안 내용을 법률 지식이 없는 일반 시민도 이해할 수 있도록 명확하게 설명하십시오.\n"
            "다음 JSON 형식을 엄격하게 지켜서 응답하십시오:\n\n"
            "{\n"
            '  "summary": "법안의 핵심 내용을 간결하고 명확하게 설명",\n'
            '  "purpose": "이 법안이 제정된 배경 또는 필요성을 명확하게 설명"\n'
            "}\n\n"
            "JSON 외의 다른 말머리나 형식은 절대 포함하지 마십시오.\n"
            "법안 내용은 다음과 같습니다:"
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
            "당신은 정책 분석 전문가입니다. 아래 법안이 시행될 경우 예상되는 사회적, 행정적, 경제적 영향을 분석하세요. "
            "긍정적인 영향과 부정적인 영향을 각각 제목과 상세 설명으로 구성하여 구체적으로 작성하십시오.\n\n"
            "다음 JSON 형식에 정확히 맞추어 응답하십시오:\n\n"
            "{\n"
            '  "positive_effects": [\n'
            '    {\n'
            '      "title": "긍정적 영향의 제목",\n'
            '      "description": "그 영향이 발생하는 이유와 맥락을 설명하는 문단"\n'
            '    },\n'
            '    ...\n'
            '  ],\n'
            '  "negative_effects": [\n'
            '    {\n'
            '      "title": "부정적 영향의 제목",\n'
            '      "description": "그 영향이 발생할 수 있는 이유나 우려를 설명하는 문단"\n'
            '    },\n'
            '    ...\n'
            '  ]\n'
            "}\n\n"
            "형식 외의 다른 설명은 포함하지 말고, 반드시 위 JSON 구조를 그대로 따르십시오.\n"
            "법안 내용은 다음과 같습니다:"
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
            "당신은 법률 교육 전문가입니다. 아래 법안 내용 중 일반인이 이해하기 어려울 수 있는 법률 또는 행정 용어를 선별하고, "
            "각 용어를 한 문장으로 알기 쉽게 설명하십시오.\n\n"
            "다음 JSON 형식을 정확히 따르십시오:\n\n"
            "{\n"
            '  "terms": [\n'
            '    {"term": "어려운 용어1", "description": "쉬운 설명1"},\n'
            '    {"term": "어려운 용어2", "description": "쉬운 설명2"}\n'
            "  ]\n"
            "}\n\n"
            "JSON 외의 다른 말머리나 문장은 포함하지 마십시오.\n"
            "법안 내용은 다음과 같습니다:"
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
        print(f"[기존] {apiId} - 상태/일자/위원회 업데이트")
        cursor.execute("""
            UPDATE Bill
            SET billStatus = %s,
                billDate   = %s,
                committee  = %s
            WHERE apiId = %s
        """, (billStatus, billDate, committee, apiId))

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
        raw_proposer = law.get("PROPOSER", "")
        billProposer = raw_proposer.split(" ")[0].split("ㆍ")[0].replace("의원", "").strip()

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
            print(" → 기존 항목: 상태/일자/위원회 갱신")
            save_to_db(apiId, billNumber, billTitle, billProposer, proposerId,
                    committee, billStatus, billDate,
                    None, None, None, None, None)

        else:
            detail = scrape_law_details(law.get("DETAIL_LINK", ""))
            if detail in ["내용 없음", "크롤링 실패", "크롤링 오류"]:
                print(" → 상세 내용 없음/크롤링 오류: 건너뜀")
                continue

            print(" → 신규 항목: GPT 요약 및 벡터 생성")
            summary, prediction, term = summarize_text(detail)
            combined = f"{billTitle}\n{billTitle}\n{billTitle}\n{summary}\n{detail}"
            embedding_literal = generate_embedding(combined)

            save_to_db(apiId, billNumber, billTitle, billProposer, proposerId,
                    committee, billStatus, billDate,
                    detail, summary, prediction, term, embedding_literal)


def refresh_embedding_cache():
    try:
        response = requests.post(RECOMMEND_API_URL, timeout=10)
        if response.status_code == 200:
            print("캐시 갱신 성공:", response.text)
        else:
            print("캐시 갱신 실패:", response.status_code, response.text)
    except Exception as e:
        print("캐시 갱신 요청 오류:", e)


def initial_data_load():
    process_rows(fetch_law_data(size=300))
    # refresh_embedding_cache() 

def update_latest_laws():
    process_rows(fetch_law_data(size=30))
    # refresh_embedding_cache()



if __name__ == "__main__":
    initial_data_load()
    while True:
        time.sleep(10800)
        update_latest_laws()



