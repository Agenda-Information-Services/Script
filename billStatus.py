import requests
from config import db, cursor

def sync_bill_status_link():
    print("BillStatus 링크 동기화 시작")

    cursor.execute("SELECT billId, proposerId, apiId FROM Bill")
    all_bills = cursor.fetchall()

    affected_count = 0

    for billId, proposerId, apiId in all_bills:
        detail_url = f"https://likms.assembly.go.kr/bill/billDetail.do?billId={apiId}"

        cursor.execute("SELECT 1 FROM BillStatus WHERE billId = %s", (billId,))
        exists = cursor.fetchone()

        if exists:
            cursor.execute(
                "UPDATE BillStatus SET link = %s WHERE billId = %s",
                (detail_url, billId)
            )
            print(f"[업데이트] billId={billId}")
        else:
            cursor.execute("""
                INSERT INTO BillStatus (billId, proposerId, billCount, yes, no, bookmarkCount, link)
                VALUES (%s, %s, 0, 0, 0, 0, %s)
            """, (billId, proposerId, detail_url))
            print(f"[생성] billId={billId}")

        affected_count += 1

    db.commit()
    print(f"\n총 {affected_count}건 처리 완료 (BillStatus 링크 동기화)")


if __name__ == "__main__":
    sync_bill_status_link()