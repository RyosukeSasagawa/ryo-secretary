"""
backfill_subject.py
-------------------
StudyNotesテーブルの subject=NULL レコードを
Notionの教材管理DBから取得したマッピングで一括UPDATEする。

fetch_notion_dbs()はSKIP_STATUSES（完了・読了・勉強完了）の教材を除外するため、
完了済み教材の過去レコードにも subject=NULL が残る。
このスクリプトはステータスフィルターなしで全教材を取得し、バックフィルする。

使い方: venv/bin/python backfill_subject.py
"""

import os
from dotenv import load_dotenv
from notion_client import Client
from notion_sync_v5 import get_db_connection

load_dotenv()


def fetch_all_material_subjects(notion: Client) -> dict[str, str]:
    """
    教材管理DBの全ページ（完了済み含む）から
    {教材名: カテゴリ} のマッピングを返す。
    カテゴリ未設定の教材は「その他」とみなす。
    """
    master_db_id = os.getenv("NOTION_MASTER_DB_ID")
    if not master_db_id:
        raise RuntimeError("NOTION_MASTER_DB_IDが.envに設定されていません")

    mapping: dict[str, str] = {}
    has_more = True
    start_cursor = None

    while has_more:
        kwargs = {"database_id": master_db_id, "page_size": 100}
        if start_cursor:
            kwargs["start_cursor"] = start_cursor

        response = notion.databases.query(**kwargs)

        for page in response.get("results", []):
            props = page.get("properties", {})

            # 教材名（titleタイプのプロパティを自動検出）
            material = ""
            for prop in props.values():
                if prop.get("type") == "title":
                    material = "".join(
                        item.get("plain_text", "")
                        for item in prop.get("title", [])
                    )
                    break
            if not material:
                continue

            # カテゴリ（selectタイプ）
            subject = ""
            category_prop = props.get("カテゴリ", {})
            if category_prop.get("type") == "select":
                subject = (category_prop.get("select") or {}).get("name", "")

            mapping[material] = subject if subject else "その他"

        has_more = response.get("has_more", False)
        start_cursor = response.get("next_cursor")

    return mapping


def main() -> None:
    notion = Client(auth=os.getenv("NOTION_TOKEN"))

    print("=" * 50)
    print("backfill_subject.py 開始")
    print("=" * 50)

    # Step 1: Notionから全教材マッピングを取得
    print("\n[Step 1] Notionから全教材マッピングを取得中（完了済み含む）...")
    mapping = fetch_all_material_subjects(notion)
    print(f"  取得: {len(mapping)} 件の教材")
    for material, subject in sorted(mapping.items()):
        print(f"    {material} → {subject}")

    # Step 2: subject=NULL のdb_nameを確認
    print("\n[Step 2] subject=NULL のレコードを確認中...")
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT db_name, COUNT(*) AS cnt
        FROM StudyNotes
        WHERE subject IS NULL
        GROUP BY db_name
        ORDER BY db_name
    """)
    null_rows = cursor.fetchall()

    if not null_rows:
        print("  subject=NULL のレコードはありません。処理不要です。")
        conn.close()
        return

    print(f"  subject=NULL のdb_name: {len(null_rows)} 種類")
    for db_name, cnt in null_rows:
        subject = mapping.get(db_name, "（マッピングなし）")
        print(f"    [{db_name}] {cnt}件 → {subject}")

    # Step 3: 一括UPDATE
    print("\n[Step 3] subject を一括UPDATE中...")
    total_updated = 0
    total_skipped = 0

    for db_name, cnt in null_rows:
        subject = mapping.get(db_name)
        if subject:
            cursor.execute(
                "UPDATE StudyNotes SET subject = ? WHERE db_name = ? AND subject IS NULL",
                (subject, db_name),
            )
            updated = cursor.rowcount
            print(f"  [{db_name}] → {subject}  ({updated}件 UPDATE)")
            total_updated += updated
        else:
            print(f"  [{db_name}] → マッピングなし（スキップ）")
            total_skipped += cnt

    conn.commit()
    conn.close()

    print("\n" + "=" * 50)
    print(f"完了: {total_updated}件 UPDATE / {total_skipped}件 スキップ")
    print("=" * 50)


if __name__ == "__main__":
    main()
