"""
notion_utils.py
---------------
Notion関連の共通ユーティリティ。
notion_sync_v5.py と app.py の両方から使用する。
"""

import os
import logging

from notion_client import Client

logger = logging.getLogger(__name__)

# fetch_notion_dbs()でスキップするステータス
SKIP_STATUSES = {"完了", "読了", "勉強完了"}


def _get_text(prop) -> str:
    """Notionプロパティからテキストを抽出するユーティリティ。"""
    if prop is None:
        return ""
    ptype = prop.get("type")
    if ptype == "title":
        items = prop.get("title", [])
    elif ptype == "rich_text":
        items = prop.get("rich_text", [])
    elif ptype == "number":
        return str(prop.get("number") or "")
    elif ptype == "date":
        date_obj = prop.get("date")
        return date_obj["start"] if date_obj else ""
    else:
        return ""
    return "".join(item.get("plain_text", "") for item in items)


def fetch_notion_dbs(notion: Client) -> list[dict]:
    """
    教材管理DBを自動スキャンし、NOTION_DBSと同形式のリストを返す。

    処理フロー:
      1. 教材管理DB（NOTION_MASTER_DB_ID）の全ページ（= 教材一覧）を取得
      2. ステータスがSKIP_STATUSESに含まれるページはスキップ
      3. 各ページのブロックを取得し、child_databaseブロック（勉強メモDB）のIDを取得
      4. カテゴリ・教材名をページのプロパティから取得

    戻り値の形式（NOTION_DBSと同じ）:
      [{"database_id": "...", "subject": "語学・英語", "material": "瞬間英作文"}, ...]
    """
    master_db_id = os.getenv("NOTION_MASTER_DB_ID")
    if not master_db_id:
        logger.error("NOTION_MASTER_DB_IDが設定されていません（.envを確認）")
        return []

    result = []
    has_more = True
    start_cursor = None

    logger.info(f"  教材管理DBをスキャン中... (ID: {master_db_id})")

    while has_more:
        kwargs = {"database_id": master_db_id, "page_size": 100}
        if start_cursor:
            kwargs["start_cursor"] = start_cursor

        response = notion.databases.query(**kwargs)
        pages = response.get("results", [])

        for page in pages:
            props = page.get("properties", {})

            # ステータスがスキップ対象はスキップ（勉強中・未着手のみ対象）
            status_name = ""
            status_prop = props.get("ステータス", {})
            if status_prop.get("type") == "status":
                status_obj = status_prop.get("status") or {}
                status_name = status_obj.get("name", "")
                if status_name in SKIP_STATUSES:
                    continue

            # 教材名（titleタイプのプロパティを自動検出）
            material = ""
            for prop in props.values():
                if prop.get("type") == "title":
                    material = _get_text(prop)
                    break
            if not material:
                continue

            # カテゴリ（selectタイプ）
            subject = ""
            category_prop = props.get("カテゴリ", {})
            if category_prop.get("type") == "select":
                select_obj = category_prop.get("select") or {}
                subject = select_obj.get("name", "")

            # 子DBを検索（ページ内のchild_databaseブロック）
            page_id = page["id"]
            try:
                blocks = notion.blocks.children.list(block_id=page_id)
                for block in blocks.get("results", []):
                    if block.get("type") == "child_database":
                        # APIが返すIDはハイフン付き → 削除してNOTION_DBSと同形式に
                        child_db_id = block["id"].replace("-", "")
                        result.append({
                            "database_id": child_db_id,
                            "subject":     subject,
                            "material":    material,
                            "status":      status_name,
                        })
            except Exception as e:
                logger.error(f"  ブロック取得エラー ({material}): {e}")

        has_more = response.get("has_more", False)
        start_cursor = response.get("next_cursor")

    logger.info(f"  スキャン完了: {len(result)} 件の勉強メモDBを検出")
    return result
