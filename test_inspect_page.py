"""
test_inspect_page.py
--------------------
瞬間英作文DB（2ca223f671538122a535e7f41d900af6）の親ページを取得し、
そのページ内のブロック構成を一覧表示する調査スクリプト。

既存ファイルには一切手を加えない。
"""

import os
import json
from dotenv import load_dotenv
from notion_client import Client

load_dotenv()
notion = Client(auth=os.getenv("NOTION_TOKEN"))

DB_ID = "2ca223f671538122a535e7f41d900af6"  # 瞬間英作文

# ------------------------------------------------------------------
# Step 1: DBの情報を取得して親ページIDを調べる
# ------------------------------------------------------------------
print("=" * 60)
print("Step 1: DBの情報を取得")
print("=" * 60)

db_info = notion.databases.retrieve(database_id=DB_ID)

db_title = "".join(t.get("plain_text", "") for t in db_info.get("title", []))
parent = db_info.get("parent", {})
parent_type = parent.get("type")

print(f"DB名       : {db_title}")
print(f"DB ID      : {DB_ID}")
print(f"親の種類   : {parent_type}")

if parent_type == "page_id":
    parent_page_id = parent["page_id"]
elif parent_type == "block_id":
    parent_page_id = parent["block_id"]
else:
    print(f"親情報（生データ）: {json.dumps(parent, ensure_ascii=False, indent=2)}")
    parent_page_id = None

print(f"親ページID : {parent_page_id}")

# ------------------------------------------------------------------
# Step 2: 親ページの基本情報を取得
# ------------------------------------------------------------------
if parent_page_id:
    print()
    print("=" * 60)
    print("Step 2: 親ブロック/ページの基本情報")
    print("=" * 60)

    # parent_type が block_id の場合は pages.retrieve ではなく blocks.retrieve を使う
    try:
        if parent_type == "block_id":
            block_info = notion.blocks.retrieve(block_id=parent_page_id)
            print(f"ブロック種類   : {block_info.get('type')}")
            print(f"ブロックID     : {parent_page_id}")
            block_parent = block_info.get("parent", {})
            print(f"ブロックの親   : {json.dumps(block_parent, ensure_ascii=False)}")
            # ブロックの親がpage_idなら、そのページIDを使う
            if block_parent.get("type") == "page_id":
                parent_page_id = block_parent["page_id"]
                print(f"→ 実際の親ページID: {parent_page_id}")
                page_info = notion.pages.retrieve(page_id=parent_page_id)
            else:
                page_info = None
        else:
            page_info = notion.pages.retrieve(page_id=parent_page_id)
    except Exception as e:
        print(f"[エラー] {e}")
        page_info = None

    if page_info is None:
        print()
        print("※ 親ページがインテグレーションと共有されていません。")
        print("  Notion側で「Python Sync」との接続を追加してください。")
        print()
        print("  代替手段: DBのブロックIDを直接使ってブロック一覧を取得します。")
        print()
        print("=" * 60)
        print("Step 3 (代替): DB自身のブロック一覧（先頭5件）")
        print("=" * 60)
        try:
            sample = notion.databases.query(database_id=DB_ID, page_size=5)
            for i, page in enumerate(sample.get("results", []), 1):
                props = page.get("properties", {})
                date_p = props.get("日付", {}).get("date") or {}
                date_str = date_p.get("start", "（日付なし）")
                title_items = props.get("章", {}).get("title", []) or props.get("Name", {}).get("title", [])
                title_str = "".join(t.get("plain_text", "") for t in title_items)
                print(f"  {i}. {date_str} | {title_str[:40]}")
        except Exception as e:
            print(f"  [エラー] {e}")
        import sys; sys.exit(0)

    page_info = notion.pages.retrieve(page_id=parent_page_id)
    page_title_prop = page_info.get("properties", {}).get("title", {})
    page_title = "".join(
        t.get("plain_text", "")
        for t in page_title_prop.get("title", [])
    )
    print(f"ページタイトル : {page_title or '（タイトルなし）'}")
    print(f"ページID       : {parent_page_id}")
    print(f"URL            : {page_info.get('url', '')}")

    # さらに上の親があれば表示
    grandparent = page_info.get("parent", {})
    print(f"祖父母の種類   : {grandparent.get('type')}")
    gp_id = grandparent.get("page_id") or grandparent.get("workspace") or grandparent.get("block_id")
    print(f"祖父母ID       : {gp_id}")

    # ------------------------------------------------------------------
    # Step 3: 親ページ内のブロック一覧を取得
    # ------------------------------------------------------------------
    print()
    print("=" * 60)
    print("Step 3: 親ページ内のブロック一覧")
    print("=" * 60)

    BLOCK_TYPE_LABEL = {
        "paragraph":        "段落",
        "heading_1":        "見出し1",
        "heading_2":        "見出し2",
        "heading_3":        "見出し3",
        "bulleted_list_item": "箇条書き",
        "numbered_list_item": "番号リスト",
        "to_do":            "ToDo",
        "toggle":           "トグル",
        "child_page":       "子ページ",
        "child_database":   "子DB",
        "embed":            "埋め込み",
        "image":            "画像",
        "video":            "動画",
        "divider":          "区切り線",
        "callout":          "コールアウト",
        "quote":            "引用",
        "code":             "コード",
        "table":            "テーブル",
        "column_list":      "カラムリスト",
        "column":           "カラム",
        "bookmark":         "ブックマーク",
        "link_preview":     "リンクプレビュー",
    }

    def get_block_text(block: dict) -> str:
        """ブロックから表示テキストを抽出する。"""
        btype = block.get("type", "")
        content = block.get(btype, {})

        # child_page / child_database はタイトルを持つ
        if btype == "child_page":
            return content.get("title", "")
        if btype == "child_database":
            return content.get("title", "")
        if btype == "embed":
            return content.get("url", "")

        # rich_text を持つブロック
        rich = content.get("rich_text", [])
        return "".join(t.get("plain_text", "") for t in rich)[:60]

    def fetch_blocks(block_id: str, depth: int = 0) -> None:
        """再帰的にブロックを取得して表示する（深さ2まで）。"""
        response = notion.blocks.children.list(block_id=block_id)
        blocks = response.get("results", [])

        indent = "  " * depth
        for i, block in enumerate(blocks, 1):
            btype = block.get("type", "unknown")
            label = BLOCK_TYPE_LABEL.get(btype, btype)
            text  = get_block_text(block)
            has_children = block.get("has_children", False)
            child_mark = " ▶" if has_children else ""

            print(f"{indent}{i:>2}. [{label}]{child_mark} {text}")

            # 子ページ・子DB・トグルは1段だけ再帰
            if has_children and depth < 1 and btype in ("child_page", "toggle", "column_list"):
                fetch_blocks(block["id"], depth + 1)

        if not blocks:
            print(f"{indent}（ブロックなし）")

    fetch_blocks(parent_page_id)

print()
print("=" * 60)
print("調査完了")
print("=" * 60)
