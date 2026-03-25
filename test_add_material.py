"""
test_add_material.py
--------------------
テンプレートページをベースに、Notionへ新しい教材ページ＋勉強メモDBを作成する。

処理の流れ:
  1. CUIで教材名・カテゴリを入力
  2. 教材管理DB（マスタ）に新しいページを作成（プロパティ設定）
  3. テンプレートページのブロックを取得し新ページにコピー
     - child_database : プロパティ構造を読み取り databases.create で再現
     - image (file型) : Notion内部URLは期限付きのためスキップ
     - その他         : blocks.children.append でそのままコピー
  4. 作成結果と NOTION_DBS への追記用コードを表示

既存ファイルには一切手を加えない。
"""

import os
import sys
from dotenv import load_dotenv
from notion_client import Client

load_dotenv()
notion = Client(auth=os.getenv("NOTION_TOKEN"))

# 教材管理DB（マスタ）のID ── test_inspect_page.py で確認済み
MASTER_DB_ID = "2ca223f6-7153-807d-ae6f-d2d504c68543"

# テンプレートページID
TEMPLATE_PAGE_ID = "2ca223f67153814598c5db262b6420ce"

# Notion API で作成できないプロパティ型（読み取り専用・自動生成）
READONLY_PROP_TYPES = {
    "formula", "rollup",
    "last_edited_time", "last_edited_by",
    "created_time", "created_by",
    "unique_id",
}

CATEGORIES = [
    "語学・英語",
    "AI・機械学習",
    "統計・データ分析",
    "ビジネス",
]


# ------------------------------------------------------------------
# ユーティリティ
# ------------------------------------------------------------------

def fetch_all_blocks(block_id: str) -> list[dict]:
    """ページ/ブロックの直下のブロックを全件取得する（ページネーション対応）。"""
    blocks = []
    has_more = True
    start_cursor = None
    while has_more:
        kwargs = {"block_id": block_id}
        if start_cursor:
            kwargs["start_cursor"] = start_cursor
        resp = notion.blocks.children.list(**kwargs)
        blocks.extend(resp.get("results", []))
        has_more = resp.get("has_more", False)
        start_cursor = resp.get("next_cursor")
    return blocks


def build_copyable_block(block: dict) -> dict | None:
    """
    ブロックを blocks.children.append 用の形式に変換する。
    コピー不可のブロックは None を返す。
    """
    btype = block.get("type")
    content = block.get(btype, {})

    # child_database / child_page はこの関数では扱わない（別処理）
    if btype in ("child_database", "child_page"):
        return None

    # image: external ならコピー可、file（Notion内部）はスキップ
    if btype == "image":
        if content.get("type") == "external":
            return {"type": "image", "image": content}
        else:
            return None  # Notion内部ファイルは期限付きURLのためスキップ

    # unsupported_by_api_client などの特殊タイプはスキップ
    if btype in ("unsupported", "template", "synced_block"):
        return None

    # 通常ブロック（paragraph, heading_*, bulleted_list_item, ...）
    return {btype: content, "type": btype}


def copy_db_properties(template_db_id: str) -> dict:
    """
    テンプレートの子DBからプロパティ定義を取得し、
    API で作成可能なプロパティのみを返す。
    title 型は1つだけ許可（最初に見つかったものを使用）。
    """
    db_info = notion.databases.retrieve(database_id=template_db_id)
    props = {}
    has_title = False

    for name, prop in db_info.get("properties", {}).items():
        ptype = prop["type"]

        # 読み取り専用プロパティはスキップ
        if ptype in READONLY_PROP_TYPES:
            continue

        # title は1つしか持てない
        if ptype == "title":
            if has_title:
                continue
            has_title = True

        # select / multi_select はオプション定義もコピー
        if ptype == "select":
            props[name] = {"select": {"options": prop["select"].get("options", [])}}
        elif ptype == "multi_select":
            props[name] = {"multi_select": {"options": prop["multi_select"].get("options", [])}}
        else:
            props[name] = {ptype: {}}

    # title が1つも無い場合は "名前" を追加（DBには必ずtitleが必要）
    if not has_title:
        props["名前"] = {"title": {}}

    return props


# ------------------------------------------------------------------
# Step 1: CUI 入力
# ------------------------------------------------------------------
print("=" * 50)
print("  新しい教材を追加します（テンプレートコピー）")
print("=" * 50)

material_name = input("\n教材名を入力してください: ").strip()
if not material_name:
    print("エラー: 教材名が空です。")
    sys.exit(1)

print("\nカテゴリを選択してください:")
for i, cat in enumerate(CATEGORIES, 1):
    print(f"  {i}: {cat}")

while True:
    try:
        choice = int(input("\n番号を入力 (1〜4): "))
        if 1 <= choice <= 4:
            break
        print("  1〜4の番号を入力してください。")
    except ValueError:
        print("  数字を入力してください。")

selected_category = CATEGORIES[choice - 1]

print()
print(f"  教材名    : {material_name}")
print(f"  カテゴリ  : {selected_category}")
confirm = input("\nこの内容で作成しますか？ [y/N]: ").strip().lower()
if confirm != "y":
    print("キャンセルしました。")
    sys.exit(0)

# ------------------------------------------------------------------
# Step 2: 教材管理DBに新しいページを作成
# ------------------------------------------------------------------
print("\n[1/3] 教材ページを作成中...")

new_page = notion.pages.create(
    parent={"database_id": MASTER_DB_ID},
    properties={
        "タイトル": {
            "title": [{"text": {"content": material_name}}]
        },
        "カテゴリ": {
            "select": {"name": selected_category}
        },
        "ステータス": {
            # status型はselect型と異なりstatusキーで指定する
            "status": {"name": "未着手"}
        },
        "種別": {
            "select": {"name": "勉強"}
        },
    },
)

new_page_id  = new_page["id"]
new_page_url = new_page["url"]
print(f"  → ページID: {new_page_id}")

# ------------------------------------------------------------------
# Step 3: テンプレートのブロックを取得して新ページにコピー
# ------------------------------------------------------------------
print("[2/3] テンプレートのブロックをコピー中...")

template_blocks = fetch_all_blocks(TEMPLATE_PAGE_ID)
child_db_id = None
skipped = []

for block in template_blocks:
    btype = block.get("type")

    # ── child_database: プロパティ構造を取得して新DBを作成 ──
    if btype == "child_database":
        template_db_name = block["child_database"].get("title", "勉強メモ")
        template_db_id_raw = block["id"]

        print(f"  子DB「{template_db_name}」のプロパティを取得して新規作成中...")
        db_props = copy_db_properties(template_db_id_raw)

        new_db = notion.databases.create(
            parent={"type": "page_id", "page_id": new_page_id},
            title=[{"type": "text", "text": {"content": template_db_name}}],
            properties=db_props,
        )
        child_db_id = new_db["id"]
        print(f"  → 子DB作成完了 (ID: {child_db_id})")
        continue

    # ── その他ブロック: コピー可能なものだけ追加 ──
    copyable = build_copyable_block(block)
    if copyable is None:
        skipped.append(btype)
        continue

    try:
        notion.blocks.children.append(
            block_id=new_page_id,
            children=[copyable],
        )
    except Exception as e:
        print(f"  [警告] {btype} ブロックのコピーに失敗: {e}")
        skipped.append(btype)

if skipped:
    print(f"  ※ スキップしたブロック: {', '.join(skipped)}")
    print("    （Notion内部ファイル画像はAPIコピー不可のため除外）")

# ------------------------------------------------------------------
# Step 4: 結果を表示
# ------------------------------------------------------------------
if child_db_id is None:
    print("\n[警告] 子DBが見つかりませんでした。テンプレートを確認してください。")
    child_db_id_raw = "（取得失敗）"
else:
    child_db_id_raw = child_db_id.replace("-", "")

print()
print("[3/3] 完了！")
print()
print("=" * 50)
print("  教材ページ作成完了！")
print(f"  URL: {new_page_url}")
print("=" * 50)
print()
print("  notion_sync_v5.py の NOTION_DBS に以下を追加してください：")
print()
print(
    f'    {{"database_id": "{child_db_id_raw}", '
    f'"subject": "{selected_category}", '
    f'"material": "{material_name}"}},'
)
print()
print("=" * 50)
