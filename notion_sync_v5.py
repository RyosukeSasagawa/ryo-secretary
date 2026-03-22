"""
notion_sync_v5.py
-----------------
Notionの学習記録DBからデータを取得し、SQL Serverに保存、
Plotlyでインタラクティブグラフを生成してAWS S3にアップロード、
NotionページにEmbedブロックとして表示する。

Version: 5.0.0
改善点（v4.pyからの変更）:
  - APIキーを.env化（セキュリティ強化）
  - WSL対応（フォント依存をPlotlyに委譲）
  - DROP→CREATE廃止、差分UPSERT方式に変更
  - matplotlib/seabornをPlotlyに置き換え
  - HTML形式でS3にアップロード
"""

import os
import logging
from datetime import datetime

import boto3
import pandas as pd
import plotly.graph_objects as go
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from notion_client import Client
from plotly.subplots import make_subplots

# ---------------------------------------------------------------------------
# 環境変数の読み込み
# ---------------------------------------------------------------------------
load_dotenv()

NOTION_TOKEN      = os.getenv("NOTION_TOKEN")
NOTION_PAGE_ID    = os.getenv("NOTION_PAGE_ID")   # グラフを埋め込むNotionページID
NOTION_DBS_IDS    = os.getenv("NOTION_DBS_IDS", "")  # カンマ区切りのDB IDリスト

AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION_NAME       = os.getenv("AWS_REGION_NAME", "ap-northeast-1")
AWS_BUCKET_NAME       = os.getenv("AWS_BUCKET_NAME", "study-graphs-2025")

SQL_SERVER   = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE", "StudyNotesDB")

# ---------------------------------------------------------------------------
# ログ設定
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DBリスト
# v4.pyと同じリストを .env の NOTION_DBS_IDS に設定してください（カンマ区切り）
# 例: NOTION_DBS_IDS=abc123def456,ghi789jkl012
# ---------------------------------------------------------------------------
NOTION_DBS: list[str] = [db_id.strip() for db_id in NOTION_DBS_IDS.split(",") if db_id.strip()]

# Notionプロパティ名マッピング（v4.pyの設定に合わせて変更してください）
PROPERTY_MAP = {
    "study_date":   "日付",
    "chapter":      "章",
    "key_points":   "重要ポイント",
    "questions":    "疑問",
    "insights":     "気づき",
    "study_minutes": "学習時間",
}

# ---------------------------------------------------------------------------
# SQL Server
# ---------------------------------------------------------------------------

def get_db_connection():
    """SQL Serverへの接続を返す。"""
    import pyodbc
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)


def ensure_table_exists(cursor) -> None:
    """
    テーブルが存在しない場合のみ CREATE する（DROP は行わない）。
    notion_page_id を UNIQUE キーとして使い、UPSERT で差分更新する。
    """
    cursor.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = 'StudyNotes'
        )
        BEGIN
            CREATE TABLE StudyNotes (
                id              INT IDENTITY(1,1) PRIMARY KEY,
                notion_page_id  VARCHAR(100) NOT NULL,
                db_name         NVARCHAR(200),
                study_date      DATE,
                chapter         NVARCHAR(500),
                key_points      NVARCHAR(MAX),
                questions       NVARCHAR(MAX),
                insights        NVARCHAR(MAX),
                study_minutes   FLOAT,
                created_at      DATETIME DEFAULT GETDATE(),
                updated_at      DATETIME DEFAULT GETDATE(),
                CONSTRAINT UQ_notion_page_id UNIQUE (notion_page_id)
            );
        END
    """)
    cursor.connection.commit()
    logger.info("テーブル確認完了（存在しない場合は新規作成）")


def upsert_record(cursor, record: dict) -> None:
    """
    1件のレコードをUPSERTする。
    notion_page_id が一致するレコードがあれば UPDATE、なければ INSERT。
    SQL Server の MERGE 構文を使用。
    """
    cursor.execute("""
        MERGE INTO StudyNotes AS target
        USING (VALUES (
            :notion_page_id, :db_name, :study_date, :chapter,
            :key_points, :questions, :insights, :study_minutes
        )) AS source (
            notion_page_id, db_name, study_date, chapter,
            key_points, questions, insights, study_minutes
        )
        ON target.notion_page_id = source.notion_page_id
        WHEN MATCHED THEN
            UPDATE SET
                db_name       = source.db_name,
                study_date    = source.study_date,
                chapter       = source.chapter,
                key_points    = source.key_points,
                questions     = source.questions,
                insights      = source.insights,
                study_minutes = source.study_minutes,
                updated_at    = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT (notion_page_id, db_name, study_date, chapter,
                    key_points, questions, insights, study_minutes)
            VALUES (source.notion_page_id, source.db_name, source.study_date,
                    source.chapter, source.key_points, source.questions,
                    source.insights, source.study_minutes);
    """, record)


# ---------------------------------------------------------------------------
# Notion データ取得
# ---------------------------------------------------------------------------

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


def fetch_notion_data(notion: Client, db_id: str, db_name: str = "") -> list[dict]:
    """指定したNotion DBの全レコードを取得してリストで返す。"""
    records = []
    has_more = True
    start_cursor = None

    while has_more:
        kwargs = {"database_id": db_id, "page_size": 100}
        if start_cursor:
            kwargs["start_cursor"] = start_cursor

        response = notion.databases.query(**kwargs)
        pages = response.get("results", [])

        for page in pages:
            props = page.get("properties", {})
            pmap = PROPERTY_MAP

            study_date_str = _get_text(props.get(pmap["study_date"]))
            study_date = None
            if study_date_str:
                try:
                    study_date = datetime.strptime(study_date_str, "%Y-%m-%d").date()
                except ValueError:
                    pass

            study_minutes_raw = props.get(pmap["study_minutes"])
            study_minutes = None
            if study_minutes_raw and study_minutes_raw.get("type") == "number":
                study_minutes = study_minutes_raw.get("number")

            records.append({
                "notion_page_id": page["id"],
                "db_name":        db_name,
                "study_date":     study_date,
                "chapter":        _get_text(props.get(pmap["chapter"])),
                "key_points":     _get_text(props.get(pmap["key_points"])),
                "questions":      _get_text(props.get(pmap["questions"])),
                "insights":       _get_text(props.get(pmap["insights"])),
                "study_minutes":  study_minutes,
            })

        has_more = response.get("has_more", False)
        start_cursor = response.get("next_cursor")

    logger.info(f"  取得: {len(records)} 件 ({db_name or db_id})")
    return records


# ---------------------------------------------------------------------------
# Plotly グラフ生成
# ---------------------------------------------------------------------------

# Plotlyは HTML + JavaScript でレンダリングするため、
# WSLのフォント環境に依存しない。日本語は以下のフォント指定で対応。
PLOTLY_FONT = dict(
    family="'Noto Sans JP', 'Hiragino Sans', 'Meiryo', sans-serif",
    size=13,
    color="#333333",
)


def create_study_graphs(df: pd.DataFrame) -> str:
    """
    学習データからPlotlyのインタラクティブHTMLを生成して文字列で返す。

    グラフ構成（Phase 1）:
      - 上段: 日別学習時間の棒グラフ（教材ごとに色分け）
      - 下段: 累積学習時間の折れ線グラフ
    """
    if df.empty:
        logger.warning("データが空のためグラフをスキップします")
        return "<html><body><p>データがありません</p></body></html>"

    df = df.copy()
    df["study_date"] = pd.to_datetime(df["study_date"])
    df = df.dropna(subset=["study_date", "study_minutes"])
    df = df.sort_values("study_date")

    # 日別・教材別の集計
    daily = (
        df.groupby(["study_date", "db_name"])["study_minutes"]
        .sum()
        .reset_index()
    )

    # 累積（全教材合算）
    cumulative = (
        df.groupby("study_date")["study_minutes"]
        .sum()
        .cumsum()
        .reset_index()
        .rename(columns={"study_minutes": "cumulative_minutes"})
    )

    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=("日別学習時間（教材別）", "累積学習時間"),
        shared_xaxes=True,
        vertical_spacing=0.12,
        row_heights=[0.6, 0.4],
    )

    # 上段: 棒グラフ（教材ごとに色分け）
    for db_name in daily["db_name"].unique():
        subset = daily[daily["db_name"] == db_name]
        fig.add_trace(
            go.Bar(
                x=subset["study_date"],
                y=subset["study_minutes"],
                name=db_name,
                hovertemplate="%{x|%Y-%m-%d}<br>%{y:.0f} 分<extra>" + db_name + "</extra>",
            ),
            row=1, col=1,
        )

    # 下段: 累積折れ線グラフ
    fig.add_trace(
        go.Scatter(
            x=cumulative["study_date"],
            y=cumulative["cumulative_minutes"],
            mode="lines+markers",
            name="累積",
            line=dict(color="#2196F3", width=2),
            hovertemplate="%{x|%Y-%m-%d}<br>累積: %{y:.0f} 分<extra></extra>",
        ),
        row=2, col=1,
    )

    fig.update_layout(
        title=dict(
            text="学習記録ダッシュボード",
            font=dict(size=18),
        ),
        font=PLOTLY_FONT,
        barmode="stack",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor="#FAFAFA",
        paper_bgcolor="#FFFFFF",
        height=700,
        margin=dict(l=60, r=30, t=80, b=40),
    )

    fig.update_yaxes(title_text="学習時間（分）", row=1, col=1)
    fig.update_yaxes(title_text="累積時間（分）", row=2, col=1)

    # full_html=Trueで単独のHTMLとして出力（Notionへの埋め込みに使用）
    return fig.to_html(full_html=True, include_plotlyjs="cdn")


# ---------------------------------------------------------------------------
# AWS S3 アップロード
# ---------------------------------------------------------------------------

def upload_html_to_s3(html_content: str, s3_key: str) -> str | None:
    """
    HTMLをS3にアップロードし、公開URLを返す。
    失敗した場合はNoneを返す。
    """
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION_NAME,
    )
    try:
        s3.put_object(
            Bucket=AWS_BUCKET_NAME,
            Key=s3_key,
            Body=html_content.encode("utf-8"),
            ContentType="text/html; charset=utf-8",
        )
        url = f"https://{AWS_BUCKET_NAME}.s3.{AWS_REGION_NAME}.amazonaws.com/{s3_key}"
        logger.info(f"S3アップロード完了: {url}")
        return url
    except ClientError as e:
        logger.error(f"S3アップロード失敗: {e}")
        return None


# ---------------------------------------------------------------------------
# Notion Embed 更新
# ---------------------------------------------------------------------------

def update_notion_embed(notion: Client, page_id: str, embed_url: str) -> None:
    """
    NotionページのEmbedブロックを更新する。
    - 既存のEmbedブロックがあればURLを更新
    - なければ新規追加
    """
    # 既存ブロックを検索
    children = notion.blocks.children.list(block_id=page_id)
    existing_embed_id = None

    for block in children.get("results", []):
        if block.get("type") == "embed":
            existing_embed_id = block["id"]
            break

    if existing_embed_id:
        notion.blocks.update(
            block_id=existing_embed_id,
            embed={"url": embed_url},
        )
        logger.info(f"Embedブロック更新完了: {existing_embed_id}")
    else:
        notion.blocks.children.append(
            block_id=page_id,
            children=[{"type": "embed", "embed": {"url": embed_url}}],
        )
        logger.info("Embedブロック新規追加完了")


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info("=" * 60)
    logger.info("notion_sync_v5.py 開始")
    logger.info("=" * 60)

    # 設定チェック
    if not NOTION_TOKEN:
        logger.error("NOTION_TOKEN が設定されていません（.envを確認）")
        return
    if not NOTION_DBS:
        logger.error("NOTION_DBS_IDS が設定されていません（.envを確認）")
        return
    if not SQL_SERVER:
        logger.error("SQL_SERVER が設定されていません（.envを確認）")
        return

    notion = Client(auth=NOTION_TOKEN)

    # --- Step 1: Notionからデータ取得 ---
    logger.info("[Step 1] Notionからデータを取得中...")
    all_records: list[dict] = []
    for db_id in NOTION_DBS:
        try:
            # DB名はNotion APIで取得
            db_info = notion.databases.retrieve(database_id=db_id)
            db_name = ""
            title_items = db_info.get("title", [])
            if title_items:
                db_name = title_items[0].get("plain_text", db_id)
            records = fetch_notion_data(notion, db_id, db_name)
            all_records.extend(records)
        except Exception as e:
            logger.error(f"  DB取得エラー ({db_id}): {e}")

    logger.info(f"  合計取得: {len(all_records)} 件")

    if not all_records:
        logger.warning("取得データが0件のため処理を終了します")
        return

    # --- Step 2: SQL ServerにUPSERT ---
    logger.info("[Step 2] SQL Serverに差分保存中...")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        ensure_table_exists(cursor)

        upsert_count = 0
        for record in all_records:
            try:
                upsert_record(cursor, record)
                upsert_count += 1
            except Exception as e:
                logger.error(f"  UPSERTエラー ({record.get('notion_page_id')}): {e}")

        conn.commit()
        conn.close()
        logger.info(f"  UPSERT完了: {upsert_count} 件")
    except Exception as e:
        logger.error(f"  SQL Server接続エラー: {e}")
        return

    # --- Step 3: Plotlyグラフ生成 ---
    logger.info("[Step 3] Plotlyグラフを生成中...")
    df = pd.DataFrame(all_records)
    html_content = create_study_graphs(df)

    # --- Step 4: S3にアップロード ---
    logger.info("[Step 4] AWS S3にアップロード中...")
    today_str = datetime.now().strftime("%Y%m%d")
    s3_key = f"study_graph_{today_str}.html"
    embed_url = upload_html_to_s3(html_content, s3_key)

    if not embed_url:
        logger.error("S3アップロード失敗のためNotionへの反映をスキップします")
        return

    # --- Step 5: NotionページにEmbed反映 ---
    if NOTION_PAGE_ID:
        logger.info("[Step 5] NotionページにEmbed反映中...")
        try:
            update_notion_embed(notion, NOTION_PAGE_ID, embed_url)
        except Exception as e:
            logger.error(f"  Notion Embed更新エラー: {e}")
    else:
        logger.warning("NOTION_PAGE_ID が未設定のためNotionへの反映をスキップします")

    logger.info("=" * 60)
    logger.info("notion_sync_v5.py 完了")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
