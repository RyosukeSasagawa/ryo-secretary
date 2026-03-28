"""
notion_sync_v5.py
-----------------
Notionの学習記録DBからデータを取得し、SQL Serverに保存、
Plotlyでインタラクティブグラフを生成してAWS S3にアップロード、
NotionページにEmbedブロックとして表示する。

Version: 5.1.0
改善点（v5.0.0からの変更）:
  - NOTION_DBSハードコード廃止
  - 教材管理DBを起動時に自動スキャン（fetch_notion_dbs関数）
  - 新教材追加時にコード修正不要
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
from notion_utils import fetch_notion_dbs, SKIP_STATUSES
from plotly.subplots import make_subplots

# ---------------------------------------------------------------------------
# 環境変数の読み込み
# ---------------------------------------------------------------------------
load_dotenv()

NOTION_TOKEN      = os.getenv("NOTION_TOKEN")
NOTION_PAGE_ID    = os.getenv("NOTION_PAGE_ID")    # グラフを埋め込むNotionページID
NOTION_MASTER_DB_ID = os.getenv("NOTION_MASTER_DB_ID")  # 教材管理DB（自動スキャン用）

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

# Notionプロパティ名マッピング
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
    """SQL Serverへの接続を返す。WSLからはSQL Server認証を使用。"""
    import pyodbc

    sql_user = os.getenv("SQL_USER")
    sql_password = os.getenv("SQL_PASSWORD")
    raw = os.getenv("SQL_SERVER", "")
    # ホスト名をIPに変換し、ポート番号をカンマ形式で指定
    sql_server = raw.replace("SASAGAWAS_PC\\SQLEXPRESS", "172.26.80.1,1433").replace("SASAGAWAS_PC", "172.26.80.1")

    if sql_user and sql_password:
        # SQL Server認証（WSLからの接続用）
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={sql_server};"
            f"DATABASE={SQL_DATABASE};"
            f"UID={sql_user};"
            f"PWD={sql_password};"
            "TrustServerCertificate=yes;"
        )
    else:
        # Windows認証（Windows上で直接実行する場合）
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={SQL_SERVER};"
            f"DATABASE={SQL_DATABASE};"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
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


def ensure_columns_exist(cursor) -> None:
    """
    StudyNotesテーブルに不足カラムをALTERで追加する。
    既に存在する場合はスキップする（冪等）。
    """
    new_columns = [
        ("subject",         "NVARCHAR(100)"),
        ("study_start",     "DATETIME"),
        ("study_end",       "DATETIME"),
        ("material_status", "NVARCHAR(50)"),
    ]
    for col_name, col_type in new_columns:
        cursor.execute("""
            IF NOT EXISTS (
                SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = 'StudyNotes' AND COLUMN_NAME = ?
            )
            BEGIN
                EXEC('ALTER TABLE StudyNotes ADD {} {}')
            END
        """.format(col_name, col_type), (col_name,))
    cursor.connection.commit()
    logger.info("カラム確認完了（不足分を追加）")


def upsert_record(cursor, record: dict) -> None:
    """
    1件のレコードをUPSERTする。
    notion_page_id が一致するレコードがあれば UPDATE、なければ INSERT。
    SQL Server の MERGE 構文を使用。
    pyodbc はパラメータに ? を使い、値をタプルで渡す。
    """
    cursor.execute("""
        MERGE INTO StudyNotes AS target
        USING (VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )) AS source (
            notion_page_id, db_name, study_date, chapter,
            key_points, questions, insights, study_minutes,
            subject, study_start, study_end, material_status
        )
        ON target.notion_page_id = source.notion_page_id
        WHEN MATCHED THEN
            UPDATE SET
                db_name         = source.db_name,
                study_date      = source.study_date,
                chapter         = source.chapter,
                key_points      = source.key_points,
                questions       = source.questions,
                insights        = source.insights,
                study_minutes   = source.study_minutes,
                subject         = source.subject,
                study_start     = source.study_start,
                study_end       = source.study_end,
                material_status = source.material_status,
                updated_at      = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT (notion_page_id, db_name, study_date, chapter,
                    key_points, questions, insights, study_minutes,
                    subject, study_start, study_end, material_status)
            VALUES (source.notion_page_id, source.db_name, source.study_date,
                    source.chapter, source.key_points, source.questions,
                    source.insights, source.study_minutes,
                    source.subject, source.study_start, source.study_end,
                    source.material_status);
    """, (
        record["notion_page_id"],
        record["db_name"],
        record["study_date"],
        record["chapter"],
        record["key_points"],
        record["questions"],
        record["insights"],
        record["study_minutes"],
        record.get("subject"),
        record.get("study_start"),
        record.get("study_end"),
        record.get("material_status"),
    ))


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

            # 「学習時間」はdate型（開始〜終了時刻）なので差分を分に換算する
            study_minutes = None
            study_start = None
            study_end = None
            time_prop = props.get("学習時間", {}).get("date")
            if time_prop:
                start = time_prop.get("start")
                end = time_prop.get("end")
                if start and end:
                    try:
                        s = datetime.fromisoformat(start)
                        e = datetime.fromisoformat(end)
                        study_minutes = (e - s).total_seconds() / 60
                        study_start = s.replace(tzinfo=None)
                        study_end = e.replace(tzinfo=None)
                    except (ValueError, TypeError):
                        study_minutes = None

            records.append({
                "notion_page_id": page["id"],
                "db_name":        db_name,
                "study_date":     study_date,
                "chapter":        _get_text(props.get(pmap["chapter"])),
                "key_points":     _get_text(props.get(pmap["key_points"])),
                "questions":      _get_text(props.get(pmap["questions"])),
                "insights":       _get_text(props.get(pmap["insights"])),
                "study_minutes":  study_minutes,
                "study_start":    study_start,
                "study_end":      study_end,
            })

        has_more = response.get("has_more", False)
        start_cursor = response.get("next_cursor")

    logger.info(f"  取得: {len(records)} 件 ({db_name or db_id})")
    return records


# ---------------------------------------------------------------------------
# SQL Server → DataFrame 読み込み
# ---------------------------------------------------------------------------

def load_df_from_sql() -> pd.DataFrame | None:
    """
    StudyNotesテーブルからグラフ用データを取得してDataFrameで返す。
    失敗した場合はNoneを返す（呼び出し元でNotionデータにフォールバック）。
    """
    try:
        conn = get_db_connection()
        query = """
            SELECT study_date, study_minutes, db_name, subject
            FROM StudyNotes
            WHERE study_date IS NOT NULL
            ORDER BY study_date
        """
        df = pd.read_sql(query, conn)
        conn.close()
        logger.info(f"  SQL Serverからグラフ用データ取得: {len(df)} 件")
        return df
    except Exception as e:
        logger.error(f"  SQL Serverからのデータ取得失敗: {e}")
        return None


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


def create_study_graphs(df: pd.DataFrame, notion_dbs: list[dict]) -> str:
    """
    学習データから9種類のグラフをタブ切り替えで表示するHTMLダッシュボードを生成して返す。
    戻り値は <!DOCTYPE html> から始まる完全なHTML文字列。

    notion_dbs: fetch_notion_dbs()の戻り値（フォールバック時のマッピングに使用）
    """
    if df.empty:
        logger.warning("データが空のためグラフをスキップします")
        return "<!DOCTYPE html><html><body><p>データがありません</p></body></html>"

    df = df.copy()
    df["study_date"] = pd.to_datetime(df["study_date"])
    df = df.dropna(subset=["study_date", "study_minutes"])
    df = df.sort_values("study_date")

    # subject列の確定:
    #   SQL Server起点の場合: subjectカラムが既に存在するのでそのまま使う
    #   フォールバック時（Notionデータ）: notion_dbsでマッピングして生成する
    if "subject" in df.columns:
        df["subject"] = df["subject"].fillna("その他")
    else:
        subject_map = {db["material"]: db["subject"] for db in notion_dbs}
        df["subject"] = df["db_name"].map(subject_map).fillna("その他")

    CATEGORY_ORDER = ["語学・英語", "AI・機械学習", "統計・データ分析", "ビジネス", "コンピューター・IT", "その他"]
    CATEGORY_COLORS = {
        "語学・英語":       "#4C9BE8",
        "AI・機械学習":     "#E85C4C",
        "統計・データ分析":  "#52B788",
        "ビジネス":         "#F4A261",
        "コンピューター・IT": "#7B61FF",
        "その他":           "#ADB5BD",
    }
    COMMON_LAYOUT = dict(
        font=PLOTLY_FONT,
        plot_bgcolor="#FAFAFA",
        paper_bgcolor="#FFFFFF",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=60, r=30, t=60, b=40),
        height=480,
    )

    tab_names = [
        "① 日別学習時間",
        "② 週別集計",
        "③ 月別集計",
        "④ 累積学習時間",
        "⑤ 30日ヒートマップ",
        "⑥ 教材別累積",
        "⑦ カテゴリ別円グラフ",
        "⑧ 曜日別平均",
        "⑨ 週別カテゴリ推移",
        "🔥 Streak",
    ]
    figs = []

    # ------------------------------------------------------------------
    # Graph 1: 日別学習時間（教材別積み上げ棒グラフ）
    # ------------------------------------------------------------------
    daily = df.groupby(["study_date", "db_name"])["study_minutes"].sum().reset_index()
    fig1 = go.Figure()
    for db_name in sorted(daily["db_name"].unique()):
        subset = daily[daily["db_name"] == db_name]
        fig1.add_trace(go.Bar(
            x=subset["study_date"],
            y=subset["study_minutes"],
            name=db_name,
            hovertemplate="%{x|%Y-%m-%d}<br>%{y:.0f} 分<extra>" + db_name + "</extra>",
        ))
    fig1.update_layout(
        title="日別学習時間（教材別積み上げ）",
        barmode="stack",
        yaxis_title="学習時間（分）",
        **COMMON_LAYOUT,
    )
    figs.append(fig1)

    # ------------------------------------------------------------------
    # Graph 2: 週別集計棒グラフ（カテゴリ別積み上げ）
    # ------------------------------------------------------------------
    df["week"] = df["study_date"].dt.to_period("W").dt.start_time
    weekly = df.groupby(["week", "subject"])["study_minutes"].sum().reset_index()
    fig2 = go.Figure()
    for subj in CATEGORY_ORDER:
        subset = weekly[weekly["subject"] == subj]
        if subset.empty:
            continue
        fig2.add_trace(go.Bar(
            x=subset["week"],
            y=subset["study_minutes"],
            name=subj,
            marker_color=CATEGORY_COLORS[subj],
            hovertemplate="%{x|%Y-%m-%d}週<br>%{y:.0f} 分<extra>" + subj + "</extra>",
        ))
    fig2.update_layout(
        title="週別集計（カテゴリ別積み上げ）",
        barmode="stack",
        yaxis_title="学習時間（分）",
        **COMMON_LAYOUT,
    )
    figs.append(fig2)

    # ------------------------------------------------------------------
    # Graph 3: 月別集計棒グラフ（カテゴリ別積み上げ）
    # ------------------------------------------------------------------
    df["month"] = df["study_date"].dt.to_period("M").dt.start_time
    monthly = df.groupby(["month", "subject"])["study_minutes"].sum().reset_index()
    fig3 = go.Figure()
    for subj in CATEGORY_ORDER:
        subset = monthly[monthly["subject"] == subj]
        if subset.empty:
            continue
        fig3.add_trace(go.Bar(
            x=subset["month"],
            y=subset["study_minutes"],
            name=subj,
            marker_color=CATEGORY_COLORS[subj],
            hovertemplate="%{x|%Y-%m}月<br>%{y:.0f} 分<extra>" + subj + "</extra>",
        ))
    fig3.update_layout(
        title="月別集計（カテゴリ別積み上げ）",
        barmode="stack",
        yaxis_title="学習時間（分）",
        **COMMON_LAYOUT,
    )
    figs.append(fig3)

    # ------------------------------------------------------------------
    # Graph 4: 累積学習時間折れ線グラフ
    # ------------------------------------------------------------------
    cumulative = (
        df.groupby("study_date")["study_minutes"]
        .sum()
        .cumsum()
        .reset_index()
    )
    cumulative["cumulative_hours"] = cumulative["study_minutes"] / 60
    fig4 = go.Figure()
    fig4.add_trace(go.Scatter(
        x=cumulative["study_date"],
        y=cumulative["cumulative_hours"],
        mode="lines+markers",
        name="累積学習時間",
        line=dict(color="#2196F3", width=2),
        hovertemplate="%{x|%Y-%m-%d}<br>累積: %{y:.1f} 時間<extra></extra>",
    ))
    fig4.update_layout(
        title="累積学習時間",
        yaxis_title="累積時間（時間）",
        **COMMON_LAYOUT,
    )
    figs.append(fig4)

    # ------------------------------------------------------------------
    # Graph 5: 直近30日ヒートマップ（1行×30列・月またぎ対応）
    # X軸 = 日付（MM/DD）、Y軸 = 1行のみ
    # 0分の日はグレー、学習した日は緑の濃淡で表示
    # ------------------------------------------------------------------
    today = pd.Timestamp.now().normalize()
    start_30 = today - pd.Timedelta(days=29)
    daily_30 = df[df["study_date"] >= start_30].groupby("study_date")["study_minutes"].sum()
    date_range_30 = pd.date_range(start=start_30, end=today)
    daily_full = daily_30.reindex(date_range_30, fill_value=0)

    x_labels_30 = [d.strftime("%m/%d") for d in date_range_30]
    z_30 = [[float(v) for v in daily_full.values]]
    text_30 = [[f"{d.strftime('%m/%d')}<br>{v:.0f}分" for d, v in zip(date_range_30, daily_full.values)]]

    fig5 = go.Figure(data=go.Heatmap(
        z=z_30,
        x=x_labels_30,
        y=[""],
        colorscale=[
            [0.0,   "#ebedf0"],
            [0.001, "#c6e48b"],
            [0.3,   "#40c463"],
            [0.6,   "#30a14e"],
            [1.0,   "#216e39"],
        ],
        zmin=0,
        text=text_30,
        hovertemplate="%{text}<extra></extra>",
        showscale=True,
        colorbar=dict(title="分"),
    ))
    fig5.update_layout(
        title="直近30日の学習ヒートマップ",
        xaxis=dict(tickangle=-45),
        font=PLOTLY_FONT,
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="#FAFAFA",
        margin=dict(l=40, r=30, t=60, b=60),
        height=220,
    )
    figs.append(fig5)

    # ------------------------------------------------------------------
    # Graph 6: 教材別累積時間横棒グラフ（時間単位、昇順ソート）
    # ------------------------------------------------------------------
    material_total = (
        df.groupby("db_name")["study_minutes"]
        .sum()
        .reset_index()
    )
    material_total["study_hours"] = material_total["study_minutes"] / 60
    material_total = material_total.sort_values("study_hours", ascending=True)
    fig6 = go.Figure()
    fig6.add_trace(go.Bar(
        x=material_total["study_hours"],
        y=material_total["db_name"],
        orientation="h",
        marker_color="#4C9BE8",
        hovertemplate="%{y}<br>%{x:.1f} 時間<extra></extra>",
    ))
    fig6.update_layout(
        title="教材別累積学習時間",
        xaxis_title="累積時間（時間）",
        font=PLOTLY_FONT,
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="#FAFAFA",
        margin=dict(l=200, r=30, t=60, b=40),
        height=500,
        hovermode="y unified",
    )
    figs.append(fig6)

    # ------------------------------------------------------------------
    # Graph 7: カテゴリ別円グラフ（ドーナツ型、hole=0.4）
    # ------------------------------------------------------------------
    category_total = df.groupby("subject")["study_minutes"].sum().reset_index()
    fig7 = go.Figure()
    fig7.add_trace(go.Pie(
        labels=category_total["subject"],
        values=category_total["study_minutes"],
        hole=0.4,
        marker_colors=[CATEGORY_COLORS.get(s, "#ADB5BD") for s in category_total["subject"]],
        hovertemplate="%{label}<br>%{value:.0f} 分 (%{percent})<extra></extra>",
    ))
    fig7.update_layout(
        title="カテゴリ別学習時間（ドーナツグラフ）",
        font=PLOTLY_FONT,
        paper_bgcolor="#FFFFFF",
        margin=dict(l=30, r=30, t=60, b=40),
        height=480,
    )
    figs.append(fig7)

    # ------------------------------------------------------------------
    # Graph 8: 曜日別平均学習時間棒グラフ（土日は赤、平日は青）
    # ------------------------------------------------------------------
    WEEKDAY_LABELS = ["月", "火", "水", "木", "金", "土", "日"]
    df["weekday_num"] = df["study_date"].dt.weekday  # 0=月, 6=日
    # 日付単位で合計 → 曜日で平均
    daily_per_day = df.groupby(["study_date", "weekday_num"])["study_minutes"].sum().reset_index()
    weekday_avg_raw = daily_per_day.groupby("weekday_num")["study_minutes"].mean().reset_index()
    weekday_avg = (
        pd.DataFrame({"weekday_num": range(7)})
        .merge(weekday_avg_raw, on="weekday_num", how="left")
        .fillna(0)
    )
    colors_wd = ["#E63946" if d >= 5 else "#4C9BE8" for d in weekday_avg["weekday_num"]]
    fig8 = go.Figure()
    fig8.add_trace(go.Bar(
        x=[WEEKDAY_LABELS[int(d)] for d in weekday_avg["weekday_num"]],
        y=weekday_avg["study_minutes"],
        marker_color=colors_wd,
        hovertemplate="%{x}<br>平均: %{y:.1f} 分<extra></extra>",
    ))
    fig8.update_layout(
        title="曜日別平均学習時間（平日=青、土日=赤）",
        yaxis_title="平均学習時間（分）",
        font=PLOTLY_FONT,
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="#FAFAFA",
        margin=dict(l=60, r=30, t=60, b=40),
        height=400,
        hovermode="x",
    )
    figs.append(fig8)

    # ------------------------------------------------------------------
    # Graph 9: 直近3ヶ月の週別カテゴリ推移（折れ線）
    # ------------------------------------------------------------------
    three_months_ago = today - pd.DateOffset(months=3)
    df_3m = df[df["study_date"] >= three_months_ago].copy()
    df_3m["week"] = df_3m["study_date"].dt.to_period("W").dt.start_time
    weekly_cat = df_3m.groupby(["week", "subject"])["study_minutes"].sum().reset_index()
    fig9 = go.Figure()
    for subj in CATEGORY_ORDER:
        subset = weekly_cat[weekly_cat["subject"] == subj]
        if subset.empty:
            continue
        fig9.add_trace(go.Scatter(
            x=subset["week"],
            y=subset["study_minutes"],
            mode="lines+markers",
            name=subj,
            line=dict(color=CATEGORY_COLORS[subj], width=2),
            hovertemplate="%{x|%Y-%m-%d}週<br>%{y:.0f} 分<extra>" + subj + "</extra>",
        ))
    fig9.update_layout(
        title="直近3ヶ月の週別カテゴリ推移",
        yaxis_title="学習時間（分）",
        **COMMON_LAYOUT,
    )
    figs.append(fig9)

    # ------------------------------------------------------------------
    # Graph 10: Streak（連続学習記録）
    # 上段: 全体の現在Streak / 過去最長Streak（go.Indicator で大きく表示）
    # 下段: カテゴリ別 現在Streak（横棒グラフ）
    # ------------------------------------------------------------------

    # study_minutes > 0 の日付セットを作成（正規化してdateに統一）
    studied_dates = set(
        df[df["study_minutes"] > 0]["study_date"].dt.normalize().tolist()
    )
    today_norm = pd.Timestamp.now().normalize()
    yesterday_norm = today_norm - pd.Timedelta(days=1)

    def _calc_current_streak(dates_set: set) -> int:
        """今日または昨日から遡って連続した日数を返す。"""
        if not dates_set:
            return 0
        start = today_norm if today_norm in dates_set else yesterday_norm
        if start not in dates_set:
            return 0
        count = 0
        check = start
        while check in dates_set:
            count += 1
            check -= pd.Timedelta(days=1)
        return count

    def _calc_max_streak(dates_set: set) -> int:
        """全期間の最長連続日数を返す。"""
        if not dates_set:
            return 0
        sorted_d = sorted(dates_set)
        max_s = cur = 1
        for i in range(1, len(sorted_d)):
            if (sorted_d[i] - sorted_d[i - 1]).days == 1:
                cur += 1
                max_s = max(max_s, cur)
            else:
                cur = 1
        return max(max_s, cur)

    overall_current = _calc_current_streak(studied_dates)
    overall_max = _calc_max_streak(studied_dates)

    # カテゴリ別 現在Streak
    cat_streak_values = []
    for subj in CATEGORY_ORDER:
        cat_dates = set(
            df[(df["study_minutes"] > 0) & (df["subject"] == subj)]["study_date"]
            .dt.normalize().tolist()
        )
        cat_streak_values.append(_calc_current_streak(cat_dates))

    fig10 = make_subplots(
        rows=2, cols=2,
        row_heights=[0.42, 0.58],
        specs=[
            [{"type": "indicator"}, {"type": "indicator"}],
            [{"type": "xy", "colspan": 2}, None],
        ],
        subplot_titles=["現在のStreak", "過去最長Streak", "カテゴリ別 現在のStreak"],
    )
    fig10.add_trace(go.Indicator(
        mode="number",
        value=overall_current,
        number={"suffix": " 日", "font": {"size": 80, "color": "#E85C4C"}},
    ), row=1, col=1)
    fig10.add_trace(go.Indicator(
        mode="number",
        value=overall_max,
        number={"suffix": " 日", "font": {"size": 80, "color": "#F4A261"}},
    ), row=1, col=2)
    fig10.add_trace(go.Bar(
        x=cat_streak_values,
        y=CATEGORY_ORDER,
        orientation="h",
        marker_color=[CATEGORY_COLORS[s] for s in CATEGORY_ORDER],
        text=[f"{v}日" for v in cat_streak_values],
        textposition="outside",
        hovertemplate="%{y}<br>%{x}日連続<extra></extra>",
    ), row=2, col=1)
    fig10.update_layout(
        title="Streak（連続学習記録）",
        showlegend=False,
        font=PLOTLY_FONT,
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="#FAFAFA",
        height=580,
        margin=dict(l=150, r=80, t=80, b=40),
    )
    fig10.update_xaxes(title_text="連続日数", row=2, col=1)
    figs.append(fig10)

    # ------------------------------------------------------------------
    # タブHTMLの組み立て
    # ------------------------------------------------------------------
    # plotly.jsはCDNから1回だけ読み込み、各グラフはdivのみ出力
    div_list = [fig.to_html(full_html=False, include_plotlyjs=False) for fig in figs]

    tab_buttons_html = "\n".join(
        f'    <button class="tab-btn{"  active" if i == 0 else ""}" '
        f'onclick="showTab({i})" id="tab-btn-{i}">{name}</button>'
        for i, name in enumerate(tab_names)
    )
    tab_contents_html = "\n".join(
        f'  <div id="tab-content-{i}" class="tab-content" '
        f'style="display:{"block" if i == 0 else "none"}">\n{div}\n  </div>'
        for i, div in enumerate(div_list)
    )
    n_tabs = len(tab_names)

    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>学習記録ダッシュボード</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js" charset="utf-8"></script>
  <style>
    body {{
      font-family: 'Noto Sans JP', 'Hiragino Sans', 'Meiryo', sans-serif;
      margin: 0;
      padding: 16px;
      background: #f5f5f5;
      color: #333;
    }}
    h1 {{
      font-size: 1.4em;
      margin: 0 0 14px;
      color: #222;
    }}
    .tab-bar {{
      display: flex;
      flex-wrap: wrap;
      gap: 4px;
      margin-bottom: 14px;
    }}
    .tab-btn {{
      padding: 6px 12px;
      border: 1px solid #ccc;
      border-radius: 4px;
      background: #fff;
      cursor: pointer;
      font-size: 0.83em;
      color: #555;
      transition: background 0.15s;
    }}
    .tab-btn:hover {{
      background: #e8f0fe;
    }}
    .tab-btn.active {{
      background: #1a73e8;
      color: #fff;
      border-color: #1a73e8;
      font-weight: bold;
    }}
    .tab-content {{
      background: #fff;
      border-radius: 6px;
      padding: 8px;
      box-shadow: 0 1px 4px rgba(0,0,0,0.1);
    }}
  </style>
</head>
<body>
  <h1>学習記録ダッシュボード</h1>
  <div class="tab-bar">
{tab_buttons_html}
  </div>
{tab_contents_html}
  <script>
    function showTab(idx) {{
      for (var i = 0; i < {n_tabs}; i++) {{
        var content = document.getElementById('tab-content-' + i);
        var btn = document.getElementById('tab-btn-' + i);
        var isActive = (i === idx);
        content.style.display = isActive ? 'block' : 'none';
        btn.classList.toggle('active', isActive);
        if (isActive) {{
          content.querySelectorAll('.plotly-graph-div').forEach(function(div) {{
            Plotly.relayout(div, {{autosize: true}});
          }});
        }}
      }}
    }}
  </script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# AWS S3 ライフサイクルルール設定
# ---------------------------------------------------------------------------

def setup_s3_lifecycle() -> None:
    """
    S3バケットにライフサイクルルールを設定する（冪等）。
    30日以上経過した study_graph_ プレフィックスのHTMLファイルを自動削除。
    ルールが既に存在する場合はスキップする。
    """
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION_NAME,
    )
    rule_id = "delete-old-graphs"

    try:
        response = s3.get_bucket_lifecycle_configuration(Bucket=AWS_BUCKET_NAME)
        existing_rules = response.get("Rules", [])
        if any(r["ID"] == rule_id for r in existing_rules):
            logger.info(f"S3ライフサイクルルール「{rule_id}」は設定済み（スキップ）")
            return
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchLifecycleConfiguration":
            logger.error(f"S3ライフサイクル設定取得エラー: {e}")
            return
        existing_rules = []

    new_rule = {
        "ID": rule_id,
        "Status": "Enabled",
        "Filter": {"Prefix": "study_graph_"},
        "Expiration": {"Days": 30},
    }
    try:
        s3.put_bucket_lifecycle_configuration(
            Bucket=AWS_BUCKET_NAME,
            LifecycleConfiguration={"Rules": existing_rules + [new_rule]},
        )
        logger.info(f"S3ライフサイクルルール「{rule_id}」を設定しました（30日で自動削除）")
    except ClientError as e:
        logger.error(f"S3ライフサイクルルール設定失敗: {e}")


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
    if not NOTION_MASTER_DB_ID:
        logger.error("NOTION_MASTER_DB_ID が設定されていません（.envを確認）")
        return
    if not SQL_SERVER:
        logger.error("SQL_SERVER が設定されていません（.envを確認）")
        return

    notion = Client(auth=NOTION_TOKEN)

    # --- Step 1: 教材管理DBを自動スキャン ---
    logger.info("[Step 1a] 教材管理DBをスキャン中...")
    notion_dbs = fetch_notion_dbs(notion)
    if not notion_dbs:
        logger.error("勉強メモDBが1件も検出されませんでした（教材管理DBを確認）")
        return

    # --- Step 1b: Notionからデータ取得 ---
    logger.info("[Step 1b] Notionからデータを取得中...")
    all_records: list[dict] = []
    for db in notion_dbs:
        db_id       = db["database_id"]
        material    = db["material"]
        subject     = db.get("subject", "")
        status      = db.get("status", "")
        try:
            records = fetch_notion_data(notion, db_id, material)
            for r in records:
                r["subject"]         = subject
                r["material_status"] = status
            all_records.extend(records)
        except Exception as e:
            logger.error(f"  DB取得エラー ({material}): {e}")

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
        ensure_columns_exist(cursor)

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
    df = load_df_from_sql()
    if df is None:
        logger.warning("  SQL Server取得失敗のためNotionデータでフォールバック")
        df = pd.DataFrame(all_records)
    html_content = create_study_graphs(df, notion_dbs)

    # --- Step 4 前: S3ライフサイクルルール確認（初回のみ設定・以降はスキップ）---
    logger.info("[Step 4 前] S3ライフサイクルルールを確認中...")
    setup_s3_lifecycle()

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
