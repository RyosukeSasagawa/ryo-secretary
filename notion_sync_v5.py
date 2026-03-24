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

import numpy as np
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

NOTION_TOKEN   = os.getenv("NOTION_TOKEN")
NOTION_PAGE_ID = os.getenv("NOTION_PAGE_ID")   # グラフを埋め込むNotionページID

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
# DBリスト（v4.pyと同じ定義）
# subject: 科目分類、material: 教材名（グラフの凡例に使用）
# ---------------------------------------------------------------------------
NOTION_DBS = [
    {"database_id": "2ca223f671538122a535e7f41d900af6", "subject": "語学・英語",       "material": "瞬間英作文"},
    {"database_id": "2ca223f671538175b4b3d5c2d380071e", "subject": "語学・英語",       "material": "文法問題 でる1000問"},
    {"database_id": "2ca223f6715381608585ca15a5e3b62e", "subject": "語学・英語",       "material": "金の読解"},
    {"database_id": "2ca223f6715381a9866bee5796934ba2", "subject": "語学・英語",       "material": "公式TOEIC 問題集8"},
    {"database_id": "2ca223f671538102ae62d797b83308a1", "subject": "語学・英語",       "material": "金のパッケージ"},
    {"database_id": "2ca223f6715381e8b814df983ca2b559", "subject": "語学・英語",       "material": "AI英会話（ChatGPT）"},
    {"database_id": "2ca223f6715381f3b3f5e25f66e4d97e", "subject": "語学・英語",       "material": "オリジナル瞬間英作文"},
    {"database_id": "2ca223f6715381898281c07c90e44853", "subject": "AI・機械学習",     "material": "大規模言語モデル基礎"},
    {"database_id": "2ca223f6715381128502ff3f3edc123e", "subject": "AI・機械学習",     "material": "大規模言語モデル応用"},
    {"database_id": "2ca223f67153817ea892cf6eae459a83", "subject": "AI・機械学習",     "material": "AI経営寄付講座"},
    {"database_id": "2ca223f671538110ac94c84866a7223f", "subject": "AI・機械学習",     "material": "実践へのBridge講座"},
    {"database_id": "2ca223f6715381ffbb3ec25d59c18ad1", "subject": "AI・機械学習",     "material": "ゼロから作るDeep Learning❷"},
    {"database_id": "2ca223f671538165bd9df2ac4e378a9a", "subject": "AI・機械学習",     "material": "Kaggle"},
    {"database_id": "2ca223f671538143b4aee50c9d6f3ab2", "subject": "統計・データ分析", "material": "統計検定2級対策講座"},
    {"database_id": "2ca223f6715381718bfdc67a838c46fc", "subject": "統計・データ分析", "material": "統計学が最強の学問である"},
    {"database_id": "2d3223f6715381b4b889d82c9dfecc13", "subject": "ビジネス",         "material": "1億人のための統計解析"},
    {"database_id": "300223f6715381259c2be6ef25fd2a10", "subject": "AI・機械学習",     "material": "JOAI Competition 2026"},
]

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
            ?, ?, ?, ?, ?, ?, ?, ?
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
    """, (
        record["notion_page_id"],
        record["db_name"],
        record["study_date"],
        record["chapter"],
        record["key_points"],
        record["questions"],
        record["insights"],
        record["study_minutes"],
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
            time_prop = props.get("学習時間", {}).get("date")
            if time_prop:
                start = time_prop.get("start")
                end = time_prop.get("end")
                if start and end:
                    try:
                        s = datetime.fromisoformat(start)
                        e = datetime.fromisoformat(end)
                        study_minutes = (e - s).total_seconds() / 60
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
    学習データから9種類のグラフをタブ切り替えで表示するHTMLダッシュボードを生成して返す。
    戻り値は <!DOCTYPE html> から始まる完全なHTML文字列。
    """
    if df.empty:
        logger.warning("データが空のためグラフをスキップします")
        return "<!DOCTYPE html><html><body><p>データがありません</p></body></html>"

    df = df.copy()
    df["study_date"] = pd.to_datetime(df["study_date"])
    df = df.dropna(subset=["study_date", "study_minutes"])
    df = df.sort_values("study_date")

    # subject列を追加（NOTION_DBSのmaterial→subject対応）
    subject_map = {db["material"]: db["subject"] for db in NOTION_DBS}
    df["subject"] = df["db_name"].map(subject_map).fillna("その他")

    CATEGORY_ORDER = ["語学・英語", "AI・機械学習", "統計・データ分析", "ビジネス", "その他"]
    CATEGORY_COLORS = {
        "語学・英語":     "#4C9BE8",
        "AI・機械学習":   "#E85C4C",
        "統計・データ分析": "#52B788",
        "ビジネス":       "#F4A261",
        "その他":         "#ADB5BD",
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
    # Graph 5: 直近30日ヒートマップ（GitHub草スタイル、緑のカラースケール）
    # ------------------------------------------------------------------
    today = pd.Timestamp.now().normalize()
    start_30 = today - pd.Timedelta(days=29)
    daily_30 = df[df["study_date"] >= start_30].groupby("study_date")["study_minutes"].sum()
    date_range_30 = pd.date_range(start=start_30, end=today)
    daily_full = daily_30.reindex(date_range_30, fill_value=0)

    weekday_labels = ["月", "火", "水", "木", "金", "土", "日"]
    start_wd = date_range_30[0].weekday()  # 最初の日の曜日（0=月）
    n_weeks = (len(date_range_30) - 1 + start_wd) // 7 + 1

    z_heat = np.full((7, n_weeks), np.nan)
    text_heat = [[""] * n_weeks for _ in range(7)]

    for i, (date, minutes) in enumerate(zip(date_range_30, daily_full.values)):
        col = (i + start_wd) // 7
        row = date.weekday()
        z_heat[row][col] = float(minutes)
        text_heat[row][col] = f"{date.strftime('%m/%d')}<br>{minutes:.0f}分"

    # 各週の月曜日をX軸ラベルに使用
    week_labels = []
    for col in range(n_weeks):
        for i, date in enumerate(date_range_30):
            if (i + start_wd) // 7 == col:
                monday = date - pd.Timedelta(days=date.weekday())
                week_labels.append(monday.strftime("%m/%d"))
                break

    fig5 = go.Figure(data=go.Heatmap(
        z=z_heat,
        x=week_labels,
        y=weekday_labels,
        colorscale=[
            [0.0,   "#ebedf0"],
            [0.001, "#c6e48b"],
            [0.3,   "#40c463"],
            [0.6,   "#30a14e"],
            [1.0,   "#216e39"],
        ],
        zmin=0,
        text=text_heat,
        hovertemplate="%{text}<extra></extra>",
        showscale=True,
        colorbar=dict(title="分"),
    ))
    fig5.update_layout(
        title="直近30日の学習ヒートマップ（GitHub草スタイル）",
        yaxis=dict(autorange="reversed"),
        font=PLOTLY_FONT,
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="#FAFAFA",
        margin=dict(l=60, r=30, t=60, b=40),
        height=320,
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
        logger.error("NOTION_DBS が空です（notion_sync_v5.py内のリストを確認）")
        return
    if not SQL_SERVER:
        logger.error("SQL_SERVER が設定されていません（.envを確認）")
        return

    notion = Client(auth=NOTION_TOKEN)

    # --- Step 1: Notionからデータ取得 ---
    logger.info("[Step 1] Notionからデータを取得中...")
    all_records: list[dict] = []
    for db in NOTION_DBS:
        db_id = db["database_id"]
        material = db["material"]
        try:
            records = fetch_notion_data(notion, db_id, material)
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
