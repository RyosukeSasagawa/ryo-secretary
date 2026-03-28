"""
weekly_report.py
----------------
先週の学習データをSQL Serverから集計し、Claude APIでコメントを生成して
HTMLレポートをS3に保存・NotionにEmbed反映する。

実行方法: venv/bin/python weekly_report.py
対象週 : 実行日の直前に完了した月〜日曜（先週）
冪等性 : 同じ週のレコードが既にあれば何もしない
"""

import json
import logging
import os
from datetime import date, timedelta
from pathlib import Path

import google.generativeai as genai
import pandas as pd
import plotly.graph_objects as go
from dotenv import load_dotenv
from notion_client import Client

from notion_sync_v5 import get_db_connection, update_notion_embed, upload_html_to_s3

# ---------------------------------------------------------------------------
# 環境変数の読み込み
# ---------------------------------------------------------------------------
load_dotenv(dotenv_path=Path(__file__).parent / ".env")

GEMINI_API_KEY       = os.getenv("GEMINI_API_KEY")
NOTION_TOKEN         = os.getenv("NOTION_TOKEN")
NOTION_WEEKLY_PAGE_ID = os.getenv("NOTION_WEEKLY_PAGE_ID")

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
# 定数
# ---------------------------------------------------------------------------
WEEKDAY_JP = ["月", "火", "水", "木", "金", "土", "日"]

PLOTLY_FONT = dict(
    family="'Noto Sans JP', 'Hiragino Sans', 'Meiryo', sans-serif",
    size=13,
    color="#333333",
)

CATEGORY_COLORS = {
    "語学・英語":       "#4C9BE8",
    "AI・機械学習":     "#E85C4C",
    "統計・データ分析":  "#52B788",
    "ビジネス":         "#F4A261",
    "コンピューター・IT": "#7B61FF",
    "その他":           "#ADB5BD",
}


# ---------------------------------------------------------------------------
# Step 1: WeeklyReports テーブル作成（冪等）
# ---------------------------------------------------------------------------

def ensure_weekly_reports_table(cursor) -> None:
    """WeeklyReports テーブルが存在しない場合のみ CREATE する。"""
    cursor.execute("""
        IF NOT EXISTS (
            SELECT 1 FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = 'WeeklyReports'
        )
        BEGIN
            CREATE TABLE WeeklyReports (
                id                    INT IDENTITY(1,1) PRIMARY KEY,
                week_start            DATE NOT NULL,
                total_hours           FLOAT,
                category_breakdown    NVARCHAR(MAX),
                best_day              NVARCHAR(20),
                best_material         NVARCHAR(200),
                streak_days           INT,
                goal_achievement_rate FLOAT,
                llm_comment           NVARCHAR(MAX),
                s3_url                NVARCHAR(500),
                created_at            DATETIME DEFAULT GETDATE(),
                CONSTRAINT UQ_week_start UNIQUE (week_start)
            )
        END
    """)
    cursor.connection.commit()
    logger.info("WeeklyReports テーブル確認完了")


# ---------------------------------------------------------------------------
# Step 2: 先週のデータ集計
# ---------------------------------------------------------------------------

def get_last_week_range() -> tuple[date, date]:
    """先週の月曜〜日曜を返す（今日がどの曜日でも直前の完了週）。"""
    today = date.today()
    last_monday = today - timedelta(days=today.weekday() + 7)
    last_sunday = last_monday + timedelta(days=6)
    return last_monday, last_sunday


def collect_weekly_stats(week_start: date, week_end: date) -> dict | None:
    """
    先週の学習データをSQL Serverから集計して dict で返す。
    既にその週のレコードが存在する場合は None を返す（冪等スキップ）。
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    ensure_weekly_reports_table(cursor)

    # 冪等チェック：既に同じ week_start のレコードがあればスキップ
    cursor.execute("SELECT id FROM WeeklyReports WHERE week_start = ?", (week_start,))
    if cursor.fetchone():
        logger.info(f"  week_start={week_start} のレコードが既に存在します（スキップ）")
        conn.close()
        return None

    # 先週のStudyNotesを取得
    df = pd.read_sql(
        """
        SELECT study_date, study_minutes, db_name, subject
        FROM StudyNotes
        WHERE study_date >= ? AND study_date <= ?
          AND study_minutes IS NOT NULL AND study_minutes > 0
        """,
        conn,
        params=(week_start, week_end),
    )
    df["study_date"] = pd.to_datetime(df["study_date"])

    # 全データ（Streak計算用）
    df_all = pd.read_sql(
        "SELECT DISTINCT study_date FROM StudyNotes WHERE study_minutes > 0",
        conn,
    )
    df_all["study_date"] = pd.to_datetime(df_all["study_date"])

    # 目標（全体）取得
    cursor.execute(
        "SELECT monthly_hours FROM StudyGoals WHERE goal_type = 'total'"
    )
    row = cursor.fetchone()
    monthly_goal_hours = row[0] if row else 0.0
    conn.close()

    # --- 集計 ---
    total_hours = df["study_minutes"].sum() / 60 if not df.empty else 0.0

    # カテゴリ別時間
    if not df.empty:
        cat_series = (
            df.groupby("subject")["study_minutes"].sum() / 60
        ).round(2)
        category_breakdown = cat_series.to_dict()
    else:
        category_breakdown = {}

    # 最もよく勉強した曜日
    if not df.empty:
        df["weekday"] = df["study_date"].dt.weekday
        day_totals = df.groupby("weekday")["study_minutes"].sum()
        best_day = WEEKDAY_JP[int(day_totals.idxmax())] + "曜日"
    else:
        best_day = "なし"

    # 最も時間をかけた教材
    if not df.empty:
        mat_totals = df.groupby("db_name")["study_minutes"].sum()
        best_material = mat_totals.idxmax()
    else:
        best_material = "なし"

    # 週末時点のStreak（week_end = 先週の日曜まで）
    studied_dates = set(df_all["study_date"].dt.normalize().tolist())
    week_end_ts = pd.Timestamp(week_end)
    streak_days = 0
    check = week_end_ts
    while check in studied_dates:
        streak_days += 1
        check -= pd.Timedelta(days=1)

    # 全体目標達成率（先週の時間 / 月の目標時間 × 100）
    if monthly_goal_hours and monthly_goal_hours > 0:
        goal_achievement_rate = round(total_hours / monthly_goal_hours * 100, 1)
    else:
        goal_achievement_rate = 0.0

    return {
        "week_start":            week_start,
        "week_end":              week_end,
        "total_hours":           round(total_hours, 2),
        "category_breakdown":    category_breakdown,
        "best_day":              best_day,
        "best_material":         best_material,
        "streak_days":           streak_days,
        "goal_achievement_rate": goal_achievement_rate,
    }


# ---------------------------------------------------------------------------
# Step 3: Gemini API でコメント生成
# ---------------------------------------------------------------------------

def generate_llm_comment(stats: dict) -> str:
    """
    集計データをプロンプトに渡し、Gemini で日本語コメントを生成する。
    API キー未設定の場合は空文字を返す。
    """
    if not GEMINI_API_KEY:
        logger.warning("  GEMINI_API_KEY が未設定のためコメント生成をスキップします")
        return ""

    week_start = stats["week_start"]
    week_end   = stats["week_end"]
    cat_str = "、".join(
        f"{cat}: {h:.1f}時間"
        for cat, h in sorted(
            stats["category_breakdown"].items(), key=lambda x: -x[1]
        )
    ) or "データなし"

    prompt = f"""以下は先週（{week_start}〜{week_end}）の学習データです。

【先週の学習サマリー】
- 合計学習時間: {stats['total_hours']:.1f}時間
- カテゴリ別: {cat_str}
- 最も勉強した曜日: {stats['best_day']}
- 最も時間をかけた教材: {stats['best_material']}
- 週末時点のStreak: {stats['streak_days']}日連続
- 月間目標に対する達成率: {stats['goal_achievement_rate']:.0f}%

上記のデータをもとに、以下の3点を含む300〜500文字の日本語コメントを生成してください。
1. 今週の学習を振り返った励ましのメッセージ
2. 具体的なデータに基づいた分析（良かった点・改善できる点）
3. 来週に向けての具体的なアドバイス

コメントのみを出力してください（前置きや説明は不要です）。"""

    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel("gemini-2.0-flash")
    response = model.generate_content(prompt)
    comment = response.text.strip()
    logger.info(f"  LLMコメント生成完了（{len(comment)}文字）")
    return comment


# ---------------------------------------------------------------------------
# Step 4: WeeklyReports に UPSERT
# ---------------------------------------------------------------------------

def upsert_weekly_report(stats: dict, llm_comment: str, s3_url: str) -> None:
    """WeeklyReports テーブルに1件 UPSERT する。"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        MERGE INTO WeeklyReports AS target
        USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)) AS source (
            week_start, total_hours, category_breakdown,
            best_day, best_material, streak_days,
            goal_achievement_rate, llm_comment, s3_url
        )
        ON target.week_start = source.week_start
        WHEN MATCHED THEN
            UPDATE SET
                total_hours           = source.total_hours,
                category_breakdown    = source.category_breakdown,
                best_day              = source.best_day,
                best_material         = source.best_material,
                streak_days           = source.streak_days,
                goal_achievement_rate = source.goal_achievement_rate,
                llm_comment           = source.llm_comment,
                s3_url                = source.s3_url
        WHEN NOT MATCHED THEN
            INSERT (week_start, total_hours, category_breakdown,
                    best_day, best_material, streak_days,
                    goal_achievement_rate, llm_comment, s3_url)
            VALUES (source.week_start, source.total_hours, source.category_breakdown,
                    source.best_day, source.best_material, source.streak_days,
                    source.goal_achievement_rate, source.llm_comment, source.s3_url);
    """, (
        stats["week_start"],
        stats["total_hours"],
        json.dumps(stats["category_breakdown"], ensure_ascii=False),
        stats["best_day"],
        stats["best_material"],
        stats["streak_days"],
        stats["goal_achievement_rate"],
        llm_comment,
        s3_url,
    ))
    conn.commit()
    conn.close()
    logger.info("  WeeklyReports UPSERT 完了")


# ---------------------------------------------------------------------------
# Step 5: HTML レポート生成
# ---------------------------------------------------------------------------

def _week_label(week_start: date) -> str:
    """'2026-03-21' → '2026年3月第4週' 形式に変換する。"""
    nth = (week_start.day - 1) // 7 + 1
    return f"{week_start.year}年{week_start.month}月 第{nth}週"


def create_report_html(stats: dict, llm_comment: str) -> str:
    """サマリー・グラフ・LLMコメント・過去推移を含む HTML を生成して返す。"""

    week_start = stats["week_start"]
    week_end   = stats["week_end"]
    title      = _week_label(week_start) + " 学習レポート"

    # ── グラフ1: カテゴリ別棒グラフ ──────────────────────────────────────
    cat_data = stats["category_breakdown"]
    if cat_data:
        sorted_cats = sorted(cat_data.items(), key=lambda x: x[1])
        fig_cat = go.Figure(go.Bar(
            x=[v for _, v in sorted_cats],
            y=[k for k, _ in sorted_cats],
            orientation="h",
            marker_color=[CATEGORY_COLORS.get(k, "#ADB5BD") for k, _ in sorted_cats],
            text=[f"{v:.1f}h" for _, v in sorted_cats],
            textposition="outside",
            hovertemplate="%{y}<br>%{x:.1f} 時間<extra></extra>",
        ))
        fig_cat.update_layout(
            title="カテゴリ別学習時間",
            xaxis_title="時間",
            font=PLOTLY_FONT,
            paper_bgcolor="#FFFFFF",
            plot_bgcolor="#FAFAFA",
            margin=dict(l=150, r=80, t=60, b=40),
            height=max(280, len(sorted_cats) * 50 + 100),
        )
    else:
        fig_cat = go.Figure()
        fig_cat.add_annotation(
            text="先週の学習データがありません",
            xref="paper", yref="paper", x=0.5, y=0.5,
            showarrow=False, font=dict(size=18, color="#999"),
        )
        fig_cat.update_layout(height=200, paper_bgcolor="#FFFFFF", font=PLOTLY_FONT)

    cat_div = fig_cat.to_html(full_html=False, include_plotlyjs=False)

    # ── グラフ2: 過去4週間の合計時間推移 ──────────────────────────────────
    try:
        conn = get_db_connection()
        df_hist = pd.read_sql(
            """
            SELECT TOP 4 week_start, total_hours
            FROM WeeklyReports
            ORDER BY week_start DESC
            """,
            conn,
        )
        conn.close()
        df_hist = df_hist.sort_values("week_start")
        df_hist["label"] = df_hist["week_start"].apply(
            lambda d: f"{d.month}/{d.day}週"
        )

        fig_trend = go.Figure(go.Bar(
            x=df_hist["label"].tolist(),
            y=df_hist["total_hours"].tolist(),
            marker_color="#4C9BE8",
            text=[f"{v:.1f}h" for v in df_hist["total_hours"]],
            textposition="outside",
            hovertemplate="%{x}<br>%{y:.1f} 時間<extra></extra>",
        ))
        fig_trend.update_layout(
            title="過去4週間の合計学習時間",
            yaxis_title="時間",
            font=PLOTLY_FONT,
            paper_bgcolor="#FFFFFF",
            plot_bgcolor="#FAFAFA",
            margin=dict(l=60, r=60, t=60, b=40),
            height=300,
        )
        trend_div = fig_trend.to_html(full_html=False, include_plotlyjs=False)
    except Exception:
        trend_div = "<p style='color:#999'>推移グラフの取得に失敗しました</p>"

    # ── 達成率の色 ────────────────────────────────────────────────────────
    rate = stats["goal_achievement_rate"]
    if rate >= 100:
        rate_color = "#52B788"
    elif rate >= 80:
        rate_color = "#C5E384"
    elif rate >= 50:
        rate_color = "#F4A261"
    else:
        rate_color = "#E85C4C"

    # ── LLMコメントのエスケープ ───────────────────────────────────────────
    comment_html = llm_comment.replace("\n", "<br>") if llm_comment else \
        "<span style='color:#aaa'>コメントは生成されませんでした（ANTHROPIC_API_KEYを確認）</span>"

    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js" charset="utf-8"></script>
  <style>
    body {{
      font-family: 'Noto Sans JP', 'Hiragino Sans', 'Meiryo', sans-serif;
      margin: 0; padding: 20px;
      background: #f5f5f5; color: #333;
      max-width: 900px; margin: 0 auto; padding: 20px;
    }}
    h1 {{ font-size: 1.5em; color: #222; margin-bottom: 6px; }}
    .period {{ color: #888; font-size: 0.9em; margin-bottom: 20px; }}
    .cards {{ display: flex; gap: 16px; margin-bottom: 24px; flex-wrap: wrap; }}
    .card {{
      flex: 1; min-width: 160px;
      background: #fff; border-radius: 8px;
      padding: 16px 20px;
      box-shadow: 0 1px 4px rgba(0,0,0,0.1);
      text-align: center;
    }}
    .card-label {{ font-size: 0.8em; color: #888; margin-bottom: 6px; }}
    .card-value {{ font-size: 2em; font-weight: bold; color: #222; }}
    .card-unit {{ font-size: 0.75em; color: #888; }}
    .section {{
      background: #fff; border-radius: 8px; padding: 16px;
      box-shadow: 0 1px 4px rgba(0,0,0,0.1); margin-bottom: 20px;
    }}
    .section h2 {{ font-size: 1.1em; margin: 0 0 12px; color: #444; }}
    .comment-box {{
      background: #f0f7ff; border-left: 4px solid #4C9BE8;
      border-radius: 0 8px 8px 0; padding: 16px 20px;
      line-height: 1.8; font-size: 0.95em; color: #333;
    }}
    .meta {{ font-size: 0.8em; color: #aaa; text-align: right; margin-top: 8px; }}
  </style>
</head>
<body>
  <h1>📊 {title}</h1>
  <p class="period">対象期間: {week_start} 〜 {week_end}</p>

  <div class="cards">
    <div class="card">
      <div class="card-label">合計学習時間</div>
      <div class="card-value">{stats['total_hours']:.1f}<span class="card-unit"> h</span></div>
    </div>
    <div class="card">
      <div class="card-label">Streak</div>
      <div class="card-value">{stats['streak_days']}<span class="card-unit"> 日</span></div>
    </div>
    <div class="card">
      <div class="card-label">月間目標達成率</div>
      <div class="card-value" style="color:{rate_color}">{rate:.0f}<span class="card-unit"> %</span></div>
    </div>
    <div class="card">
      <div class="card-label">ベスト曜日</div>
      <div class="card-value" style="font-size:1.4em">{stats['best_day']}</div>
    </div>
  </div>

  <div class="section">
    <h2>📚 最も時間をかけた教材</h2>
    <p style="font-size:1.1em; margin:0">{stats['best_material']}</p>
  </div>

  <div class="section">
    {cat_div}
  </div>

  <div class="section">
    <h2>🤖 AIコメント</h2>
    <div class="comment-box">{comment_html}</div>
  </div>

  <div class="section">
    {trend_div}
  </div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info("=" * 60)
    logger.info("weekly_report.py 開始")
    logger.info("=" * 60)

    if not NOTION_TOKEN:
        logger.error("NOTION_TOKEN が未設定です（.envを確認）")
        return
    if not GEMINI_API_KEY:
        logger.warning("GEMINI_API_KEY が未設定です（LLMコメントはスキップされます）")

    week_start, week_end = get_last_week_range()
    logger.info(f"  対象週: {week_start}（月）〜 {week_end}（日）")

    # --- Step 2: データ集計 ---
    logger.info("[Step 2] 先週データを集計中...")
    stats = collect_weekly_stats(week_start, week_end)
    if stats is None:
        logger.info("処理済みのため終了します")
        return
    logger.info(f"  合計: {stats['total_hours']:.1f}h / "
                f"Streak: {stats['streak_days']}日 / "
                f"達成率: {stats['goal_achievement_rate']:.0f}%")

    # --- Step 3: LLMコメント生成 ---
    logger.info("[Step 3] Claude APIでコメントを生成中...")
    llm_comment = generate_llm_comment(stats)

    # --- Step 5: HTML生成（S3 URL確定前に仮作成、Step4はURL確定後）---
    logger.info("[Step 5] HTMLレポートを生成中...")
    s3_key = f"weekly_report_{week_start.strftime('%Y%m%d')}.html"
    html_content = create_report_html(stats, llm_comment)

    # --- Step 6: S3アップロード ---
    logger.info("[Step 6] S3にアップロード中...")
    s3_url = upload_html_to_s3(html_content, s3_key)
    if not s3_url:
        logger.error("S3アップロード失敗のため処理を中断します")
        return

    # --- Step 4: WeeklyReports に保存 ---
    logger.info("[Step 4] WeeklyReports テーブルに保存中...")
    upsert_weekly_report(stats, llm_comment, s3_url)

    # --- Step 7: Notion Embed 反映 ---
    if NOTION_WEEKLY_PAGE_ID:
        logger.info("[Step 7] NotionページにEmbed反映中...")
        notion = Client(auth=NOTION_TOKEN)
        try:
            update_notion_embed(notion, NOTION_WEEKLY_PAGE_ID, s3_url)
        except Exception as e:
            logger.error(f"  Notion Embed更新エラー: {e}")
    else:
        logger.warning("NOTION_WEEKLY_PAGE_ID が未設定のためNotionへの反映をスキップします")

    logger.info("=" * 60)
    logger.info(f"weekly_report.py 完了: {s3_url}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
