import os
from pathlib import Path
import streamlit as st
from notion_client import Client
from dotenv import load_dotenv
from datetime import datetime, date

# .envファイルを絶対パスで読み込む
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

# デバッグ用：トークンが読み込めているか確認
if not os.getenv("NOTION_TOKEN"):
    st.error("❌ NOTION_TOKENが読み込めていません。.envファイルを確認してください。")
    st.code(f"探しているパス: {env_path}")
    st.stop()

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
notion = Client(auth=NOTION_TOKEN)

NOTION_DBS = [
    {"database_id": "2ca223f671538122a535e7f41d900af6", "subject": "語学・英語", "material": "瞬間英作文"},
    {"database_id": "2ca223f671538175b4b3d5c2d380071e", "subject": "語学・英語", "material": "でる1000問"},
    {"database_id": "2ca223f6715381608585ca15a5e3b62e", "subject": "語学・英語", "material": "金の読解"},
    {"database_id": "2ca223f6715381a9866bee5796934ba2", "subject": "語学・英語", "material": "公式TOEIC問題集8"},
    {"database_id": "2ca223f671538102ae62d797b83308a1", "subject": "語学・英語", "material": "金のパッケージ"},
    {"database_id": "2ca223f6715381e8b814df983ca2b559", "subject": "語学・英語", "material": "AI英会話"},
    {"database_id": "2ca223f6715381f3b3f5e25f66e4d97e", "subject": "語学・英語", "material": "オリジナル瞬間英作文"},
    {"database_id": "2ca223f671538189828 1c07c90e44853", "subject": "AI・機械学習", "material": "大規模言語モデル基礎"},
    {"database_id": "2ca223f6715381128502ff3f3edc123e", "subject": "AI・機械学習", "material": "大規模言語モデル応用"},
    {"database_id": "2ca223f67153817ea892cf6eae459a83", "subject": "AI・機械学習", "material": "AI経営寄付講座"},
    {"database_id": "2ca223f671538110ac94c84866a7223f", "subject": "AI・機械学習", "material": "実践へのBridge講座"},
    {"database_id": "2ca223f6715381ffbb3ec25d59c18ad1", "subject": "AI・機械学習", "material": "ゼロから作るDL"},
    {"database_id": "2ca223f671538165bd9df2ac4e378a9a", "subject": "AI・機械学習", "material": "Kaggle"},
    {"database_id": "2ca223f671538143b4aee50c9d6f3ab2", "subject": "統計・データ分析", "material": "統計検定"},
    {"database_id": "2ca223f6715381718bfdc67a838c46fc", "subject": "統計・データ分析", "material": "統計学が最強"},
    {"database_id": "2d3223f6715381b4b889d82c9dfecc13", "subject": "ビジネス", "material": "1億人のための統計解析"},
    {"database_id": "300223f6715381259c2be6ef25fd2a10", "subject": "AI・機械学習", "material": "JOAI Competition"},
]

MATERIAL_OPTIONS = {f"{i+1}. [{db['subject']}] {db['material']}": i for i, db in enumerate(NOTION_DBS)}

WEEKDAY_JP = ["月", "火", "水", "木", "金", "土", "日"]

STEP_QUESTIONS = {
    "start_time": "開始時間を入力してください（例: 04:00）",
    "end_time":   "終了時間を入力してください（例: 04:25）",
    "chapter":    "章・範囲を入力してください（例: Chapter 3）",
    "important":  "重要ポイントを入力してください",
    "questions":  "疑問点を入力してください（なければ「なし」）",
    "insights":   "気づき・実践を入力してください",
}

LABELS_3 = ["前回", "2回前", "3回前"]

# テキストステップの各フィールドキー（recent_recordsのキーと対応）
TEXT_STEP_FIELD = {
    "chapter":   "chapter",
    "important": "important",
    "questions": "questions",
    "insights":  "insights",
}


def format_date_with_weekday(date_str: str) -> str:
    """'2026-03-21' → '2026-03-21（土）'"""
    d = datetime.strptime(date_str, "%Y-%m-%d").date()
    return f"{date_str}（{WEEKDAY_JP[d.weekday()]}）"


def auto_generate_name(date_str: str, material: str) -> str:
    """名前プロパティを自動生成: '2026-03-21（土）瞬間英作文'"""
    return f"{format_date_with_weekday(date_str)}{material}"


def get_recent_records(database_id: str, n: int = 3) -> list:
    """指定DBの直近n件の記録を取得する"""
    try:
        response = notion.request(
            path=f"databases/{database_id}/query",
            method="POST",
            body={
                "sorts": [{"timestamp": "created_time", "direction": "descending"}],
                "page_size": n
            }
        )
        results = response.get("results", [])
        records = []
        for page in results:
            props = page["properties"]
            date_prop = props.get("日付", {}).get("date")
            date_str = date_prop.get("start", "")[:10] if date_prop else ""
            time_prop = props.get("学習時間", {}).get("date")
            start_time, end_time = "", ""
            if time_prop:
                s = time_prop.get("start", "")
                e = time_prop.get("end", "")
                if s and "T" in s:
                    start_time = s[11:16]
                if e and "T" in e:
                    end_time = e[11:16]
            chapter_prop = props.get("章", {}).get("rich_text", [])
            chapter = chapter_prop[0]["text"]["content"] if chapter_prop else ""
            important_prop = props.get("重要ポイント", {}).get("rich_text", [])
            important = important_prop[0]["text"]["content"] if important_prop else ""
            questions_prop = props.get("疑問", {}).get("rich_text", [])
            questions = questions_prop[0]["text"]["content"] if questions_prop else ""
            insights_prop = props.get("気づき・実践", {}).get("rich_text", [])
            insights = insights_prop[0]["text"]["content"] if insights_prop else ""
            records.append({
                "date":      date_str,
                "start_time": start_time,
                "end_time":   end_time,
                "chapter":   chapter,
                "important": important,
                "questions": questions,
                "insights":  insights,
            })
        return records
    except Exception as e:
        st.warning(f"前回の記録取得に失敗: {e}")
        return []


def get_streak(database_id: str) -> int:
    """指定DBの連続学習日数を計算する"""
    try:
        results = notion.request(
            path=f"databases/{database_id}/query",
            method="POST",
            body={"sorts": [{"property": "日付", "direction": "descending"}]},
        ).get("results", [])

        dates = set()
        for page in results:
            d = page["properties"].get("日付", {}).get("date")
            if d and d.get("start"):
                dates.add(d["start"][:10])

        streak = 0
        check = date.today()
        while check.isoformat() in dates:
            streak += 1
            check = date.fromordinal(check.toordinal() - 1)
        return streak
    except Exception:
        return 0


def register_to_notion(db_info: dict, details: dict) -> bool:
    properties = {
        "名前": {"title": [{"text": {"content": details["name"]}}]},
        "日付": {"date": {"start": details["date"]}},
        "学習時間": {"date": {
            "start": details["start"],
            "end":   details["end"],
        }},
        "章":         {"rich_text": [{"text": {"content": details["chapter"]}}]},
        "重要ポイント": {"rich_text": [{"text": {"content": details["important"]}}]},
        "疑問":        {"rich_text": [{"text": {"content": details["questions"]}}]},
        "気づき・実践": {"rich_text": [{"text": {"content": details["insights"]}}]},
    }
    try:
        notion.pages.create(
            parent={"database_id": db_info["database_id"]},
            properties=properties,
        )
        return True
    except Exception as e:
        st.error(f"❌ 登録に失敗しました。エラー詳細: {str(e)}")
        import traceback
        st.code(traceback.format_exc())
        return False


def advance_text_step(step: str, value: str):
    """テキストステップを進める共通処理（ボタン・chat_input 共用）"""
    st.session_state.data[step] = value
    display = value if value else "（なし）"
    st.session_state.messages.append({"role": "user", "content": display})

    all_steps = list(STEP_QUESTIONS.keys())
    current_idx = all_steps.index(step)
    if current_idx + 1 < len(all_steps):
        next_step = all_steps[current_idx + 1]
        st.session_state.messages.append({"role": "assistant", "content": STEP_QUESTIONS[next_step]})
        st.session_state.step = next_step
    else:
        d = st.session_state.data
        db = d["db_info"]
        date_display = format_date_with_weekday(d["date"])
        name = auto_generate_name(d["date"], db["material"])
        summary = (
            f"以下の内容でNotionに登録します。よろしいですか？\n\n"
            f"📖 **教材**: {db['material']}\n"
            f"📝 **名前**: {name}\n"
            f"📅 **日付**: {date_display}\n"
            f"⏰ **時間**: {d['start_time']} ～ {d['end_time']}\n"
            f"📚 **章**: {d['chapter']}\n"
            f"⭐ **重要ポイント**: {d['important']}\n"
            f"❓ **疑問**: {d['questions']}\n"
            f"💡 **気づき**: {d['insights']}"
        )
        st.session_state.messages.append({"role": "assistant", "content": summary})
        st.session_state.step = "confirm"
    st.rerun()


def show_past_time_buttons(field: str, next_step: str, btn_prefix: str):
    """過去3回の時刻ボタンを横並びで表示し、押すと時刻を確定して次のステップへ"""
    recent = st.session_state.recent_records
    btns = [(LABELS_3[i], r[field]) for i, r in enumerate(recent) if r.get(field)]
    if not btns:
        return
    st.markdown("📋 **過去の記録から選ぶ**")
    cols = st.columns(len(btns))
    for i, (label, t) in enumerate(btns):
        with cols[i]:
            if st.button(f"{label}: {t}", key=f"{btn_prefix}_{i}", use_container_width=True):
                st.session_state.data[field] = t
                st.session_state.messages.append({"role": "user", "content": t})
                st.session_state.messages.append({"role": "assistant", "content": STEP_QUESTIONS[next_step]})
                st.session_state.step = next_step
                st.rerun()


def show_past_text_buttons(step: str, field_key: str):
    """過去3回のテキストボタン＋なしボタンを横並びで表示し、押すと次のステップへ"""
    recent = st.session_state.recent_records
    # 各レコードの値を（ラベル, 値）リストに（空の場合はスキップ）
    btns = [(LABELS_3[i], r[field_key]) for i, r in enumerate(recent) if r.get(field_key)]
    if not btns:
        # 過去データなしでもなしボタンは表示
        if st.button("なし（空白）", key=f"none_{step}", use_container_width=True):
            advance_text_step(step, "")
        return
    st.markdown("📋 **過去の記録から選ぶ**")
    cols = st.columns(len(btns) + 1)
    for i, (label, val) in enumerate(btns):
        truncated = val[:15] if len(val) > 15 else val
        with cols[i]:
            if st.button(f"{label}: {truncated}", key=f"past_{step}_{i}", use_container_width=True):
                advance_text_step(step, val)
    with cols[len(btns)]:
        if st.button("なし（空白）", key=f"none_{step}", use_container_width=True):
            advance_text_step(step, "")


# ── セッション初期化 ──────────────────────────────────────────────────────────
if "step" not in st.session_state:
    st.session_state.step = "material"
if "data" not in st.session_state:
    st.session_state.data = {}
if "messages" not in st.session_state:
    st.session_state.messages = [
        {"role": "assistant", "content": "こんにちは！今日何の勉強をしましたか？\n\n教材を番号で選んでください👇"}
    ]
if "recent_records" not in st.session_state:
    st.session_state.recent_records = []

# ── ページ設定 ────────────────────────────────────────────────────────────────
st.set_page_config(page_title="AI学習記録秘書", page_icon="📚")
st.title("📚 AI学習記録秘書")

# ── サイドバー: セッション終了ボタン ─────────────────────────────────────────
with st.sidebar:
    st.markdown("### メニュー")
    if st.button("🚪 セッションを終了する", use_container_width=True):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()

# ── チャット履歴を表示 ────────────────────────────────────────────────────────
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# ── 終了状態 ─────────────────────────────────────────────────────────────────
if st.session_state.step == "finished":
    st.success("お疲れ様でした！またね！👋")
    st.stop()

# ── 登録完了後の選択 ──────────────────────────────────────────────────────────
if st.session_state.step == "done":
    col1, col2 = st.columns(2)
    with col1:
        if st.button("📝 続けて記録する", use_container_width=True):
            st.session_state.step = "material"
            st.session_state.data = {}
            st.session_state.recent_records = []
            st.session_state.messages.append({"role": "assistant", "content": "続けて記録しましょう！\n\n教材を番号で選んでください👇"})
            st.rerun()
    with col2:
        if st.button("👋 終了する", use_container_width=True):
            st.session_state.messages.append({"role": "user", "content": "終了する"})
            st.session_state.step = "finished"
            st.rerun()
    st.stop()

# ── 教材選択（selectbox）────────────────────────────────────────────────────
if st.session_state.step == "material":
    selected_label = st.selectbox("教材を選択", list(MATERIAL_OPTIONS.keys()), label_visibility="collapsed")
    if st.button("この教材で記録する"):
        idx = MATERIAL_OPTIONS[selected_label]
        st.session_state.data["db_info"] = NOTION_DBS[idx]
        st.session_state.recent_records = get_recent_records(NOTION_DBS[idx]["database_id"])
        st.session_state.messages.append({"role": "user", "content": selected_label})
        st.session_state.messages.append({"role": "assistant", "content": "学習日を選択してください📅"})
        st.session_state.step = "date"
        st.rerun()

# ── 学習日選択（カレンダーUI）────────────────────────────────────────────────
elif st.session_state.step == "date":
    selected_date = st.date_input("学習日", value=date.today(), label_visibility="collapsed")
    if st.button("この日付で進む"):
        date_str = selected_date.strftime("%Y-%m-%d")
        st.session_state.data["date"] = date_str
        st.session_state.messages.append({"role": "user", "content": format_date_with_weekday(date_str)})
        st.session_state.messages.append({"role": "assistant", "content": STEP_QUESTIONS["start_time"]})
        st.session_state.step = "start_time"
        st.rerun()

# ── 開始時間（スピナー）─────────────────────────────────────────────────────
elif st.session_state.step == "start_time":
    st.markdown("**開始時間を入力してください**")
    col1, col2 = st.columns(2)
    with col1:
        sh = st.number_input("時", min_value=0, max_value=23, value=4, step=1, key="sh")
    with col2:
        sm = st.number_input("分", min_value=0, max_value=59, value=0, step=5, key="sm")
    if st.button("開始時間を確定"):
        value = f"{sh:02d}:{sm:02d}"
        st.session_state.data["start_time"] = value
        st.session_state.messages.append({"role": "user", "content": value})
        st.session_state.messages.append({"role": "assistant", "content": STEP_QUESTIONS["end_time"]})
        st.session_state.step = "end_time"
        st.rerun()
    show_past_time_buttons("start_time", "end_time", "st_btn")

# ── 終了時間（スピナー）─────────────────────────────────────────────────────
elif st.session_state.step == "end_time":
    st.markdown("**終了時間を入力してください**")
    col1, col2 = st.columns(2)
    with col1:
        eh = st.number_input("時", min_value=0, max_value=23, value=4, step=1, key="eh")
    with col2:
        em = st.number_input("分", min_value=0, max_value=59, value=25, step=5, key="em")
    if st.button("終了時間を確定"):
        end_value = f"{eh:02d}:{em:02d}"
        start_value = st.session_state.data.get("start_time", "00:00")
        if end_value <= start_value:
            st.error("❌ 終了時間は開始時間より後にしてください")
        else:
            st.session_state.data["end_time"] = end_value
            st.session_state.messages.append({"role": "user", "content": end_value})
            st.session_state.messages.append({"role": "assistant", "content": STEP_QUESTIONS["chapter"]})
            st.session_state.step = "chapter"
            st.rerun()
    show_past_time_buttons("end_time", "chapter", "et_btn")

# ── テキスト入力ステップ ──────────────────────────────────────────────────────
elif st.session_state.step in STEP_QUESTIONS:
    step = st.session_state.step
    field_key = TEXT_STEP_FIELD.get(step)

    # 過去記録ボタン＋なしボタン
    if field_key and st.session_state.recent_records:
        show_past_text_buttons(step, field_key)
    elif field_key:
        # 過去記録がない場合もなしボタンを表示
        if st.button("なし（空白）", key=f"none_{step}"):
            advance_text_step(step, "")

    # 手動入力（chat_input）
    user_input = st.chat_input("入力してください...")
    if user_input is not None:
        advance_text_step(step, user_input.strip())

# ── 確認ステップ ──────────────────────────────────────────────────────────────
elif st.session_state.step == "confirm":
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("✅ 登録する", use_container_width=True):
            d = st.session_state.data
            db = d["db_info"]
            name = auto_generate_name(d["date"], db["material"])
            details = {
                "name":      name,
                "date":      d["date"],
                "start":     f"{d['date']}T{d['start_time'].zfill(5)}:00+09:00",
                "end":       f"{d['date']}T{d['end_time'].zfill(5)}:00+09:00",
                "chapter":   d["chapter"],
                "important": d["important"],
                "questions": d["questions"],
                "insights":  d["insights"],
            }
            success = register_to_notion(db, details)
            if success:
                streak = get_streak(db["database_id"])
                msg = f"✅ Notionに登録しました！\n\nお疲れ様です！今日で **{streak}日連続** です！🎉"
            else:
                msg = "❌ 登録に失敗しました。.envのNOTION_TOKENを確認してください。"
            st.session_state.messages.append({"role": "user", "content": "登録する"})
            st.session_state.messages.append({"role": "assistant", "content": msg})
            st.session_state.step = "done"
            st.rerun()
    with col2:
        if st.button("✏️ 修正する", use_container_width=True):
            st.session_state.step = "material"
            st.session_state.data = {}
            st.session_state.recent_records = []
            st.session_state.messages.append({"role": "user", "content": "修正する"})
            st.session_state.messages.append({"role": "assistant", "content": "最初からやり直しましょう。教材を選んでください👇"})
            st.rerun()
    with col3:
        if st.button("❌ キャンセル", use_container_width=True):
            st.session_state.messages.append({"role": "user", "content": "キャンセル"})
            st.session_state.messages.append({"role": "assistant", "content": "キャンセルしました。"})
            st.session_state.step = "finished"
            st.rerun()
