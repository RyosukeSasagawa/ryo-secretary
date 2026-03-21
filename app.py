import os
import streamlit as st
from notion_client import Client
from dotenv import load_dotenv
from datetime import datetime, date

load_dotenv()

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

# 教材番号 → インデックスの辞書
MATERIAL_OPTIONS = {f"{i+1}. [{db['subject']}] {db['material']}": i for i, db in enumerate(NOTION_DBS)}


def get_streak(database_id: str) -> int:
    """指定DBの連続学習日数を計算する"""
    try:
        results = notion.databases.query(
            database_id=database_id,
            sorts=[{"property": "日付", "direction": "descending"}],
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
        "名前": {"title": [{"text": {"content": f"{details['date']} {db_info['material']}"}}]},
        "日付": {"date": {"start": details["date"]}},
        "学習時間": {"date": {
            "start": details["start"],
            "end": details["end"],
        }},
        "章": {"rich_text": [{"text": {"content": details["chapter"]}}]},
        "重要ポイント": {"rich_text": [{"text": {"content": details["important"]}}]},
        "疑問": {"rich_text": [{"text": {"content": details["questions"]}}]},
        "気づき・実践": {"rich_text": [{"text": {"content": details["insights"]}}]},
    }
    try:
        notion.pages.create(
            parent={"database_id": db_info["database_id"]},
            properties=properties,
        )
        return True
    except Exception as e:
        st.error(f"Notion登録エラー: {e}")
        return False


# ── セッション初期化 ──────────────────────────────────────────────────────────
STEPS = ["material", "date", "start_time", "end_time", "chapter", "important", "questions", "insights", "confirm", "done"]

if "step" not in st.session_state:
    st.session_state.step = "material"
if "data" not in st.session_state:
    st.session_state.data = {}
if "messages" not in st.session_state:
    st.session_state.messages = [
        {"role": "assistant", "content": "こんにちは！今日何の勉強をしましたか？\n\n教材を番号で選んでください👇"}
    ]

# ── ページ設定 ────────────────────────────────────────────────────────────────
st.set_page_config(page_title="AI学習記録秘書", page_icon="📚")
st.title("📚 AI学習記録秘書")

# ── チャット履歴を表示 ────────────────────────────────────────────────────────
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["role"] == "assistant" and msg["content"] or msg["content"])

# ── 完了済みなら入力欄を非表示 ────────────────────────────────────────────────
if st.session_state.step == "done":
    st.stop()


# ── ステップ別の質問テキスト ──────────────────────────────────────────────────
STEP_QUESTIONS = {
    "date":      f"学習日を入力してください（例: {datetime.now().strftime('%Y-%m-%d')}）\n空Enterで今日になります",
    "start_time": "開始時間を入力してください（例: 04:00）",
    "end_time":   "終了時間を入力してください（例: 04:25）",
    "chapter":    "章・範囲を入力してください（例: Chapter 3）",
    "important":  "重要ポイントを入力してください",
    "questions":  "疑問点を入力してください（なければ「なし」）",
    "insights":   "気づき・実践を入力してください",
}

# ── 教材選択（selectbox）────────────────────────────────────────────────────
if st.session_state.step == "material":
    selected_label = st.selectbox("教材を選択", list(MATERIAL_OPTIONS.keys()), label_visibility="collapsed")
    if st.button("この教材で記録する"):
        idx = MATERIAL_OPTIONS[selected_label]
        st.session_state.data["db_info"] = NOTION_DBS[idx]
        st.session_state.messages.append({"role": "user", "content": selected_label})
        st.session_state.messages.append({"role": "assistant", "content": STEP_QUESTIONS["date"]})
        st.session_state.step = "date"
        st.rerun()

# ── テキスト入力ステップ ──────────────────────────────────────────────────────
elif st.session_state.step in STEP_QUESTIONS:
    user_input = st.chat_input("入力してください...")
    if user_input is not None:
        value = user_input.strip()
        step = st.session_state.step

        # 日付の空入力 → 今日
        if step == "date" and not value:
            value = datetime.now().strftime("%Y-%m-%d")

        st.session_state.data[step] = value
        st.session_state.messages.append({"role": "user", "content": value or "（今日）"})

        # 次ステップへ
        next_steps = list(STEP_QUESTIONS.keys())
        current_idx = next_steps.index(step)
        if current_idx + 1 < len(next_steps):
            next_step = next_steps[current_idx + 1]
            st.session_state.messages.append({"role": "assistant", "content": STEP_QUESTIONS[next_step]})
            st.session_state.step = next_step
        else:
            # 確認ステップへ
            d = st.session_state.data
            db = d["db_info"]
            summary = (
                f"以下の内容でNotionに登録します。よろしいですか？\n\n"
                f"📖 **教材**: {db['material']}\n"
                f"📅 **日付**: {d['date']}\n"
                f"⏰ **時間**: {d['start_time']} ～ {d['end_time']}\n"
                f"📝 **章**: {d['chapter']}\n"
                f"⭐ **重要ポイント**: {d['important']}\n"
                f"❓ **疑問**: {d['questions']}\n"
                f"💡 **気づき**: {d['insights']}"
            )
            st.session_state.messages.append({"role": "assistant", "content": summary})
            st.session_state.step = "confirm"
        st.rerun()

# ── 確認ステップ ──────────────────────────────────────────────────────────────
elif st.session_state.step == "confirm":
    col1, col2 = st.columns(2)
    with col1:
        if st.button("✅ 登録する", use_container_width=True):
            d = st.session_state.data
            db = d["db_info"]
            details = {
                "date": d["date"],
                "start": f"{d['date']}T{d['start_time']}:00+09:00",
                "end":   f"{d['date']}T{d['end_time']}:00+09:00",
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
        if st.button("❌ キャンセル", use_container_width=True):
            st.session_state.messages.append({"role": "user", "content": "キャンセル"})
            st.session_state.messages.append({"role": "assistant", "content": "キャンセルしました。ページを再読み込みすると最初からやり直せます。"})
            st.session_state.step = "done"
            st.rerun()
