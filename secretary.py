import os
from notion_client import Client
from dotenv import load_dotenv
from datetime import datetime

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

def select_material():
    print("\n=== 学習記録AI秘書 ===")
    print("今日勉強した教材を選んでください：\n")
    for i, db in enumerate(NOTION_DBS):
        print(f"  {i+1}. [{db['subject']}] {db['material']}")

    while True:
        try:
            choice = int(input("\n番号を入力: ")) - 1
            if 0 <= choice < len(NOTION_DBS):
                return NOTION_DBS[choice]
        except ValueError:
            pass
        print("正しい番号を入力してください")

def get_study_details():
    print("\n--- 学習内容を教えてください ---")
    date = input("学習日（例: 2026-03-20）[Enterで今日]: ").strip()
    if not date:
        date = datetime.now().strftime("%Y-%m-%d")

    start_time = input("開始時間（例: 04:00）: ").strip()
    start_time = start_time.zfill(5) if start_time else ""
    end_time = input("終了時間（例: 04:25）: ").strip()
    end_time = end_time.zfill(5) if end_time else ""
    chapter = input("章・範囲（例: Chapter 3）: ").strip()
    important = input("重要ポイント: ").strip()
    questions = input("疑問点: ").strip()
    insights = input("気づき・実践: ").strip()

    start_datetime = f"{date}T{start_time}:00+09:00" if start_time else None
    end_datetime = f"{date}T{end_time}:00+09:00" if end_time else None

    return {
        "date": date,
        "start": start_datetime,
        "end": end_datetime,
        "chapter": chapter,
        "important": important,
        "questions": questions,
        "insights": insights,
    }

def register_to_notion(db_info, details):
    properties = {
        "名前": {"title": [{"text": {"content": f"{details['date']} {db_info['material']}"}}]},
        "日付": {"date": {"start": details["date"]}},
        "学習時間": {"date": {
            "start": details["start"],
            "end": details["end"]
        }},
        "章": {"rich_text": [{"text": {"content": details["chapter"]}}]},
        "重要ポイント": {"rich_text": [{"text": {"content": details["important"]}}]},
        "疑問": {"rich_text": [{"text": {"content": details["questions"]}}]},
        "気づき・実践": {"rich_text": [{"text": {"content": details["insights"]}}]},
    }

    try:
        notion.pages.create(
            parent={"database_id": db_info["database_id"]},
            properties=properties
        )
        print(f"\n✅ Notionに登録しました！")
        print(f"   教材: {db_info['material']}")
        print(f"   日付: {details['date']}")
    except Exception as e:
        print(f"\n❌ エラーが発生しました: {e}")

def main():
    db_info = select_material()
    details = get_study_details()

    print(f"\n以下の内容でNotionに登録します：")
    print(f"  教材: {db_info['material']}")
    print(f"  日付: {details['date']}")
    print(f"  時間: {details['start']} ～ {details['end']}")

    confirm = input("\n登録しますか？ (y/n): ").strip().lower()
    if confirm == 'y':
        register_to_notion(db_info, details)
    else:
        print("キャンセルしました")

if __name__ == "__main__":
    main()
