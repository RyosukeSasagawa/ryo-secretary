# ryo-secretary 🤖

**Whale Hunt v1.0.0 — AI学習記録管理システム**

生産技術エンジニアがClaude Codeで構築した、個人学習を完全自動管理するシステム。
「今日、瞬間英作文を25分やりました」と話しかけるだけでNotionに登録され、
SQL ServerへのデータパイプラインとAIによる週次レポートまで全自動で動く。

---

## 作者について

- 生産技術エンジニア（精密機器設計 約18年）
- 社内AI/DX推進リーダーとして活動
- Claude Code × Python で個人学習管理システムを構築・運用中

---

## システム構成

```
【日次フロー】
Notion（学習記録を手入力）
    ↓
app.py（Streamlit WebUI・学習記録入力 / 目標設定）
    ↓
notion_sync_v5.py（毎朝2:00・Windowsタスクスケジューラ）
    ↓
SQL Server（StudyNotes / StudyGoals / WeeklyReports）
    ↓
Plotly（11種類のインタラクティブグラフ生成）
    ↓
AWS S3（HTML保存・30日ライフサイクル自動削除）
    ↓
Notion Embed（グラフをNotionページに自動反映）

【週次フロー】
weekly_report.py（毎週月曜2:30・Windowsタスクスケジューラ）
    ↓
SQL Server（先週の集計データ取得）
    ↓
Gemini API（AIコメント自動生成）
    ↓
AWS S3 + Notion Embed（週次レポートをNotionに自動反映）
```

---

## 主な機能

### 学習記録入力
- Streamlit WebUI でチャット形式の入力体験を実現
- 教材管理DBをNotionから自動スキャン（コード修正不要で新教材追加可能）
- 過去の記録をサジェスト表示・ワンクリックで入力

### 11種類のインタラクティブグラフ（Plotlyタブ切り替え）
| # | グラフ名 | 内容 |
|---|---------|------|
| ① | 日別学習時間 | 教材別積み上げ棒グラフ |
| ② | 週別集計 | カテゴリ別積み上げ |
| ③ | 月別集計 | カテゴリ別積み上げ |
| ④ | 累積学習時間 | 折れ線グラフ |
| ⑤ | 30日ヒートマップ | 月またぎ対応・GitHub草スタイル |
| ⑥ | 教材別累積 | 横棒グラフ |
| ⑦ | カテゴリ別 | ドーナツグラフ |
| ⑧ | 曜日別平均 | 平日/土日で色分け |
| ⑨ | 週別カテゴリ推移 | 直近3ヶ月の折れ線 |
| 🔥 | Streak | 連続学習日数（全体・カテゴリ別） |
| 🎯 | 今月の進捗 | 目標 vs 実績・達成率色分け |

### 月間目標管理
- Streamlit サイドバーからカテゴリ別の月間目標時間を設定
- 全体合計はカテゴリの合計から自動計算
- SQL Server の StudyGoals テーブルに永続保存

### Weeklyレポート自動生成
- 毎週月曜日に先週の学習データを自動集計
- Gemini AI が励まし・分析・来週アドバイスを日本語で生成
- サマリーカード・グラフ・AIコメントをHTML化してNotionに反映
- WeeklyReports テーブルに永続保存（冪等設計）

---

## 技術スタック

| カテゴリ | 技術 |
|---------|------|
| 言語 | Python 3.12 |
| WebUI | Streamlit |
| データ入力 | Notion API（notion-client） |
| データベース | Microsoft SQL Server（pyodbc） |
| グラフ生成 | Plotly |
| クラウドストレージ | AWS S3（boto3） |
| AI | Gemini API（google-generativeai） |
| 自動実行 | Windows タスクスケジューラ / logrotate |
| 開発環境 | WSL2（Ubuntu on Windows 11）/ Claude Code |

---

## ファイル構成

```
ryo-secretary/
├── app.py                  # Streamlit WebUI（学習記録入力・目標設定）
├── notion_sync_v5.py       # メイン同期スクリプト（Notion→SQL Server→S3→Notion）
├── notion_utils.py         # Notion共通ユーティリティ（DB自動スキャン）
├── weekly_report.py        # 週次レポート自動生成（Gemini AI）
├── backfill_subject.py     # subjectカラム一括更新ユーティリティ
├── logrotate.conf          # ログローテーション設定（毎月・3世代保持）
├── requirements.txt        # 依存ライブラリ
└── logs/
    └── sync.log            # 実行ログ
```

---

## セットアップ

### 1. リポジトリをクローン

```bash
git clone https://github.com/RyosukeSasagawa/ryo-secretary.git
cd ryo-secretary
python3 -m venv venv
venv/bin/pip install -r requirements.txt
```

### 2. .env ファイルを作成

以下の環境変数を設定してください。

```
# Notion
NOTION_TOKEN
NOTION_PAGE_ID           # グラフ表示用NotionページID
NOTION_MASTER_DB_ID      # 教材管理DB ID
NOTION_WEEKLY_PAGE_ID    # 週次レポート表示用NotionページID

# SQL Server
SQL_SERVER
SQL_DATABASE
SQL_USER
SQL_PASSWORD

# AWS S3
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_REGION_NAME
AWS_BUCKET_NAME

# Gemini API
GEMINI_API_KEY
```

### 3. SQL Server の準備

- Mixed Mode 認証を有効化
- TCP/IP ポート 1433 を開放
- 学習用ユーザーアカウントを作成
- テーブルは初回実行時に自動作成されます（StudyNotes / StudyGoals / WeeklyReports）

---

## 実行方法

```bash
# 学習記録入力（Streamlit WebUI）
AI秘書起動.bat  # Windows デスクトップショートカット

# 手動同期（Notion → SQL Server → グラフ生成 → S3 → Notion）
venv/bin/python notion_sync_v5.py

# 週次レポート生成
venv/bin/python weekly_report.py
```

---

## 自動実行スケジュール

| スクリプト | タイミング | 内容 |
|-----------|-----------|------|
| notion_sync_v5.py | 毎朝 2:00 | データ同期・グラフ更新 |
| weekly_report.py | 毎週月曜 2:30 | 週次レポート生成 |
| logrotate | 毎月 1日 4:00 | ログローテーション |
