# ryo-secretary 🤖

AI学習記録秘書 — Notion × Claude Code × Python

## 概要
学習記録の入力コストをゼロにするためにClaude Codeで構築したAI秘書。
「今日、瞬間英作文を25分やりました」と話しかけるだけで、
NotionのDBに自動登録される仕組みを実装。

## 作者について
- 生産技術エンジニア（精密機器設計 約18年）
- 社内AI/DX推進リーダーとして活動
- Claude CodeとPythonで個人学習管理システムを構築中

## システム構成
secretary.py（本リポジトリ）
↓ 学習記録を入力
Notion DB（17教材を管理）
↓ 毎朝3時に自動実行
Python + SQL Server（データ蓄積・解析）
↓
Plotly HTMLグラフ → Notion Embedで表示

## 使い方
```bash
# 仮想環境を起動
cd ~/projects/ryo-secretary
.venv/bin/python secretary.py
```

起動すると17教材の中から番号を選択し、
学習時間・内容・気づきを入力するだけでNotionに自動登録されます。

## 技術スタック
- Python 3.12
- Notion API（notion-client）
- Claude Code v2.1.70
- WSL2（Ubuntu on Windows 11）

## 開発環境のセットアップ
```bash
git clone https://github.com/RyosukeSasagawa/ryo-secretary.git
cd ryo-secretary
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env  # APIキーを設定
.venv/bin/python secretary.py
```

## 注意事項
`.env`ファイルにNotion APIキーを設定する必要があります。
`.env.example`を参考に設定してください。
