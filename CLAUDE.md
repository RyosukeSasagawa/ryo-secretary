# RYO AI引き継ぎドキュメント

## このファイルの目的
どのAIがこのプロジェクトを引き継いでも、RYOさんの状況・システム構成・タスクを即座に理解できるようにする。

## RYOプロフィール
- 名前: 笹川 亮介（RYO）
- 職種: 生産技術エンジニア（精密機器設計 約18年）
- AI/DX経験: 約3年（社内AI実装リーダー）
- 転職活動: 製造業社内AI実装ポジションを狙って活動中
- ツール方針:
  - 会社業務: Microsoft Copilot
  - 個人開発: Claude / Gemini API
  - コーディング: Claude Code（WSL + VSCode）

## 現在のシステム構成
Notion（学習記録を手入力）→ Python（毎朝3時自動実行）→ SQL Server（蓄積・解析）→ Plotly HTMLグラフ → Notion Embed表示

## Notion構成
- スペース: Whale Hunt v1.0.0
- 教材管理DB（マスタ）+ 教材ごとの勉強メモDB
- 共通プロパティ: 日付/章/重要ポイント/疑問/気づき/学習時間

## WSL環境
- OS: Windows 11 + WSL2（Ubuntu）
- Node.js: v24.13.1 / Claude Code: v2.1.70
- 起動方法: PowerShell → wsl → cd ~/projects/XXX → claude
- プロジェクト置き場: ~/projects/

## プロジェクト一覧
- ~/projects/ryo-secretary/  ← AI秘書（現在開発中）
- ~/projects/kaggle-project/ ← Kaggle関連
- ~/projects/ur-sim-zero/    ← URシミュレーション

## 進行中タスク: AI秘書 Phase 1
目標:
  RYO「今日、瞬間英作文を朝4時から25分やりました」
  → Claude Codeが質問 → RYOが回答
  → NotionのDBに自動登録 + SQL Serverにも保存

次にやること:
  1. app.pyで実際の毎日の学習記録を運用開始
  2. notion-sync_v4.pyも notion-client==2.3.0 対応か確認
  3. app.pyのデスクトップショートカットを作成（起動を楽にする）

## 完了済みタスク
- 2026-03-20: GASエラー修正（日付フォーマット・トークン管理）
- 2026-03-20: 不要なGASとGoogleスプレッドシートを削除
- 2026-03-20: WSLフォルダ構成を整理（~/projects/配下に統一）
- 2026-03-20: CLAUDE.md作成（AI引き継ぎ体制を確立）
- 2026-03-20: AI秘書Phase 1完成（secretary.py動作確認済み）
- 2026-03-21: GitHubにryo-secretaryを公開（ポートフォリオ化）
- 2026-03-21: Streamlit WebアプリUI（app.py）作成・改善
- 2026-03-21: 時刻スピナー・バリデーション・連続入力・終了ボタン実装
- 2026-03-21: notion-client v2.3.0にダウングレード（v3.0.0でquery廃止のため）
- 2026-03-22: Auto-updateエラー修正（npm --prefix ~/.npm-global）
- 2026-03-22: 全項目に過去記録ボタン+なしボタン追加（サジェスト機能完成）

## このファイルの更新ルール
- 作業完了のたびに「次にやること」を更新する
- 完了タスクは1行で「完了済みタスク」に追記する
- 詳細な作業記録はNotionに残す（このファイルは常に簡潔に保つ）
- このファイルは「AIへの地図」なので最大100行以内を目安にする

## AIへの作業スタイル指示
- 専門用語は都度説明する（IT専門家ではなく生産技術エンジニア）
- MECE・図解・ステップバイステップを好む
- 作業はポートフォリオになるよう意識する（転職活動並行中）
- 作業完了後はNotionに記録を残す
- 「なぜそうするか」の理由を必ず添える
