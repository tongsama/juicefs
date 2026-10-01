# VM I/O 調査・修正 Implementation Plan

> **For agentic workers:** 調査はsub-workerに分担し、修正は主担当が順番に実施する。

**Goal:** Readのflushエラー無視を回帰テストで修正し、メタデータ滞留の段階を実機で識別できるようにする。

**Architecture:** 初期調査ではロック順序・compaction・flush期限・writeback耐久性を維持し、Readのflushエラー伝播と段階計測を追加した。実機障害を特定した後、ユーザー承認によりflushの既定期限を無期限へ変更した。実保存失敗は伝播し、未完了成功を禁止する。ロック順序・compaction・writeback耐久性は引き続き維持する。

**Tech Stack:** Go / JuiceFS CE v1.4.1 / memkv回帰テスト。

**Spec:** ../../../../juicefs_vm_corruption_handoff_codex.md（リポジトリの親に配置）。

## Global Constraints

- 日本語で連絡。GitHubのpush/リモート変更前に確認。
- 初期状態はHEAD febf149a、未コミット差分なし。既存slice timer変更は保持。
- 初期調査のロック/WAN仮説は未確定だった。実機ログで、同期削除に滞留する背景compactionを同期Writeが待ち、5分EIOになる経路を特定した。
- >=2500同期compactionはMeta.Write内部でbackend commit後も停止し得る。682という後刻の1chunk値は過去や他chunkの閾値超過を排除しない。
- 本番データへ接続・変更しない。初期調査は未コミットで進めたが、2026-10-01にユーザーが派生ブランチへのローカルコミットを承認。pushは禁止。

## Task 1: Readエラーの回帰修正

Files: pkg/vfs/vfs_test.go, pkg/vfs/vfs.go。

- [x] memkv/object-memoryの実VFSで古いデータを書きfsyncする。Meta.Writeだけ失敗させるwrapperをdataWriterに注入し、新しい書き込み後にReadする。
- [x] Readが古いデータを成功として返す失敗を確認する。EIO/ENOSPC/EDQUOT/ENOENT、出力bufferとhandleのop cleanupを検証する。
- [x] ReadのFlush errnoをチェックし、失敗時はreaderを呼ばずreturnする。removeOpはdeferし早期returnでも解除する。
- [x] 回帰テストと既存VFSテストを実行する。

## Task 2: 滞留段階の診断

Files: pkg/meta/base.go, pkg/meta/redis.go, pkg/vfs/writer.go, docs内の調査記録。

- [x] baseMeta.Writeのinode lock待ち、doWrite、stat更新、同期compaction時間をslow WARNで記録する。debug開始/段階ログで戻らないcallも識別する。
- [x] Redis txnの内部lock待ちと実行時間を分離し、doWriteをネットワーク時間と誤認しないようにする。
- [x] commitThreadのMeta.Write時間を計測する。制御フローやデータ耐久性は変更しない。
- [x] gofmt、git diff --check、meta core / pkg指定チェック、ビルドを実施し、環境依存の制約を記録する。
- [x] agent_memo.mdに継続方針、確定事実、未確定仮説、検証結果を保存する。

## 検証の制約

Makefile指定チェックは実行済みだが全成功ではない。test.pkgはGlusterFSライブラリ不足、test.meta.coreはTestLoadDumpで未配置TiKVに接続して停止。追加のchunkリンクエラーとFUSE FstatDeleted属性不一致はunchanged HEADでも再現。必要範囲のVFS/FS、Redis/SQLite/MemKV、quota、cancel検査と通常ビルドを別途確認する。実機の5分EIO根因は未確定のため、本変更を根因修正と扱わない。

## 完了した追加修正

- [x] --writer-flush-timeoutを追加。既定0sで無期限、autoで旧期限、正durationで明示期限。既定変更はユーザー承認済み。
- [x] 実writer errorは別chunkの処理待ちに隠さず伝播。未完了成功は禁止。
- [x] 削除queue飽和、背景/同期compaction待ち、queue排出後のWrite完了を回帰テストで検証。
- [x] 負のmax-deletesが無制限並列ではなく同期削除であることをWARN・CLI help・docsへ明記。
- [x] normal/writeback両モードで旧5分期限を実時間で越える待機テストと、対象race検査を実施。
- [x] PostgreSQLのSQL共通/固有経路をソース確認。ユーザー指示どおりPostgreSQL実テストは未実施。

小さいPUTとslice timerの後続調査は、リポジトリの親にあるTODO.mdで管理する。
