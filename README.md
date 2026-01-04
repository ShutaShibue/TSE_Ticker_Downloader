# Daily Bar Downloader

日本株の日足データをYahoo Financeから取得し、CSVファイルに保存するツールです。

## 機能

- すべての日本株の日足データ（2016年〜最新）を取得
- 分割等修正済みの調整済み価格を使用
- 後日最新データをアップデート可能
- 日付、OHLCV（始値、高値、安値、終値、出来高）を保存

## インストール

```bash
pip install -r requirements.txt
```

## 使用方法

### 銘柄リストの準備

**自動取得（推奨）**:
プログラムは自動的に[東証の上場銘柄一覧](https://www.jpx.co.jp/markets/statistics-equities/misc/01.html)から最新の銘柄リストを取得し、`tickers.csv`に保存します。
銘柄コードと銘柄名の両方が保存されます。

**手動準備**:
`tickers.csv`ファイルを手動で用意することもできます。以下の形式で作成してください：

```csv
ticker,name
7203,トヨタ自動車
6758,ソニーグループ
9984,ソフトバンクグループ
...
```

`name`列は省略可能です。`ticker`列のみでも動作します。

`tickers.csv`がない場合、プログラムは自動的に東証のウェブサイトから取得を試みます。
それでも取得できない場合、1000-9999の全銘柄コードを試行しますが、
有効でない銘柄も含まれるため、エラーが多くなります。

### 初回データ取得

```bash
python download_stocks.py --start-date 2016-01-01
```

オプション：

- `--start-date`: 開始日（YYYY-MM-DD形式、デフォルト: 2016-01-01）
- `--end-date`: 終了日（YYYY-MM-DD形式、デフォルト: 最新まで）
- `--data-dir`: データ保存ディレクトリ（デフォルト: data）
- `--delay`: API呼び出し間隔（秒、デフォルト: 1.0）

例：

```bash
python download_stocks.py --start-date 2016-01-01 --data-dir data --delay 1.5
```

### データ更新

既存データの最新日以降のみを取得します：

```bash
python download_stocks.py --update
```

更新モードでは：

- 既存データがある銘柄は、最新日の翌日から取得
- 既存データがない銘柄は、初回取得として扱う
- 既存データと新規データを自動的にマージ（重複除去）

## データ保存形式

- 保存先: `data/` ディレクトリ（`--data-dir`で変更可能）
- ファイル名: `{ticker}.csv` (例: `7203.csv`)
- 形式: CSV（Date, Open, High, Low, Close, Volume）
- 日付形式: YYYY-MM-DD

## ログ

実行ログは`download.log`に保存されます。

## 注意事項

- Yahoo Finance APIのレート制限に注意してください
- 大量の銘柄を取得する場合、時間がかかります
- 有効でない銘柄コードは自動的にスキップされます
- ネットワークエラーが発生した場合、該当銘柄はスキップされます
