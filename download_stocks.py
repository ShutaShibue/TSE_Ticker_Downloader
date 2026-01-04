"""
日本株の日足データをダウンロードするメインスクリプト
"""
import argparse
from datetime import datetime
from stock_downloader import download_all_stocks


def main():
    parser = argparse.ArgumentParser(
        description='日本株の日足データをYahoo Financeから取得'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        default='2016-01-01',
        help='開始日（YYYY-MM-DD形式、デフォルト: 2016-01-01）'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        default=None,
        help='終了日（YYYY-MM-DD形式、デフォルト: 最新まで）'
    )
    parser.add_argument(
        '--update',
        action='store_true',
        help='更新モード：既存データの最新日以降のみ取得'
    )
    parser.add_argument(
        '--data-dir',
        type=str,
        default='data',
        help='データ保存ディレクトリ（デフォルト: data）'
    )
    parser.add_argument(
        '--delay',
        type=float,
        default=1.0,
        help='API呼び出し間隔（秒、デフォルト: 1.0）'
    )
    
    args = parser.parse_args()
    
    # 日付の妥当性チェック
    try:
        datetime.strptime(args.start_date, '%Y-%m-%d')
        if args.end_date:
            datetime.strptime(args.end_date, '%Y-%m-%d')
    except ValueError:
        print("エラー: 日付は YYYY-MM-DD 形式で指定してください")
        return
    
    print(f"開始日: {args.start_date}")
    if args.end_date:
        print(f"終了日: {args.end_date}")
    else:
        print("終了日: 最新まで")
    print(f"モード: {'更新' if args.update else '初回取得'}")
    print(f"データ保存先: {args.data_dir}")
    print()
    
    download_all_stocks(
        start_date=args.start_date,
        end_date=args.end_date,
        data_dir=args.data_dir,
        delay=args.delay,
        update_mode=args.update
    )


if __name__ == '__main__':
    main()

