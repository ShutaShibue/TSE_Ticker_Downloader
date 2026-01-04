"""
日本株の日足データをYahoo Financeから取得するモジュール
"""
import os
import time
import pandas as pd
import yfinance as yf
from typing import List, Optional
from tqdm import tqdm
import logging
import requests
from pathlib import Path

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('download.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_tokyo_stock_list_from_csv(csv_path: str = "tickers.csv") -> Optional[List[str]]:
    """
    CSVファイルから銘柄リストを読み込む
    
    Args:
        csv_path: CSVファイルのパス（銘柄コードの列が必要）
    
    Returns:
        銘柄コードのリスト、ファイルが存在しない場合はNone
    """
    if not os.path.exists(csv_path):
        return None
    
    try:
        df = pd.read_csv(csv_path)
        # 'ticker' または 'code' または最初の列を使用
        if 'ticker' in df.columns:
            tickers = df['ticker'].astype(str).str.zfill(4).tolist()
        elif 'code' in df.columns:
            tickers = df['code'].astype(str).str.zfill(4).tolist()
        else:
            tickers = df.iloc[:, 0].astype(str).str.zfill(4).tolist()
        return tickers
    except Exception as e:
        logger.error(f"CSV読み込みエラー: {str(e)}")
        return None


def get_tokyo_stock_list_from_tse() -> Optional[List[str]]:
    """
    東証のウェブサイトから銘柄リストを取得
    注意: 東証のサイト構造が変更された場合、動作しない可能性があります
    
    Returns:
        銘柄コードのリスト、取得失敗時はNone
    """
    try:
        # 東証の銘柄リストを取得（実際のURLは変更される可能性がある）
        # ここでは簡易版として、主要な銘柄コードの範囲を返す
        # 実際の運用では、東証の公式APIやCSVダウンロード機能を使用することを推奨
        
        # 東証の銘柄コードは通常4桁（1000-9999）
        # 実際には全てのコードが有効な銘柄ではないため、
        # 有効な銘柄のみをフィルタリングする必要がある
        
        logger.warning("東証のウェブサイトからの自動取得は未実装です。")
        logger.warning("tickers.csvファイルを用意するか、全銘柄コードを試行します。")
        return None
    except Exception as e:
        logger.error(f"東証からの銘柄リスト取得エラー: {str(e)}")
        return None


def get_tokyo_stock_list() -> List[str]:
    """
    東証上場銘柄のリストを取得
    優先順位:
    1. tickers.csvファイルから読み込み
    2. 全銘柄コード（1000-9999）を返す（実際には有効でない銘柄も含む）
    
    Returns:
        銘柄コードのリスト
    """
    # まずCSVファイルから読み込みを試みる
    tickers = get_tokyo_stock_list_from_csv()
    if tickers:
        logger.info(f"CSVファイルから {len(tickers)} 銘柄を読み込みました")
        return tickers
    
    # CSVファイルがない場合、全銘柄コードを返す
    # 注意: 実際には全てのコードが有効な銘柄ではないため、
    # データ取得時にエラーになる銘柄はスキップされる
    logger.warning("tickers.csvが見つかりません。全銘柄コード（1000-9999）を試行します。")
    logger.warning("有効な銘柄のみを取得するには、tickers.csvファイルを用意してください。")
    
    return [f"{i:04d}" for i in range(1000, 10000)]


def get_stock_data(ticker: str, start_date: str, end_date: Optional[str] = None) -> Optional[pd.DataFrame]:
    """
    指定された銘柄の日足データを取得
    
    Args:
        ticker: 銘柄コード（例: '7203'）
        start_date: 開始日（YYYY-MM-DD形式）
        end_date: 終了日（YYYY-MM-DD形式、Noneの場合は最新まで）
    
    Returns:
        DataFrame（日付, Open, High, Low, Close, Volume, Adjusted Close）
        取得失敗時はNone
    """
    # 日本株の場合は '.T' を付ける
    yahoo_ticker = f"{ticker}.T"
    
    try:
        stock = yf.Ticker(yahoo_ticker)
        hist = stock.history(start=start_date, end=end_date, auto_adjust=True)
        
        if hist.empty:
            logger.warning(f"データが取得できませんでした: {ticker}")
            return None
        
        # 調整済み価格を使用（auto_adjust=Trueで自動的に調整済み価格が使用される）
        # 列名を整理
        df = hist[['Open', 'High', 'Low', 'Close', 'Volume']].copy()
        df.reset_index(inplace=True)
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        
        return df
    
    except Exception as e:
        logger.error(f"エラーが発生しました ({ticker}): {str(e)}")
        return None


def save_to_csv(df: pd.DataFrame, ticker: str, output_dir: str = "data") -> bool:
    """
    DataFrameをCSVファイルに保存
    
    Args:
        df: 保存するDataFrame
        ticker: 銘柄コード
        output_dir: 出力ディレクトリ
    
    Returns:
        成功時True、失敗時False
    """
    try:
        os.makedirs(output_dir, exist_ok=True)
        filepath = os.path.join(output_dir, f"{ticker}.csv")
        df.to_csv(filepath, index=False)
        return True
    except Exception as e:
        logger.error(f"CSV保存エラー ({ticker}): {str(e)}")
        return False


def load_existing_data(ticker: str, data_dir: str = "data") -> Optional[pd.DataFrame]:
    """
    既存のCSVデータを読み込む
    
    Args:
        ticker: 銘柄コード
        data_dir: データディレクトリ
    
    Returns:
        DataFrame、ファイルが存在しない場合はNone
    """
    filepath = os.path.join(data_dir, f"{ticker}.csv")
    if not os.path.exists(filepath):
        return None
    
    try:
        df = pd.read_csv(filepath)
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        return df
    except Exception as e:
        logger.error(f"データ読み込みエラー ({ticker}): {str(e)}")
        return None


def merge_data(existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    """
    既存データと新規データをマージ（重複除去）
    
    Args:
        existing_df: 既存のDataFrame
        new_df: 新規のDataFrame
    
    Returns:
        マージされたDataFrame
    """
    # 日付をキーにしてマージ
    combined = pd.concat([existing_df, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=['Date'], keep='last')
    combined = combined.sort_values('Date').reset_index(drop=True)
    return combined


def download_all_stocks(
    start_date: str,
    end_date: Optional[str] = None,
    data_dir: str = "data",
    delay: float = 1.0,
    update_mode: bool = False
) -> None:
    """
    すべての日本株の日足データを取得
    
    Args:
        start_date: 開始日（YYYY-MM-DD形式）
        end_date: 終了日（YYYY-MM-DD形式、Noneの場合は最新まで）
        data_dir: データ保存ディレクトリ
        delay: API呼び出し間隔（秒）
        update_mode: 更新モード（Trueの場合、既存データの最新日以降のみ取得）
    """
    tickers = get_tokyo_stock_list()
    logger.info(f"取得対象銘柄数: {len(tickers)}")
    
    success_count = 0
    skip_count = 0
    error_count = 0
    
    for ticker in tqdm(tickers, desc="データ取得中"):
        existing_df = None
        
        # 更新モードの場合、既存データの最新日を確認
        if update_mode:
            existing_df = load_existing_data(ticker, data_dir)
            if existing_df is not None and not existing_df.empty:
                last_date = existing_df['Date'].max()
                # 最新日の翌日から取得
                if isinstance(last_date, str):
                    last_date = pd.to_datetime(last_date).date()
                elif not isinstance(last_date, pd.Timestamp):
                    last_date = pd.to_datetime(last_date).date()
                
                # 最新日の翌日を計算
                next_date = pd.to_datetime(last_date) + pd.Timedelta(days=1)
                start_date_for_ticker = next_date.strftime('%Y-%m-%d')
                
                # 最新データが既にある場合はスキップ
                today = pd.to_datetime('today').normalize()
                if next_date > today:
                    skip_count += 1
                    continue
            else:
                # 既存データがない場合は初回取得として扱う
                start_date_for_ticker = start_date
        else:
            start_date_for_ticker = start_date
        
        # データ取得
        df = get_stock_data(ticker, start_date_for_ticker, end_date)
        
        if df is None or df.empty:
            error_count += 1
            time.sleep(delay)
            continue
        
        # 更新モードの場合、既存データとマージ
        if update_mode and existing_df is not None and not existing_df.empty:
            df = merge_data(existing_df, df)
        
        # CSV保存
        if save_to_csv(df, ticker, data_dir):
            success_count += 1
        else:
            error_count += 1
        
        # API呼び出し間隔を空ける
        time.sleep(delay)
    
    logger.info(f"完了: 成功={success_count}, スキップ={skip_count}, エラー={error_count}")

