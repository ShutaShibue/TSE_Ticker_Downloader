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
import tempfile
import re
from urllib.parse import urljoin

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
        # 証券コード列を文字列として読み込む（dtypeを使用）
        # これにより、131Aなどの文字列を含む証券コードが正しく読み込まれる
        df = pd.read_csv(csv_path, dtype=str)
        
        # 「市場・商品区分」列を探す
        market_column = None
        for col in df.columns:
            col_str = str(col).strip()
            if '市場・商品区分' in col_str or '市場' in col_str or '商品区分' in col_str or 'market' in col_str.lower():
                market_column = col
                break
        
        # PRO Marketの銘柄を除外
        if market_column is not None and market_column in df.columns:
            pro_market_mask = df[market_column].astype(str).str.contains('PRO Market', case=False, na=False)
            pro_market_count = pro_market_mask.sum()
            if pro_market_count > 0:
                df = df[~pro_market_mask]
                logger.info(f"CSVからPRO Marketの銘柄 {pro_market_count} 件を除外しました")
        
        # 'ticker' または 'code' または最初の列を使用
        # 文字列として扱う（アルファベットを含む可能性があるため）
        if 'ticker' in df.columns:
            tickers = df['ticker'].astype(str).str.strip().tolist()
        elif 'code' in df.columns:
            tickers = df['code'].astype(str).str.strip().tolist()
        else:
            tickers = df.iloc[:, 0].astype(str).str.strip().tolist()
        # 空文字列を除外
        tickers = [t for t in tickers if t]
        # 'nan'などの無効な値を除外
        tickers = [t for t in tickers if t.lower() not in ['nan', 'none', '']]
        return tickers
    except Exception as e:
        logger.error(f"CSV読み込みエラー: {str(e)}")
        return None


def get_tokyo_stock_list_from_tse(
    excel_url: Optional[str] = None,
    save_to_csv: bool = True,
    csv_path: str = "tickers.csv"
) -> Optional[List[str]]:
    """
    東証のウェブサイトから銘柄リストを取得
    https://www.jpx.co.jp/markets/statistics-equities/misc/01.html のExcelファイルから取得
    
    Args:
        excel_url: ExcelファイルのURL（Noneの場合は自動検出を試みる）
        save_to_csv: tickers.csvに保存するかどうか
        csv_path: 保存先のCSVファイルパス
    
    Returns:
        銘柄コードのリスト、取得失敗時はNone
    """
    try:
        # ExcelファイルのURL
        if excel_url is None:
            # 東証の銘柄一覧ページからExcelファイルのURLを取得
            base_url = "https://www.jpx.co.jp"
            list_page_url = "https://www.jpx.co.jp/markets/statistics-equities/misc/01.html"
            
            logger.info("東証の銘柄一覧ページからExcelファイルのURLを取得中...")
            response = requests.get(list_page_url, timeout=30)
            response.raise_for_status()
            
            # HTMLからExcelファイルのURLを抽出
            # パターン: data_j.xls または data_j.xlsx
            excel_pattern = r'href=["\']([^"\']*data[^"\']*\.xls[x]?[^"\']*)["\']'
            matches = re.findall(excel_pattern, response.text, re.IGNORECASE)
            
            if not matches:
                # 直接的なURLパターンを試す
                # 通常は /markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls のような形式
                excel_url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
                logger.info(f"デフォルトURLを使用: {excel_url}")
            else:
                excel_url = urljoin(base_url, matches[0])
                logger.info(f"ExcelファイルのURLを検出: {excel_url}")
        
        # Excelファイルをダウンロード
        logger.info("Excelファイルをダウンロード中...")
        response = requests.get(excel_url, timeout=30)
        response.raise_for_status()
        
        # 一時ファイルに保存
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xls') as tmp_file:
            tmp_file.write(response.content)
            tmp_file_path = tmp_file.name
        
        try:
            # Excelファイルを読み込む
            logger.info("Excelファイルを読み込み中...")
            # 複数のシートがある可能性があるため、最初のシートを読み込む
            # まず列名を確認するために1行だけ読み込む
            df_header = pd.read_excel(tmp_file_path, sheet_name=0, engine='xlrd', nrows=0)
            
            # 銘柄コード列を探す
            # 一般的な列名: 'コード', '銘柄コード', 'コード番号', 'Code' など
            code_column = None
            for col in df_header.columns:
                col_str = str(col).strip()
                if 'コード' in col_str or 'code' in col_str.lower() or col_str == 'Code':
                    code_column = col
                    break
            
            # コード列が見つからない場合、最初の列を試す
            if code_column is None:
                # 最初の列を使用（銘柄コードは文字列として扱う）
                code_column = df_header.columns[0]
                logger.warning(f"コード列が見つかりませんでした。最初の列 '{code_column}' を使用します。")
            
            # 証券コード列を文字列として読み込む（convertersを使用）
            # これにより、131Aなどの文字列を含む証券コードが正しく読み込まれる
            converters = {code_column: str}
            df = pd.read_excel(tmp_file_path, sheet_name=0, engine='xlrd', converters=converters)
            
            # 銘柄名列を探す（列名は「銘柄名」で固定）
            name_column = None
            for col in df.columns:
                col_str = str(col).strip()
                if col_str == '銘柄名':
                    name_column = col
                    break
            
            # 「銘柄名」が見つからない場合、類似の列名を探す（フォールバック）
            if name_column is None:
                for col in df.columns:
                    col_str = str(col).strip()
                    if '銘柄名' in col_str or '名称' in col_str or '会社名' in col_str or 'name' in col_str.lower():
                        name_column = col
                        logger.warning(f"「銘柄名」列が見つかりませんでした。「{col_str}」列を使用します。")
                        break
            
            # 「市場・商品区分」列を探す
            market_column = None
            for col in df.columns:
                col_str = str(col).strip()
                if '市場・商品区分' in col_str or '市場' in col_str or '商品区分' in col_str or 'market' in col_str.lower():
                    market_column = col
                    break
            
            # 銘柄コードと銘柄名を抽出
            ticker_data = []
            pro_market_count = 0
            for idx, row in df.iterrows():
                code_value = row[code_column]
                
                # 文字列として扱う（アルファベットを含む可能性があるため）
                if pd.isna(code_value):
                    continue
                
                code_str = str(code_value).strip()
                
                # 空文字列や無効な値をスキップ
                if not code_str or code_str.lower() in ['nan', 'none', '']:
                    continue
                
                # 市場・商品区分をチェック（PRO Marketの場合はスキップ）
                if market_column is not None:
                    market_value = row[market_column]
                    if pd.notna(market_value):
                        market_str = str(market_value).strip()
                        if 'PRO Market' in market_str or '[PRO Market]' in market_str:
                            pro_market_count += 1
                            continue
                
                # 数値として読み込まれた場合の処理（例: 131Aが131.0として読み込まれた場合）
                # ただし、文字列を含む証券コードはそのまま保持する
                # 小数点を含み、かつ小数点を除いた部分が数値のみの場合のみ処理
                if '.' in code_str:
                    # 小数点を除いた部分をチェック
                    parts = code_str.split('.')
                    if len(parts) == 2 and parts[0].isdigit() and parts[1].replace('0', '').replace('.', '') == '':
                        # 純粋な数値（例: 131.0）の場合のみ整数部分を使用
                        code_str = parts[0]
                    # それ以外（例: 131A.0など）はそのまま保持
                
                # 銘柄コードとして有効な値（空でない文字列）を抽出
                ticker_code = code_str
                
                # 銘柄名を取得
                if name_column is not None:
                    name_value = row[name_column]
                    ticker_name = str(name_value).strip() if pd.notna(name_value) else ""
                else:
                    ticker_name = ""
                
                ticker_data.append({
                    'ticker': ticker_code,
                    'name': ticker_name
                })
            
            if pro_market_count > 0:
                logger.info(f"PRO Marketの銘柄 {pro_market_count} 件を除外しました")
            
            if not ticker_data:
                logger.error("銘柄コードが抽出できませんでした。")
                return None
            
            # 重複を除去（tickerをキーに）
            seen = set()
            unique_ticker_data = []
            for item in ticker_data:
                if item['ticker'] not in seen:
                    seen.add(item['ticker'])
                    unique_ticker_data.append(item)
            
            # tickerでソート
            unique_ticker_data = sorted(unique_ticker_data, key=lambda x: x['ticker'])
            tickers = [item['ticker'] for item in unique_ticker_data]
            
            logger.info(f"東証から {len(tickers)} 銘柄を取得しました")
            
            # CSVファイルに保存
            if save_to_csv:
                ticker_df = pd.DataFrame(unique_ticker_data)
                ticker_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                logger.info(f"銘柄リストを {csv_path} に保存しました（銘柄名も含む）")
            
            return tickers
            
        finally:
            # 一時ファイルを削除
            try:
                os.unlink(tmp_file_path)
            except:
                pass
                
    except requests.RequestException as e:
        logger.error(f"Excelファイルのダウンロードエラー: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"東証からの銘柄リスト取得エラー: {str(e)}")
        return None


def get_tokyo_stock_list(use_jpx: bool = True) -> List[str]:
    """
    東証上場銘柄のリストを取得
    優先順位:
    1. tickers.csvファイルから読み込み
    2. 東証のウェブサイトから自動取得（use_jpx=Trueの場合）
    3. 全銘柄コード（1000-9999）を返す（実際には有効でない銘柄も含む）
    
    Args:
        use_jpx: 東証のウェブサイトから自動取得を試みるかどうか
    
    Returns:
        銘柄コードのリスト
    """
    # まずCSVファイルから読み込みを試みる
    tickers = get_tokyo_stock_list_from_csv()
    if tickers:
        logger.info(f"CSVファイルから {len(tickers)} 銘柄を読み込みました")
        return tickers
    
    # CSVファイルがない場合、東証のウェブサイトから取得を試みる
    if use_jpx:
        logger.info("tickers.csvが見つかりません。東証のウェブサイトから取得を試みます...")
        tickers = get_tokyo_stock_list_from_tse()
        if tickers:
            logger.info(f"東証から {len(tickers)} 銘柄を取得しました")
            return tickers
        else:
            logger.warning("東証からの取得に失敗しました。")
    
    # それでも取得できない場合、全銘柄コードを返す
    # 注意: 実際には全てのコードが有効な銘柄ではないため、
    # データ取得時にエラーになる銘柄はスキップされる
    logger.warning("tickers.csvが見つかりません。全銘柄コード（1000-9999）を試行します。")
    logger.warning("有効な銘柄のみを取得するには、tickers.csvファイルを用意するか、")
    logger.warning("東証のウェブサイトから自動取得を有効にしてください。")
    
    return [f"{i:04d}" for i in range(1000, 10000)]


def get_stock_data(ticker: str, start_date: str, end_date: Optional[str] = None, max_retries: int = 3) -> Optional[pd.DataFrame]:
    """
    指定された銘柄の日足データを取得
    
    Args:
        ticker: 銘柄コード（例: '7203'）
        start_date: 開始日（YYYY-MM-DD形式）
        end_date: 終了日（YYYY-MM-DD形式、Noneの場合は最新まで）
        max_retries: 最大リトライ回数（デフォルト: 3）
    
    Returns:
        DataFrame（日付, Open, High, Low, Close, Volume, Adjusted Close）
        取得失敗時はNone
    """
    # 日本株の場合は '.T' を付ける
    yahoo_ticker = f"{ticker}.T"
    
    for attempt in range(max_retries):
        try:
            stock = yf.Ticker(yahoo_ticker)
            hist = stock.history(start=start_date, end=end_date, auto_adjust=True)
            
            # histがNoneの場合のチェック
            if hist is None:
                if attempt < max_retries - 1:
                    logger.warning(f"データがNoneでした ({ticker}), リトライ中... ({attempt + 1}/{max_retries})")
                    time.sleep(2 * (attempt + 1))  # 指数バックオフ
                    continue
                else:
                    logger.warning(f"データが取得できませんでした（None）: {ticker}")
                    return None
            
            # 空のDataFrameの場合のチェック
            if hist.empty:
                # エラーメッセージから上場廃止の可能性をチェック
                # yfinanceはエラーメッセージをログに出力するが、例外を投げない場合がある
                logger.warning(f"データが取得できませんでした（空）: {ticker} - 上場廃止の可能性があります")
                return None
            
            # 必要な列が存在するかチェック
            required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            missing_columns = [col for col in required_columns if col not in hist.columns]
            if missing_columns:
                logger.warning(f"必要な列が存在しません ({ticker}): {missing_columns}")
                return None
            
            # 調整済み価格を使用（auto_adjust=Trueで自動的に調整済み価格が使用される）
            # 列名を整理
            df = hist[required_columns].copy()
            df.reset_index(inplace=True)
            df['Date'] = pd.to_datetime(df['Date']).dt.date
            
            return df
        
        except Exception as e:
            error_msg = str(e)
            # 上場廃止の可能性がある場合のチェック
            if 'possibly delisted' in error_msg.lower() or 'no price data found' in error_msg.lower():
                logger.warning(f"上場廃止の可能性があります ({ticker}): {error_msg}")
                return None
            
            # ネットワークエラーの場合はリトライ
            if ('curl' in error_msg.lower() or 'resolve host' in error_msg.lower() or 
                'connection' in error_msg.lower() or 'timeout' in error_msg.lower()):
                if attempt < max_retries - 1:
                    logger.warning(f"ネットワークエラー ({ticker}), リトライ中... ({attempt + 1}/{max_retries}): {error_msg}")
                    time.sleep(5 * (attempt + 1))  # ネットワークエラー時は長めに待機
                    continue
                else:
                    logger.error(f"ネットワークエラーが発生しました ({ticker}): {error_msg}")
                    return None
            else:
                # その他のエラーはリトライしない
                logger.error(f"エラーが発生しました ({ticker}): {error_msg}")
                return None
    
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
    delisted_count = 0  # 上場廃止の可能性がある銘柄数
    
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
            # 更新モードで既存データがある場合、上場廃止の可能性があるが既存データは保持
            if update_mode and existing_df is not None and not existing_df.empty:
                # 既存データをそのまま保持（上場廃止の可能性がある）
                delisted_count += 1
                logger.info(f"既存データを保持します（上場廃止の可能性）: {ticker}")
            else:
                # 初回取得でデータが取得できない場合はエラー
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
    
    logger.info(f"完了: 成功={success_count}, スキップ={skip_count}, エラー={error_count}, 上場廃止の可能性={delisted_count}")

