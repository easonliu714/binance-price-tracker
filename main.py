import os
import json
import time
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import telegram
import gc

# 設定日誌
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 環境變數檢查與獲取
def get_env_var(var_name, fallback=None, required=False):
    value = os.environ.get(var_name, fallback)
    if required and not value:
        logger.error(f"缺少必要的環境變數: {var_name}")
        raise EnvironmentError(f"缺少必要的環境變數: {var_name}")
    if not value and not fallback:
        logger.warning(f"環境變數 {var_name} 未設定")
    return value

# 環境變數
TELEGRAM_TOKEN = get_env_var('TELEGRAM_TOKEN', required=True)
TELEGRAM_CHAT_ID = get_env_var('TELEGRAM_CHAT_ID', required=True)
GOOGLE_SHEET_CREDS_JSON = get_env_var('GOOGLE_SHEET_CREDS_JSON', required=True)
SPREADSHEET_ID = get_env_var('SPREADSHEET_ID', "1Bny_4th50YM2mKSTZDbH7Zqd9Uhl6PHMCCveFMgqMrE")
SHEET_NAME = get_env_var('SHEET_NAME', "Sheet1")

# 執行時間限制 (9分鐘，考慮到 Cloud Run 最大允許 10 分鐘)
MAX_EXECUTION_TIME = 540  

# 幣安 API 端點
BINANCE_API_BASE = "https://api.binance.com/api"

def get_usdt_trading_pairs():
    """獲取所有與 USDT 配對的交易對"""
    logger.info("開始獲取 USDT 交易對...")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
        
    try:
        response = requests.get(f"{BINANCE_API_BASE}/v3/exchangeInfo", timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # 篩選出以 USDT 結尾的交易對
        usdt_pairs = [symbol['symbol'] for symbol in data['symbols'] 
                      if symbol['symbol'].endswith('USDT') and symbol['status'] == 'TRADING']
        
        logger.info(f"成功獲取 {len(usdt_pairs)} 個 USDT 交易對")
        return usdt_pairs
    except Exception as e:
        logger.error(f"獲取交易對時發生錯誤: {str(e)}")
        return []

def get_klines(symbol, interval="1h", limit=500):
    """獲取特定交易對的 K 線數據"""
    logger.info(f"獲取 {symbol} 的 {interval} K線數據...")
    
    try:
        response = requests.get(
            f"{BINANCE_API_BASE}/v3/klines",
            params={
                "symbol": symbol,
                "interval": interval,
                "limit": limit
            },
            timeout=30
        )
        response.raise_for_status()
        klines = response.json()
        
        # 將 K 線數據轉換為 DataFrame
        df = pd.DataFrame(klines, columns=[
            'open_time', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
        ])
        
        # 轉換數據類型
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
        df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
        df['close'] = df['close'].astype(float)
        
        logger.info(f"成功獲取 {symbol} 的 {len(df)} 筆 K線數據")
        return df
    except Exception as e:
        logger.error(f"獲取 {symbol} K線數據時發生錯誤: {str(e)}")
        return pd.DataFrame()

def calculate_moving_averages(df):
    """計算移動平均線"""
    logger.info("計算移動平均線...")
    
    try:
        # 確保有足夠的數據計算 MA
        if len(df) < 425:
            logger.warning(f"數據不足以計算 MA425，當前僅有 {len(df)} 筆數據")
            return df
        
        # 計算 MA175 和 MA425
        df['MA175'] = df['close'].rolling(window=175).mean()
        df['MA425'] = df['close'].rolling(window=425).mean()
        
        logger.info("移動平均線計算完成")
        return df
    except Exception as e:
        logger.error(f"計算移動平均線時發生錯誤: {str(e)}")
        return df

def check_signals(symbol, df):
    """檢查交易信號"""
    logger.info(f"檢查 {symbol} 的交易信號...")
    
    signals = []
    
    try:
        # 確保有足夠的數據
        if len(df) < 4 or df['MA175'].isna().any() or df['MA425'].isna().any():
            logger.warning(f"{symbol} 數據不足以檢查信號")
            return signals
        
        # 獲取最近 4 小時的數據（當前小時 + 前 3 小時）
        recent_data = df.iloc[-4:].copy()
        recent_data = recent_data.reset_index(drop=True)
        
        current_price = recent_data.iloc[-1]['close']
        current_ma175 = recent_data.iloc[-1]['MA175']
        current_ma425 = recent_data.iloc[-1]['MA425']
        
        # 檢查 MA425 上漲信號
        if (current_price > current_ma425 and 
            all(recent_data.iloc[i]['close'] < recent_data.iloc[i]['MA425'] for i in range(3)) and
            all(recent_data.iloc[i]['close'] < current_price for i in range(3))):
            
            logger.info(f"{symbol} 發出 MA425 上漲信號")
            signals.append({
                'symbol': symbol,
                'type': 'MA425上漲',
                'price': current_price,
                'ma': current_ma425
            })
        
        # 檢查 MA425 下跌信號
        if (current_price < current_ma425 and 
            all(recent_data.iloc[i]['close'] > recent_data.iloc[i]['MA425'] for i in range(3)) and
            all(recent_data.iloc[i]['close'] > current_price for i in range(3))):
            
            logger.info(f"{symbol} 發出 MA425 下跌信號")
            signals.append({
                'symbol': symbol,
                'type': 'MA425下跌',
                'price': current_price,
                'ma': current_ma425
            })
        
        # 檢查 MA175 上漲信號
        if (current_price > current_ma175 and 
            all(recent_data.iloc[i]['close'] < recent_data.iloc[i]['MA175'] for i in range(3)) and
            all(recent_data.iloc[i]['close'] < current_price for i in range(3))):
            
            logger.info(f"{symbol} 發出 MA175 上漲信號")
            signals.append({
                'symbol': symbol,
                'type': 'MA175上漲',
                'price': current_price,
                'ma': current_ma175
            })
        
        # 檢查 MA175 下跌信號
        if (current_price < current_ma175 and 
            all(recent_data.iloc[i]['close'] > recent_data.iloc[i]['MA175'] for i in range(3)) and
            all(recent_data.iloc[i]['close'] > current_price for i in range(3))):
            
            logger.info(f"{symbol} 發出 MA175 下跌信號")
            signals.append({
                'symbol': symbol,
                'type': 'MA175下跌',
                'price': current_price,
                'ma': current_ma175
            })
        
        return signals
    except Exception as e:
        logger.error(f"檢查 {symbol} 交易信號時發生錯誤: {str(e)}")
        return signals

def send_telegram_message(message):
    """發送 Telegram 訊息"""
    logger.info(f"發送 Telegram 訊息: {message}")
    
    try:
        bot = telegram.Bot(token=TELEGRAM_TOKEN)
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode='Markdown')
        logger.info("Telegram 訊息發送成功")
        return True
    except Exception as e:
        logger.error(f"發送 Telegram 訊息時發生錯誤: {str(e)}")
        return False

def get_google_sheets_service():
    """獲取 Google Sheets API 服務"""
    logger.info("初始化 Google Sheets API 服務...")
    
    try:
        # 從環境變數獲取憑證
        creds_json = json.loads(GOOGLE_SHEET_CREDS_JSON)
        creds = service_account.Credentials.from_service_account_info(
            creds_json, scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        service = build('sheets', 'v4', credentials=creds)
        logger.info("Google Sheets API 服務初始化成功")
        return service
    except Exception as e:
        logger.error(f"初始化 Google Sheets API 服務時發生錯誤: {str(e)}")
        return None

def update_google_sheet(signals):
    """更新 Google Sheet 數據"""
    logger.info(f"更新 Google Sheet 共 {len(signals)} 筆信號")
    
    if not signals:
        logger.info("沒有信號需要更新到 Google Sheet")
        return
    
    try:
        service = get_google_sheets_service()
        if not service:
            return
        
        # 獲取當前時間（台北時區）
        taipei_tz = pytz.timezone('Asia/Taipei')
        current_time = datetime.now(taipei_tz).strftime('%Y-%m-%d %H:%M:%S')
        
        # 準備要寫入的數據
        rows = []
        for signal in signals:
            rows.append([
                signal['symbol'],
                signal['type'],
                str(signal['price']),
                str(signal['ma']),
                current_time
            ])
        
        # 先檢查現有數據的行數
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A:E"
        ).execute()
        
        existing_values = result.get('values', [])
        existing_count = len(existing_values)
        
        logger.info(f"當前 Google Sheet 有 {existing_count} 筆數據")
        
        # 添加新數據
        service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A:E",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": rows}
        ).execute()
        
        # 如果數據超過 10000 筆，刪除最舊的數據
        new_count = existing_count + len(rows)
        if new_count > 10000:
            rows_to_delete = new_count - 10000
            logger.info(f"數據超過 10000 筆，刪除最舊的 {rows_to_delete} 筆數據")
            
            # 刪除最舊的數據（從第二行開始，保留標題行）
            requests = [{
                "deleteDimension": {
                    "range": {
                        "sheetId": 0,  # 假設是第一個工作表
                        "dimension": "ROWS",
                        "startIndex": 1,  # 從第二行開始（保留標題行）
                        "endIndex": 1 + rows_to_delete
                    }
                }
            }]
            
            service.spreadsheets().batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body={"requests": requests}
            ).execute()
        
        logger.info("Google Sheet 更新成功")
    except Exception as e:
        logger.error(f"更新 Google Sheet 時發生錯誤: {str(e)}")

def main(request=None):
    """主函數，由 Cloud Run 或 Cloud Scheduler 觸發"""
    logger.info("開始執行幣安價格追蹤...")
    
    start_time = time.time()
    all_signals = []
    
    try:
        # 獲取交易對
        trading_pairs = get_usdt_trading_pairs()
        
        # 限制處理的交易對數量，避免超時（可根據需要調整）
        trading_pairs = trading_pairs[:50]  # 先處理前 50 個交易對
        
        for symbol in trading_pairs:
            try:
                # 檢查是否接近超時
                current_time = time.time()
                if current_time - start_time > MAX_EXECUTION_TIME:
                    logger.warning(f"接近超時限制，停止處理更多交易對，已處理 {len(all_signals)} 個信號")
                    break
                
                # 獲取 K 線數據
                df = get_klines(symbol, interval="1h", limit=500)
                
                if df.empty:
                    continue
                
                # 計算移動平均線
                df = calculate_moving_averages(df)
                
                # 檢查信號
                signals = check_signals(symbol, df)
                all_signals.extend(signals)
                
                # 發送 Telegram 訊息
                for signal in signals:
                    # 獲取台北時間
                    taipei_tz = pytz.timezone('Asia/Taipei')
                    taipei_time = datetime.now(taipei_tz).strftime('%Y-%m-%d %H:%M:%S')
                    
                    message = (
                        f"*{signal['symbol']}* {signal['type']}\n"
                        f"現價: {signal['price']:.8f}\n"
                        f"MA值: {signal['ma']:.8f}\n"
                        f"時間: {taipei_time}"
                    )
                    send_telegram_message(message)
                
                # 清理記憶體
                del df
                gc.collect()
                
            except Exception as e:
                logger.error(f"處理 {symbol} 時發生錯誤: {str(e)}")
                continue
        
        # 更新 Google Sheet
        update_google_sheet(all_signals)
        
        execution_time = time.time() - start_time
        logger.info(f"執行完成，共發現 {len(all_signals)} ")
        logger.info(f"執行時間: {execution_time:.2f} 秒")
        
        return f"執行完成，共發現 {len(all_signals)} 個訊號"
    except Exception as e:
        logger.error(f"執行主函數時發生錯誤: {str(e)}")
        return f"執行錯誤: {str(e)}"
