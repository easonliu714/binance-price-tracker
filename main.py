import os
import time
import json
import logging
from datetime import datetime, timezone, timedelta
import pytz
import schedule
from flask import Flask
from threading import Thread
from binance_api import get_trading_pairs, get_klines
from calculator import calculate_price_indicators, calculate_volume_indicators
from notification import send_telegram_message
from sheet_handler import update_sheet, cleanup_old_data, setup_sheet_client
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('app.log')
    ]
)
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
GOOGLE_SHEET_CREDS_JSON = os.environ.get('GOOGLE_SHEET_CREDS_JSON')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '1Bny_4th50YM2mKSTZDbH7Zqd9Uhl6PHMCCveFMgqMrE')
SHEET_NAME = os.environ.get('SHEET_NAME', 'Sheet1')

app = Flask(__name__)

if not all([TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, GOOGLE_SHEET_CREDS_JSON, SPREADSHEET_ID, SHEET_NAME]):
    logger.error("環境變數設定不完整！請檢查 TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, GOOGLE_SHEET_CREDS_JSON, SPREADSHEET_ID, SHEET_NAME")
    error_msg = "環境變數設定不完整"
    send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, f"啟動失敗: {error_msg}")
    raise ValueError(error_msg)

def get_taipei_time():
    try:
        taipei_tz = pytz.timezone('Asia/Taipei')
        return datetime.now(taipei_tz).strftime("%Y-%m-%d %H:%M:%S")
    except Exception as e:
        logger.error(f"獲取台北時間失敗: {e}")
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def check_signals(trading_pair, price_data, volume_data, current_price):
    signals = []
    signal_types = []
    taipei_time = get_taipei_time()
    try:
        current_volume = volume_data["volume"][-1]
        if current_volume < 10000:
            logger.info(f"{trading_pair} 當前成交量 {current_volume} 小於10000，跳過訊號檢查")
            return signals, signal_types
        logger.info(f"{trading_pair} 當前成交量 {current_volume} 大於10000，開始檢查訊號")
        ma17 = price_data["MA17"][-1]
        ma175 = price_data["MA175"][-1]
        ma425 = price_data["MA425"][-1]
        vol7 = volume_data["VOL7"][-1]
        vol17 = volume_data["VOL17"][-1]
        recent_closes = price_data["close"][-4:-1]
        if (current_price > ma425 and
                all(close < ma425 for close in recent_closes) and
                all(current_price > close for close in recent_closes)):
            signal_msg = f"[{trading_pair}] MA425上漲，[{current_price}]大於[{ma425:.8f}]，[{taipei_time}]"
            signals.append(signal_msg)
            signal_types.append("MA425上漲")
            logger.info(f"觸發MA425上漲訊號: {signal_msg}")
        if (current_price < ma425 and
                all(close > ma425 for close in recent_closes) and
                all(current_price < close for close in recent_closes)):
            signal_msg = f"[{trading_pair}] MA425下跌，[{current_price}]小於[{ma425:.8f}]，[{taipei_time}]"
            signals.append(signal_msg)
            signal_types.append("MA425下跌")
            logger.info(f"觸發MA425下跌訊號: {signal_msg}")
        if (current_price > ma175 and
                all(close < ma175 for close in recent_closes) and
                all(current_price > close for close in recent_closes)):
            signal_msg = f"[{trading_pair}] MA175上漲，[{current_price}]大於[{ma175:.8f}]，[{taipei_time}]"
            signals.append(signal_msg)
            signal_types.append("MA175上漲")
            logger.info(f"觸發MA175上漲訊號: {signal_msg}")
        if (current_price < ma175 and
                all(close > ma175 for close in recent_closes) and
                all(current_price < close for close in recent_closes)):
            signal_msg = f"[{trading_pair}] MA175下跌，[{current_price}]小於[{ma425:.8f}]，[{taipei_time}]"
            signals.append(signal_msg)
            signal_types.append("MA175下跌")
            logger.info(f"觸發MA175下跌訊號: {signal_msg}")
        if current_volume > vol17 * 3 and vol7 > vol17:
            signal_msg = f"[{trading_pair}] 交易量3倍增，[{current_price}]，[{taipei_time}]"
            signals.append(signal_msg)
            signal_types.append("交易量3倍增")
            logger.info(f"觸發交易量增加訊號: {signal_msg}")
    except Exception as e:
        logger.error(f"檢查 {trading_pair} 訊號時發生錯誤: {e}")
        send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, f"檢查 {trading_pair} 訊號失敗: {e}")
    return signals, signal_types

def process_trading_pair(trading_pair, sheet_client):
    try:
        logger.info(f"開始處理交易對: {trading_pair}")
        klines_5m = get_klines(trading_pair, interval="5m", limit=500)
        if not klines_5m:
            logger.warning(f"無法獲取 {trading_pair} 的5分鐘K線數據")
            send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, f"無法獲取 {trading_pair} 的5分鐘K線數據")
            return
        logger.info(f"成功獲取 {trading_pair} 的5分鐘K線數據，共 {len(klines_5m)} 根")
        klines_1h = get_klines(trading_pair, interval="1h", limit=500)
        if not klines_1h:
            logger.warning(f"無法獲取 {trading_pair} 的1小時K線數據")
            send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, f"無法獲取 {trading_pair} 的1小時K線數據")
            return
        logger.info(f"成功獲取 {trading_pair} 的1小時K線數據，共 {len(klines_1h)} 根")
        price_indicators = calculate_price_indicators(klines_1h)
        if not price_indicators:
            logger.warning(f"無法計算 {trading_pair} 的價格指標")
            send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, f"無法計算 {trading_pair} 的價格指標")
            return
        volume_indicators = calculate_volume_indicators(klines_5m)
        if not volume_indicators:
            logger.warning(f"無法計算 {trading_pair} 的交易量指標")
            send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, f"無法計算 {trading_pair} 的交易量指標")
            return
        logger.info(f"成功計算 {trading_pair} 的指標")
        current_price = float(klines_5m[-1][4])
        signals, signal_types = check_signals(trading_pair, price_indicators, volume_indicators, current_price)
        if signals:
            for signal in signals:
                send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, signal)
            taipei_time = get_taipei_time()
            row_data = [
                taipei_time,
                trading_pair,
                current_price,
                price_indicators["MA17"][-1],
                price_indicators["MA175"][-1],
                price_indicators["MA425"][-1],
                volume_indicators["volume"][-1],
                volume_indicators["VOL7"][-1],
                volume_indicators["VOL17"][-1],
                ", ".join(signal_types)
            ]
            update_sheet(sheet_client, SPREADSHEET_ID, SHEET_NAME, row_data)
            cleanup_old_data(sheet_client, SPREADSHEET_ID, SHEET_NAME, max_rows=10000)
            logger.info(f"完成處理交易對: {trading_pair}，共觸發 {len(signals)} 個訊號")
        else:
            logger.info(f"完成處理交易對: {trading_pair}，無訊號觸發")
    except Exception as e:
        logger.error(f"處理交易對 {trading_pair} 時發生錯誤: {e}")
        send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, f"處理 {trading_pair} 失敗: {e}")

def main_task():
    try:
        logger.info("開始執行主要任務")
        logger.info(f"TELEGRAM_TOKEN: {TELEGRAM_TOKEN[:10]}... (隱藏後續字符)")
        logger.info(f"TELEGRAM_CHAT_ID: {TELEGRAM_CHAT_ID}")
        logger.info(f"GOOGLE_SHEET_CREDS_JSON: {GOOGLE_SHEET_CREDS_JSON[:50]}...")
        logger.info(f"SPREADSHEET_ID: {SPREADSHEET_ID}")
        logger.info(f"SHEET_NAME: {SHEET_NAME}")
        logger.info("設置 Google Sheet 客戶端")
        sheet_client = setup_sheet_client(GOOGLE_SHEET_CREDS_JSON)
        if not sheet_client:
            logger.error("無法設置 Google Sheet 客戶端")
            send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, "無法設置 Google Sheet 客戶端")
            return
        logger.info("Google Sheet 客戶端設置成功")
        trading_pairs = get_trading_pairs()
        if not trading_pairs:
            logger.error("無法獲取交易對列表")
            send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, "無法獲取交易對列表")
            return
        logger.info(f"成功獲取 {len(trading_pairs)} 個 USDT 永續合約交易對")
        requests_per_minute = 1200
        delay = 60.0 / requests_per_minute
        processed_pairs = 0
        failed_pairs = 0
        for trading_pair in trading_pairs:
            try:
                process_trading_pair(trading_pair, sheet_client)
                processed_pairs += 1
            except Exception as e:
                logger.error(f"處理 {trading_pair} 時發生錯誤: {e}")
                send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, f"處理 {trading_pair} 失敗: {e}")
                failed_pairs += 1
            time.sleep(delay)
        logger.info(f"主要任務執行完成，成功處理 {processed_pairs} 個交易對，失敗 {failed_pairs} 個")
        if failed_pairs > 0:
            send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, f"任務完成，成功 {processed_pairs} 個交易對，失敗 {failed_pairs} 個")
    except Exception as e:
        logger.error(f"執行主要任務時發生錯誤: {e}")
        send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, f"主要任務失敗: {e}")

def run_scheduler_in_thread():
    def run_scheduler():
        logger.info("啟動定時排程")
        schedule.every(5).minutes.do(main_task)
        while True:
            schedule.run_pending()
            time.sleep(1)
    thread = Thread(target=run_scheduler)
    thread.daemon = True
    thread.start()

@app.route('/', methods=['GET'])
def index():
    try:
        logger.info("接收到 HTTP 請求，執行主要任務")
        main_task()
        return "Task completed successfully", 200
    except Exception as e:
        logger.error(f"處理 HTTP 請求時發生錯誤: {e}")
        send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, f"HTTP 請求失敗: {e}")
        return f"Error: {str(e)}", 500

@app.route('/run', methods=['GET'])
def run_analysis():
    try:
        logger.info("接收到 /run 請求，執行主要任務")
        main_task()
        return "Analysis complete", 200
    except Exception as e:
        logger.error(f"/run 路由處理錯誤: {e}")
        send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, f"/run 請求失敗: {e}")
        return f"Error: {str(e)}", 500

@app.route('/test-telegram', methods=['GET'])
def test_telegram():
    try:
        logger.info(f"接收到 /test-telegram 請求，發送測試訊息")
        logger.info(f"TELEGRAM_TOKEN: {TELEGRAM_TOKEN[:10]}...")
        logger.info(f"TELEGRAM_CHAT_ID: {TELEGRAM_CHAT_ID}")
        send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, "Test message from Cloud Run")
        logger.info("測試 Telegram 訊息已發送")
        return "Telegram message sent", 200
    except Exception as e:
        logger.error(f"測試 Telegram 失敗: {e}")
        send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, f"測試 Telegram 失敗: {e}")
        return f"Error: {str(e)}", 500

@app.route('/test-sheet', methods=['GET'])
def test_sheet():
    try:
        logger.info("接收到 /test-sheet 請求，測試 Google Sheets 更新")
        logger.info(f"GOOGLE_SHEET_CREDS_JSON: {GOOGLE_SHEET_CREDS_JSON[:50]}...")
        client = setup_sheet_client(GOOGLE_SHEET_CREDS_JSON)
        update_sheet(client, SPREADSHEET_ID, SHEET_NAME, ['test', 'test', 0, 0, 0, 0, 0, 0, 0, 'test'])
        logger.info("Google Sheets 測試更新完成")
        return "Sheet updated", 200
    except Exception as e:
        logger.error(f"測試 Google Sheets 失敗: {e}")
        send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, f"測試 Google Sheets 失敗: {e}")
        return f"Error: {str(e)}", 500

if __name__ == "__main__":
    try:
        logger.info("程式啟動")
        main_task()
        run_scheduler_in_thread()
        port = int(os.environ.get('PORT', 8080))
        app.run(host='0.0.0.0', port=port)
    except Exception as e:
        logger.error(f"程式執行發生錯誤: {e}")
        send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, f"程式啟動失敗: {e}")
