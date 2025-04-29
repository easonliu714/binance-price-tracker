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

# 設定日誌記錄
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 從環境變數獲取設定
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
GOOGLE_SHEET_CREDS_JSON = os.environ.get('GOOGLE_SHEET_CREDS_JSON')
SPREADSHEET_ID = "1Bny_4th50YM2mKSTZDbH7Zqd9Uhl6PHMCCveFMgqMrE"
SHEET_NAME = "Sheet1"

# 建立 Flask 應用
app = Flask(__name__)

# 檢查環境變數是否正確設定
if not all([TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, GOOGLE_SHEET_CREDS_JSON]):
    logger.error("環境變數設定不完整！請檢查 TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, GOOGLE_SHEET_CREDS_JSON")
    raise ValueError("環境變數設定不完整")

def get_taipei_time():
    """返回台北時間"""
    taipei_tz = pytz.timezone('Asia/Taipei')
    return datetime.now(taipei_tz).strftime("%Y-%m-%d %H:%M:%S")

def check_signals(trading_pair, price_data, volume_data, current_price):
    """
    檢查各種交易訊號
    
    Args:
        trading_pair: 交易對名稱
        price_data: 包含價格指標的字典 (1小時K)
        volume_data: 包含交易量指標的字典 (5分鐘K)
        current_price: 當前價格
    
    Returns:
        signals: 觸發的訊號列表
        signal_types: 觸發的訊號類型
    """
    signals = []
    signal_types = []
    taipei_time = get_taipei_time()
    
    try:
        # 檢查當前成交量是否大於10000的門檻
        current_volume = volume_data["volume"][-1]
        if current_volume < 10000:
            logger.info(f"{trading_pair} 當前成交量 {current_volume} 小於10000，跳過訊號檢查")
            return signals, signal_types
            
        logger.info(f"{trading_pair} 當前成交量 {current_volume} 大於10000，開始檢查訊號")
        
        # 解析價格數據
        ma17 = price_data["MA17"][-1]
        ma175 = price_data["MA175"][-1]
        ma425 = price_data["MA425"][-1]
        
        # 解析交易量數據
        vol7 = volume_data["VOL7"][-1]
        vol17 = volume_data["VOL17"][-1]
        
        # 最近的K線收盤價 (1小時K)
        recent_closes = price_data["close"][-4:-1]  # 前1-3根K線的收盤價
        
        # MA425 上漲訊號
        if (current_price > ma425 and 
            all(close < ma425 for close in recent_closes) and 
            all(current_price > close for close in recent_closes)):
            signal_msg = f"[{trading_pair}] MA425上漲，[{current_price}]大於[{ma425:.8f}]，[{taipei_time}]"
            signals.append(signal_msg)
            signal_types.append("MA425上漲")
            logger.info(f"觸發MA425上漲訊號: {signal_msg}")
        
        # MA425 下跌訊號
        if (current_price < ma425 and 
            all(close > ma425 for close in recent_closes) and 
            all(current_price < close for close in recent_closes)):
            signal_msg = f"[{trading_pair}] MA425下跌，[{current_price}]小於[{ma425:.8f}]，[{taipei_time}]"
            signals.append(signal_msg)
            signal_types.append("MA425下跌")
            logger.info(f"觸發MA425下跌訊號: {signal_msg}")
        
        # MA175 上漲訊號
        if (current_price > ma175 and 
            all(close < ma175 for close in recent_closes) and 
            all(current_price > close for close in recent_closes)):
            signal_msg = f"[{trading_pair}] MA175上漲，[{current_price}]大於[{ma175:.8f}]，[{taipei_time}]"
            signals.append(signal_msg)
            signal_types.append("MA175上漲")
            logger.info(f"觸發MA175上漲訊號: {signal_msg}")
        
        # MA175 下跌訊號
        if (current_price < ma175 and 
            all(close > ma175 for close in recent_closes) and 
            all(current_price < close for close in recent_closes)):
            signal_msg = f"[{trading_pair}] MA175下跌，[{current_price}]小於[{ma175:.8f}]，[{taipei_time}]"
            signals.append(signal_msg)
            signal_types.append("MA175下跌")
            logger.info(f"觸發MA175下跌訊號: {signal_msg}")
        
        # 交易量增加訊號
        if current_volume > vol17 * 3 and vol7 > vol17:
            signal_msg = f"[{trading_pair}] 交易量3倍增，[{current_price}]，[{taipei_time}]"
            signals.append(signal_msg)
            signal_types.append("交易量3倍增")
            logger.info(f"觸發交易量增加訊號: {signal_msg}")
            
    except Exception as e:
        logger.error(f"檢查訊號時發生錯誤: {e}")
    
    return signals, signal_types

def process_trading_pair(trading_pair, sheet_client):
    """
    處理單個交易對的數據
    
    Args:
        trading_pair: 交易對名稱
        sheet_client: Google Sheet 客戶端
    """
    try:
        logger.info(f"開始處理交易對: {trading_pair}")
        
        # 獲取5分鐘K線數據 (用於交易量分析)
        klines_5m = get_klines(trading_pair, interval="5m", limit=500)
        if not klines_5m:
            logger.warning(f"無法獲取 {trading_pair} 的5分鐘K線數據")
            return
        
        logger.info(f"成功獲取 {trading_pair} 的5分鐘K線數據，共 {len(klines_5m)} 根")
        
        # 獲取1小時K線數據 (用於價格分析)
        klines_1h = get_klines(trading_pair, interval="1h", limit=500)
        if not klines_1h:
            logger.warning(f"無法獲取 {trading_pair} 的1小時K線數據")
            return
        
        logger.info(f"成功獲取 {trading_pair} 的1小時K線數據，共 {len(klines_1h)} 根")
        
        # 計算價格指標 (1小時K)
        price_indicators = calculate_price_indicators(klines_1h)
        if not price_indicators:
            logger.warning(f"無法計算 {trading_pair} 的價格指標")
            return
        
        # 計算交易量指標 (5分鐘K)
        volume_indicators = calculate_volume_indicators(klines_5m)
        if not volume_indicators:
            logger.warning(f"無法計算 {trading_pair} 的交易量指標")
            return
        
        logger.info(f"成功計算 {trading_pair} 的指標")
        
        # 獲取最新價格
        current_price = float(klines_5m[-1][4])  # 最新5分鐘K線的收盤價
        
        # 檢查訊號
        signals, signal_types = check_signals(trading_pair, price_indicators, volume_indicators, current_price)
        
        # 只有在有訊號時才記錄和發送通知
        if signals:
            # 發送訊號到Telegram
            for signal in signals:
                send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, signal)
            
            # 更新Google Sheet
            taipei_time = get_taipei_time()
            # 準備要寫入的數據，觸發條件用逗號分隔
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
                ", ".join(signal_types)  # 記錄觸發的條件
            ]
            
            update_sheet(sheet_client, SPREADSHEET_ID, SHEET_NAME, row_data)
            
            # 檢查是否需要清理舊數據
            cleanup_old_data(sheet_client, SPREADSHEET_ID, SHEET_NAME, max_rows=10000)
            
            logger.info(f"完成處理交易對: {trading_pair}，共觸發 {len(signals)} 個訊號")
        else:
            logger.info(f"完成處理交易對: {trading_pair}，無訊號觸發")
        
    except Exception as e:
        logger.error(f"處理交易對 {trading_pair} 時發生錯誤: {e}")

def main_task():
    """主要任務，獲取數據並處理訊號"""
    try:
        logger.info("開始執行主要任務")
        
        # 設置Google Sheet客戶端
        sheet_client = setup_sheet_client(GOOGLE_SHEET_CREDS_JSON)
        if not sheet_client:
            logger.error("無法設置Google Sheet客戶端")
            return
        
        # 獲取交易對列表
        trading_pairs = get_trading_pairs()
        if not trading_pairs:
            logger.error("無法獲取交易對列表")
            return
        
        logger.info(f"成功獲取 {len(trading_pairs)} 個USDT交易對")
        
        # 處理每個交易對
        for trading_pair in trading_pairs:
            process_trading_pair(trading_pair, sheet_client)
            time.sleep(1)  # 添加延遲以避免API限制
        
        logger.info("主要任務執行完成")
        
    except Exception as e:
        logger.error(f"執行主要任務時發生錯誤: {e}")

def run_scheduler_in_thread():
    """在單獨的線程中運行定時排程"""
    def run_scheduler():
        logger.info("啟動定時排程")
        
        # 每5分鐘執行一次
        schedule.every(5).minutes.do(main_task)
        
        while True:
            schedule.run_pending()
            time.sleep(1)
    
    thread = Thread(target=run_scheduler)
    thread.daemon = True
    thread.start()

# Flask路由
@app.route("/run", methods=["GET"])
def run_analysis():
    """提供 /run 路由以手動觸發分析任務"""
    try:
        logger.info("接收到 /run 請求，執行一次分析任務")
        main_task()
        return "Analysis complete", 200
    except Exception as e:
        logger.error(f"/run 路由執行分析任務時發生錯誤: {e}")
        return f"Error: {str(e)}", 500

@app.route('/', methods=['GET'])
def index():
    """處理HTTP請求的端點"""
    try:
        logger.info("接收到HTTP請求，執行主要任務")
        main_task()
        return "Task completed successfully", 200
    except Exception as e:
        logger.error(f"處理HTTP請求時發生錯誤: {e}")
        return f"Error: {str(e)}", 500

# 主程式入口
if __name__ == "__main__":
    try:
        logger.info("程式啟動")
        
        # 先執行一次任務
        main_task()
        
        # 在背景執行定時排程
        run_scheduler_in_thread()
        
        # 啟動 Flask 伺服器
        port = int(os.environ.get('PORT', 8080))
        app.run(host='0.0.0.0', port=port)

    
    except Exception as e:
        logger.error(f"程式執行發生錯誤: {e}")

