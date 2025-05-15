import os
import time
import logging
import json
from datetime import datetime, timedelta
import pytz
import requests
import schedule
import backoff
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import statistics
import math
import decimal
from logging.handlers import TimedRotatingFileHandler

# 程式版本資訊
ARTIFACT_ID = "c89b936b-1b27-4f92-8325-b7ab87f11249"
ARTIFACT_VERSION = "b1c2d3e4-f5a6-7b8c-9d0e-f1a2b3c4d5e6"

# 設定日誌
log_file = os.path.join(os.path.dirname(__file__), "trading_signals.log")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
# 命令視窗輸出
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
# 按日期輪替的檔案輸出（每天午夜輪替，保留30天）
file_handler = TimedRotatingFileHandler(
    log_file, when='midnight', interval=1, backupCount=30, encoding='utf-8'
)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# 顯示程式版本
logger.info(f"程式啟動，artifact_id: {ARTIFACT_ID}, version: {ARTIFACT_VERSION}")

# 環境變數
TELEGRAM_TOKEN = '7499365215:AAFRLQfvmrVI_nIyD_5SYpuHMDkM_t0AWEk'
TELEGRAM_CHAT_ID = '7283738727'
SPREADSHEET_ID = "1Bny_4th50YM2mKSTZDbH7Zqd9Uhl6PHMCCveFMgqMrE"
LOCAL_DATA_FILE = os.path.join(os.path.dirname(__file__), "failed_sheet_updates.json")
CREDENTIALS_FILE = os.path.join(os.path.dirname(__file__), "credentials.json")

# 全域變數
run_count = 0
new_entries = 0

# 載入 Google Sheet 憑證
logger.info(f"當前工作目錄: {os.getcwd()}")
try:
    with open(CREDENTIALS_FILE, 'r') as f:
        GOOGLE_SHEET_CREDS_JSON = json.load(f)
except FileNotFoundError:
    logger.error(f"找不到 {CREDENTIALS_FILE} 檔案，請確認檔案與 main.py 位於同一資料夾")
    raise
except Exception as e:
    logger.error(f"讀取 {CREDENTIALS_FILE} 檔案時發生錯誤: {e}")
    raise

def get_precision(value):
    """獲取數值的精度（小數點後位數）"""
    d = decimal.Decimal(str(value))
    return max(0, -d.as_tuple().exponent)

def round_to_precision(value, reference_value):
    """將數值四捨五入到參考值精度的1/100"""
    if value is None:
        return None
    precision = get_precision(reference_value)
    target_precision = precision + 2
    return round(float(value), target_precision)

def send_telegram_message(token, chat_id, message):
    """發送 Telegram 訊息"""
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        params = {"chat_id": chat_id, "text": message}
        response = requests.post(url, json=params)
        response.raise_for_status()
        logger.info(f"Telegram 訊息發送成功: {message}")
        return True
    except Exception as e:
        logger.error(f"Telegram 訊息發送失敗: {e}")
        return False

def setup_sheet_client(creds_json):
    """設置 Google Sheet 客戶端"""
    try:
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
        client = gspread.authorize(creds)
        logger.info("Google Sheet 客戶端設置成功")
        return client
    except Exception as e:
        logger.error(f"設置 Google Sheet 客戶端失敗: {e}")
        return None

def update_sheet(sheet_client, spreadsheet_id, sheet_name, data):
    """更新 Google Sheet"""
    try:
        spreadsheet = sheet_client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.append_row(data)
        logger.info(f"成功更新 Google Sheet {sheet_name}")
        return True
    except gspread.exceptions.WorksheetNotFound:
        logger.warning(f"{sheet_name} 工作表不存在，創建新工作表")
        spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=20)
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.append_row(data)
        logger.info(f"成功創建並更新 Google Sheet {sheet_name}")
        return True
    except Exception as e:
        logger.error(f"更新 Google Sheet {sheet_name} 失敗: {e}")
        return False

def cleanup_old_data(sheet_client, spreadsheet_id, sheet_name, rows_to_delete):
    """清理舊數據，保留第一列標題列，從第二列開始刪除指定行數"""
    try:
        spreadsheet = sheet_client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(sheet_name)
        all_values = worksheet.get_all_values()
        total_rows = len(all_values)
        
        if total_rows > 3200:
            headers = all_values[0]
            rows_to_keep = all_values[1:][rows_to_delete:] if rows_to_delete < total_rows - 1 else []
            worksheet.clear()
            worksheet.append_row(headers)
            if rows_to_keep:
                worksheet.append_rows(rows_to_keep)
            logger.info(f"成功清理 {sheet_name} 舊數據，保留標題列，刪除第2~{rows_to_delete + 1}列（共 {rows_to_delete} 行），保留 {len(rows_to_keep)} 行數據")
        else:
            logger.info(f"{sheet_name} 總行數 {total_rows} 未超過 3200，無需清理")
    except Exception as e:
        logger.error(f"清理 {sheet_name} 舊數據失敗: {e}")

def test_telegram_message(token, chat_id):
    """測試 Telegram 訊息發送"""
    test_message = f"測試訊息：程式即將啟動 (artifact_id: {ARTIFACT_ID}, version: {ARTIFACT_VERSION})"
    logger.info("執行 Telegram 訊息發送測試")
    success = send_telegram_message(token, chat_id, test_message)
    if success:
        logger.info("Telegram 訊息發送測試成功")
    else:
        logger.error("Telegram 訊息發送測試失敗")
    return success

def test_google_sheet_update(sheet_client, spreadsheet_id):
    """測試 Google Sheet 更新"""
    logger.info("執行 Google Sheet 更新測試")
    test_data = [
        datetime.now(pytz.timezone('Asia/Taipei')).strftime("%Y-%m-%d %H:%M:%S"),
        "TEST_PAIR", "TEST", 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
    ]
    success = update_sheet_with_retry(sheet_client, spreadsheet_id, "test", test_data)
    if success:
        logger.info("Google Sheet 更新測試成功（test 工作表）")
    else:
        logger.error("Google Sheet 更新測試失敗（test 工作表）")
    return success

def clear_record_sheet(sheet_client, spreadsheet_id):
    """清空 record 工作表，保留標題列"""
    try:
        spreadsheet = sheet_client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet("record")
        headers = [
            "交易對", "開盤時間", "開盤價", "最高價", "最低價", "收盤價",
            "成交量", "收盤時間", "成交額", "成交筆數", "主動買入成交量", "主動買入成交額"
        ]
        worksheet.clear()
        worksheet.append_row(headers)
        logger.info("成功清空 record 工作表並保留標題列")
    except gspread.exceptions.WorksheetNotFound:
        logger.warning("record 工作表不存在，創建新工作表並添加標題列")
        spreadsheet.add_worksheet(title="record", rows=1200, cols=20)
        worksheet = spreadsheet.worksheet("record")
        worksheet.append_row(headers)
    except Exception as e:
        logger.error(f"清空 record 工作表時發生錯誤: {e}")

def get_trading_pairs():
    """獲取永續合約 USDT 交易對，僅保留 TRADING 狀態"""
    try:
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        usdt_pairs = [
            symbol["symbol"]
            for symbol in data["symbols"]
            if symbol["symbol"].endswith("USDT") and symbol["status"] == "TRADING"
        ]
        logger.info(f"獲取 {len(usdt_pairs)} 個 USDT 永續合約交易對（僅 TRADING 狀態）")
        return usdt_pairs
    except Exception as e:
        logger.error(f"獲取交易對失敗: {e}")
        return []

def get_klines(symbol, interval="15m", limit=500):
    """獲取 K 線數據"""
    try:
        url = "https://fapi.binance.com/fapi/v1/klines"
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        full_url = f"{url}?symbol={symbol}&interval={interval}&limit={limit}"
        logger.info(f"發送 K 線請求: {full_url}")
        response = requests.get(url, params=params)
        response.raise_for_status()
        klines = response.json()
        if not klines:
            logger.warning(f"獲取 {symbol} 的 {interval} K 線數據為空")
        else:
            logger.info(f"成功獲取 {symbol} 的 {interval} K 線數據，數量: {len(klines)}")
        return klines
    except requests.exceptions.HTTPError as e:
        logger.error(f"獲取 {symbol} 的 {interval} K 線數據時發生 HTTP 錯誤: {e}, 回應: {response.text}")
        return []
    except requests.exceptions.RequestException as e:
        logger.error(f"獲取 {symbol} 的 {interval} K 線數據時發生網路錯誤: {e}")
        return []
    except Exception as e:
        logger.error(f"獲取 {symbol} 的 {interval} K 線數據時發生錯誤: {e}")
        return []

def calculate_indicators(klines, period, index=4):
    """計算移動平均，限制精度到收盤價的1/100"""
    values = [float(k[index]) for k in klines]
    if len(values) < period:
        return [None] * len(values)
    ma = []
    reference_value = values[-1]
    for i in range(len(values)):
        if i < period - 1:
            ma.append(None)
        else:
            avg = sum(values[i - period + 1:i + 1]) / period
            ma.append(round_to_precision(avg, reference_value))
    return ma

def calculate_macd(klines, short_period=21, long_period=34, signal_period=8):
    """計算 MACD 指標，限制精度到收盤價的1/100"""
    closes = [float(k[4]) for k in klines]
    reference_value = closes[-1]
    if len(closes) < long_period:
        return [None] * len(closes), [None] * len(closes), [None] * len(closes)
    
    def ema(data, period):
        ema = []
        k = 2 / (period + 1)
        for i in range(len(data)):
            if i == 0:
                ema.append(round_to_precision(data[i], reference_value))
            else:
                value = data[i] * k + ema[-1] * (1 - k)
                ema.append(round_to_precision(value, reference_value))
        return ema
    
    short_ema = ema(closes, short_period)
    long_ema = ema(closes, long_period)
    dif = [round_to_precision(short_ema[i] - long_ema[i], reference_value) for i in range(len(closes))]
    dea = ema(dif, signal_period)
    macd = [round_to_precision(2 * (dif[i] - dea[i]), reference_value) for i in range(len(closes))]
    
    return dif, dea, macd

def calculate_bollinger_bands(klines, period=21, bandwidth=2):
    """計算布林區間，限制精度到收盤價的1/100"""
    closes = [float(k[4]) for k in klines]
    reference_value = closes[-1]
    if len(closes) < period:
        return [None] * len(closes), [None] * len(closes), [None] * len(closes)
    
    mb = []
    up = []
    dn = []
    
    for i in range(len(closes)):
        if i < period - 1:
            mb.append(None)
            up.append(None)
            dn.append(None)
        else:
            window = closes[i - period + 1:i + 1]
            ma = sum(window) / period
            std = statistics.stdev(window) if len(window) > 1 else 0
            mb.append(round_to_precision(ma, reference_value))
            up.append(round_to_precision(ma + bandwidth * std, reference_value))
            dn.append(round_to_precision(ma - bandwidth * std, reference_value))
    
    return up, mb, dn

def calculate_ma233_angle(ma233, kline_open_time_ms, interval):
    """計算 MA233 的角度（度數），使用正規化處理，限制角度到小數點後2位，X軸基於時間區間"""
    if len(ma233) < 2 or ma233[-1] is None or ma233[-2] is None or ma233[-2] == 0:
        return None
    
    interval_minutes = {
        "15m": 15,
        "1h": 60,
        "4h": 240,
        "1d": 1440
    }.get(interval, 15)
    x = interval_minutes / 1440
    
    y = (ma233[-1] / ma233[-2]) - 1
    
    try:
        if x == 0:
            return 90.0 if y > 0 else -90.0
        tan_theta = y / x
        angle = math.degrees(math.atan(tan_theta))
        return round(angle, 2)
    except Exception as e:
        logger.error(f"計算 MA233 角度時發生錯誤: {e}")
        return None

def calculate_ma_angle(ma34, ma233, kline_open_time_ms, interval):
    """計算 MA34 和 MA233 的夾角（度數），使用正規化處理，限制角度到小數點後2位，X軸基於時間區間"""
    if (len(ma34) < 2 or len(ma233) < 2 or 
        ma34[-1] is None or ma34[-2] is None or 
        ma233[-1] is None or ma233[-2] is None or 
        ma233[-1] == 0 or ma233[-2] == 0):
        return None
    
    interval_minutes = {
        "15m": 15,
        "1h": 60,
        "4h": 240,
        "1d": 1440
    }.get(interval, 15)
    x = interval_minutes / 1440
    
    current_ratio = ma34[-1] / ma233[-1]
    prev_ratio = ma34[-2] / ma233[-2]
    y = current_ratio - prev_ratio
    
    try:
        if x == 0:
            return 90.0
        tan_theta = y / x
        angle = math.degrees(math.atan(tan_theta))
        return round(angle, 2)
    except Exception as e:
        logger.error(f"計算夾角時發生錯誤: {e}")
        return None

def calculate_previous_day_amplitude(klines, current_time):
    """計算前一日振幅：(前一日最高價 - 前一日最低價) / 前一日最低價"""
    try:
        taipei_tz = pytz.timezone('Asia/Taipei')
        current_date = current_time.date()
        previous_day = current_date - timedelta(days=1)
        
        previous_day_klines = [
            k for k in klines
            if datetime.fromtimestamp(int(k[0]) / 1000, tz=pytz.UTC).astimezone(taipei_tz).date() == previous_day
        ]
        
        if not previous_day_klines:
            logger.warning("無前一日 K 線資料，無法計算振幅")
            return None
        
        high_prices = [float(k[2]) for k in previous_day_klines]
        low_prices = [float(k[3]) for k in previous_day_klines]
        previous_day_high = max(high_prices)
        previous_day_low = min(low_prices)
        
        if previous_day_low == 0:
            logger.error("前一日最低價為 0，無法計算振幅")
            return None
        
        amplitude = (previous_day_high - previous_day_low) / previous_day_low * 100
        return round(amplitude, 2)
    except Exception as e:
        logger.error(f"計算前一日振幅時發生錯誤: {e}")
        return None

def timestamp_to_taipei(timestamp_ms):
    """將毫秒時間戳轉為 UTC+8（台北時間）格式"""
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=pytz.UTC).astimezone(pytz.timezone('Asia/Taipei')).strftime("%Y-%m-%d %H:%M:%S")

def check_signals(trading_pair, price_data, volume_data, current_price, klines, interval="15m"):
    """檢查訊號：基於成交額、K 線型態、MA34/MA233 條件及 MACD，條件2和3依賴條件1，其他條件獨立"""
    signals, signal_types = [], []
    taipei_time = datetime.now(pytz.timezone('Asia/Taipei'))
    ma233_angle = None
    ma_angle = None
    price_change_pct = None
    previous_day_amplitude = None
    
    if not volume_data["quote_volume"]:
        logger.warning(f"{trading_pair} 的成交額數據為空，無法檢查訊號")
        return signals, signal_types, ma233_angle, ma_angle, price_change_pct, previous_day_amplitude
    
    current_quote_volume = float(volume_data["quote_volume"][-1])
    vol21_current = volume_data["VOL21"][-1]
    
    condition_1_triggered = False
    if vol21_current is not None and current_quote_volume > 3 * vol21_current:
        signal = f"{taipei_time.strftime('%Y-%m-%d %H:%M:%S')} - {trading_pair}: 量增"
        signals.append(signal)
        signal_types.append("量增")
        logger.info(f"{trading_pair}: 成交額 {current_quote_volume} > 3 * VOL21 {vol21_current}")
        condition_1_triggered = True
    
    dif, dea, macd = calculate_macd(klines)
    
    ma21_current = price_data["MA21"][-1]
    ma34_current = price_data["MA34"][-1]
    ma233_current = price_data["MA233"][-1]
    ma34_prev = price_data["MA34"][-2] if len(price_data["MA34"]) >= 2 else None
    ma233_prev = price_data["MA233"][-2] if len(price_data["MA233"]) >= 2 else None
    
    open_price = float(klines[-1][1])
    high_price = float(klines[-1][2])
    low_price = float(klines[-1][3])
    close_price = float(klines[-1][4])
    
    if ma34_current is not None and ma34_current != 0:
        price_change_pct = (close_price - ma34_current) / ma34_current * 100
        price_change_pct = round(price_change_pct, 2)
    previous_day_amplitude = calculate_previous_day_amplitude(klines, taipei_time)
    
    if condition_1_triggered:
        min_open_close = min(open_price, close_price)
        price_diff = abs(close_price - open_price)
        if (min_open_close - low_price) > 2 * price_diff and \
           ma21_current is not None and close_price > ma21_current and \
           macd[-1] is not None and macd[-1] > 0 and \
           dif[-1] is not None and dea[-1] is not None and dif[-1] > dea[-1] > 0:
            ma21_last_5 = price_data["MA21"][-5:]
            if len(ma21_last_5) == 5 and all(ma21_last_5[i] <= ma21_last_5[i + 1] for i in range(len(ma21_last_5) - 1)):
                signal = f"{taipei_time.strftime('%Y-%m-%d %H:%M:%S')} - {trading_pair}: 短多"
                signals.append(signal)
                signal_types.append("短多")
                logger.info(f"{trading_pair}: 短多條件觸發")
        
        max_open_close = max(open_price, close_price)
        if (high_price - max_open_close) > 2 * price_diff and \
           ma21_current is not None and close_price < ma21_current and \
           macd[-1] is not None and macd[-1] < 0 and \
           dif[-1] is not None and dea[-1] is not None and dif[-1] < dea[-1] < 0:
            ma21_last_5 = price_data["MA21"][-5:]
            if len(ma21_last_5) == 5 and all(ma21_last_5[i] >= ma21_last_5[i + 1] for i in range(len(ma21_last_5) - 1)):
                signal = f"{taipei_time.strftime('%Y-%m-%d %H:%M:%S')} - {trading_pair}: 短空"
                signals.append(signal)
                signal_types.append("短空")
                logger.info(f"{trading_pair}: 短空條件觸發")
    
    if (ma34_current is not None and ma233_current is not None and
        ma34_prev is not None and ma233_prev is not None and
        ma34_prev >= ma233_prev and ma34_current < ma233_current and
        dif[-1] is not None and dea[-1] is not None and dif[-1] < dea[-1] < 0):
        signal = f"{taipei_time.strftime('%Y-%m-%d %H:%M:%S')} - {trading_pair}: 長空"
        signals.append(signal)
        signal_types.append("長空")
        logger.info(f"{trading_pair}: 長空條件觸發（MA34/MA233 死亡交叉）")
        ma233_angle = calculate_ma233_angle(price_data["MA233"], klines[-1][0], interval)
        ma_angle = calculate_ma_angle(price_data["MA34"], price_data["MA233"], klines[-1][0], interval)
    
    if (ma34_current is not None and ma233_current is not None and
        ma34_prev is not None and ma233_prev is not None and
        ma34_prev <= ma233_prev and ma34_current > ma233_current and
        dif[-1] is not None and dea[-1] is not None and dif[-1] > dea[-1] > 0):
        signal = f"{taipei_time.strftime('%Y-%m-%d %H:%M:%S')} - {trading_pair}: 長多"
        signals.append(signal)
        signal_types.append("長多")
        logger.info(f"{trading_pair}: 長多條件觸發（MA34/MA233 黃金交叉）")
        ma233_angle = calculate_ma233_angle(price_data["MA233"], klines[-1][0], interval)
        ma_angle = calculate_ma_angle(price_data["MA34"], price_data["MA233"], klines[-1][0], interval)
    
    if (dif[-1] is not None and dea[-1] is not None and macd[-1] is not None and
        macd[-2] is not None and macd[-2] < 0 and macd[-1] > 0 and
        0 < dif[-1]/close_price < 0.005 and
        ma21_current is not None and close_price > ma21_current and
        close_price > open_price and
        all(float(klines[i][4]) <= float(klines[i+1][4]) for i in range(-3, -1))):
        signal = f"{taipei_time.strftime('%Y-%m-%d %H:%M:%S')} - {trading_pair}: MACD轉強"
        signals.append(signal)
        signal_types.append("MACD轉強")
        logger.info(f"{trading_pair}: MACD 轉強條件觸發")
    
    if (dif[-1] is not None and dea[-1] is not None and macd[-1] is not None and
        macd[-2] is not None and macd[-2] > 0 and macd[-1] < 0 and
        -0.005 < dif[-1]/close_price < 0 and
        ma21_current is not None and close_price < ma21_current and
        close_price < open_price and
        all(float(klines[i][4]) >= float(klines[i+1][4]) for i in range(-3, -1))):
        signal = f"{taipei_time.strftime('%Y-%m-%d %H:%M:%S')} - {trading_pair}: MACD轉弱"
        signals.append(signal)
        signal_types.append("MACD轉弱")
        logger.info(f"{trading_pair}: MACD 轉弱條件觸發")
    
    if low_price > 0:
        amplitude = (high_price - low_price) / low_price
        if amplitude > 0.03:
            signal = f"{taipei_time.strftime('%Y-%m-%d %H:%M:%S')} - {trading_pair}: 振幅"
            signals.append(signal)
            signal_types.append("振幅")
            logger.info(f"{trading_pair}: 振幅條件觸發，振幅={amplitude:.4f}")
    
    up, mb, dn = calculate_bollinger_bands(klines)
    if (dn[-1] is not None and mb[-1] is not None and
        dn[-1] < mb[-1] * 0.86 and
        close_price > open_price):
        ma21_last_5 = price_data["MA21"][-5:]
        if len(ma21_last_5) == 5 and all(ma21_last_5[i] >= ma21_last_5[i + 1] for i in range(len(ma21_last_5) - 1)):
            signal = f"{taipei_time.strftime('%Y-%m-%d %H:%M:%S')} - {trading_pair}: 布林反轉向上"
            signals.append(signal)
            signal_types.append("布林反轉向上")
            logger.info(f"{trading_pair}: 布林反轉向上條件觸發")
    
    if (up[-1] is not None and mb[-1] is not None and
        up[-1] > mb[-1] * 1.14 and
        close_price < open_price):
        ma21_last_5 = price_data["MA21"][-5:]
        if len(ma21_last_5) == 5 and all(ma21_last_5[i] <= ma21_last_5[i + 1] for i in range(len(ma21_last_5) - 1)):
            signal = f"{taipei_time.strftime('%Y-%m-%d %H:%M:%S')} - {trading_pair}: 布林反轉向下"
            signals.append(signal)
            signal_types.append("布林反轉向下")
            logger.info(f"{trading_pair}: 布林反轉向下條件觸發")

    return signals, signal_types, ma233_angle, ma_angle, price_change_pct, previous_day_amplitude

def save_to_local_file(data, sheet_name):
    """將未能寫入 Google Sheet 的數據保存到本地檔案"""
    try:
        data_entry = {
            "timestamp": datetime.now(pytz.timezone('Asia/Taipei')).strftime("%Y-%m-%d %H:%M:%S"),
            "sheet_name": sheet_name,
            "data": data
        }
        if os.path.exists(LOCAL_DATA_FILE):
            with open(LOCAL_DATA_FILE, 'r') as f:
                existing_data = json.load(f)
        else:
            existing_data = []
        existing_data.append(data_entry)
        with open(LOCAL_DATA_FILE, 'w') as f:
            json.dump(existing_data, f, indent=2)
        logger.info(f"已將數據保存到本地檔案 {LOCAL_DATA_FILE}")
    except Exception as e:
        logger.error(f"保存數據到本地檔案時發生錯誤: {e}")

@backoff.on_exception(backoff.expo, Exception, max_tries=3, max_time=60)
def update_sheet_with_retry(sheet_client, spreadsheet_id, sheet_name, data):
    """帶重試機制的 Google Sheet 更新"""
    try:
        success = update_sheet(sheet_client, spreadsheet_id, sheet_name, data)
        if not success:
            logger.error(f"更新 Google Sheet {sheet_name} 失敗，保存到本地")
            save_to_local_file(data, sheet_name)
        return success
    except Exception as e:
        logger.error(f"更新 Google Sheet {sheet_name} 時發生錯誤: {e}")
        save_to_local_file(data, sheet_name)
        raise

def get_triggered_pairs(sheet_client, spreadsheet_id, time_window_hours=12):
    """從Google Sheet的15min工作表獲取開盤時間在指定時間窗口內且觸發特定訊號的交易對"""
    try:
        spreadsheet = sheet_client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet("15min")
        all_values = worksheet.get_all_values()
        if len(all_values) <= 1:
            logger.info("15min工作表無數據，無法獲取觸發交易對")
            return []

        headers = all_values[0]
        data_rows = all_values[1:]
        taipei_tz = pytz.timezone('Asia/Taipei')
        current_time = datetime.now(taipei_tz)
        time_threshold = current_time - timedelta(hours=time_window_hours)

        triggered_pairs = []
        for row in data_rows:
            try:
                open_time_str = row[0]
                open_time = datetime.strptime(open_time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=taipei_tz)
                if open_time < time_threshold:
                    continue
                trading_pair = row[1]
                signal_types = row[2].split(", ")
                if any(signal in ["長空", "長多", "空方回測續弱", "多方回測續強"] for signal in signal_types):
                    triggered_pairs.append({
                        "trading_pair": trading_pair,
                        "open_time": open_time,
                        "signal_types": signal_types
                    })
            except Exception as e:
                logger.warning(f"處理15min工作表行數據時發生錯誤: {e}")
                continue

        triggered_pairs.sort(key=lambda x: x["open_time"])
        logger.info(f"篩選到 {len(triggered_pairs)} 個符合條件的交易對（開盤時間在過去 {time_window_hours} 小時內，包含長空、長多、空方回測續弱或多方回測續強）")
        return triggered_pairs
    except Exception as e:
        logger.error(f"獲取15min工作表數據失敗: {e}")
        return []

def check_macd_conditions(trading_pair, klines, open_time, signal_types, temporary_db):
    """檢查條件11（空方回測續弱）和條件12（多方回測續強），檢查每一筆長空或長多記錄"""
    signals = []
    signal_types_out = []
    taipei_tz = pytz.timezone('Asia/Taipei')
    
    logger.info(f"{trading_pair}: 開始檢查 MACD 條件")

    if not klines:
        logger.error(f"{trading_pair}: K 線資料為空，無法進行 MACD 條件檢查")
        return signals, signal_types_out

    def has_subsequent_signals(trading_pair, open_time, target_signals):
        for record in temporary_db:
            if (record["trading_pair"] == trading_pair and
                record["open_time"] > open_time and
                any(signal in target_signals for signal in record["signal_types"])):
                return True, record
        return False, None

    # 條件11：空方回測續弱（檢查每一筆長空記錄）
    long_short_records = [r for r in temporary_db if r["trading_pair"] == trading_pair and "長空" in r["signal_types"]]
    if long_short_records:
        logger.info(f"{trading_pair}: 找到 {len(long_short_records)} 筆長空記錄，開始逐一檢查條件11")
        for record in long_short_records:
            record_open_time = record["open_time"]
            record_open_time_ms = int(record_open_time.astimezone(pytz.UTC).timestamp() * 1000)
            logger.info(f"{trading_pair}: 檢查長空記錄，開盤時間: {record_open_time}")
            
            has_signal, subsequent_record = has_subsequent_signals(
                trading_pair, record_open_time, ["空方回測續弱", "長多", "長空"]
            )
            if has_signal:
                logger.info(f"{trading_pair}: 找到後續觸發記錄（{subsequent_record['signal_types']} at {subsequent_record['open_time']}），跳過條件11檢查")
                continue
            
            logger.info(f"{trading_pair}: 無後續觸發記錄，執行條件11檢查")
            dif, dea, macd = calculate_macd(klines)
            if not macd or macd[-1] is None:
                logger.error(f"{trading_pair}: MACD 計算結果無效（長度: {len(macd) if macd else 0}, 最新值: {macd[-1] if macd and macd[-1] is not None else None}），跳過條件11檢查，K線數量: {len(klines)}, 最新K線時間: {timestamp_to_taipei(klines[-1][0]) if klines else '無'}")
                continue
            
            logger.info(f"{trading_pair}: MACD 計算成功，數據長度: {len(macd)}, 最新值: {macd[-1]:.6f}")
            
            ma233 = calculate_indicators(klines, 233)
            if not ma233 or ma233[-1] is None:
                logger.error(f"{trading_pair}: MA233 計算結果無效（長度: {len(ma233) if ma233 else 0}, 最新值: {ma233[-1] if ma233 and ma233[-1] is not None else None}），跳過條件11檢查")
                continue
            
            macd_from_open = []
            indices_from_open = []
            for i, k in enumerate(klines):
                if int(k[0]) >= record_open_time_ms and macd[i] is not None:
                    macd_from_open.append(macd[i])
                    indices_from_open.append(i)
            
            if len(macd_from_open) < 2:
                logger.error(f"{trading_pair}: 從開盤時間 {record_open_time} 起的 MACD 數據不足（數量: {len(macd_from_open)}），無法檢查條件11")
                continue
            logger.info(f"{trading_pair}: 篩選到 {len(macd_from_open)} 筆從開盤時間 {record_open_time} 起的 MACD 數據")
            
            negative_to_positive_idx = None
            for i in range(1, len(macd_from_open)):
                if macd_from_open[i-1] < 0 and macd_from_open[i] > 0:
                    negative_to_positive_idx = indices_from_open[i]
                    logger.info(f"{trading_pair}: 找到 MACD 負值轉正值，索引: {negative_to_positive_idx}, 時間: {timestamp_to_taipei(klines[negative_to_positive_idx][0])}, MACD[{i-1}]: {macd_from_open[i-1]:.6f}, MACD[{i}]: {macd_from_open[i]:.6f}")
                    break
            
            if negative_to_positive_idx is None:
                logger.info(f"{trading_pair}: 未找到 MACD 負值轉正值，條件11未觸發（開盤時間: {record_open_time}）")
                continue
            
            logger.info(f"{trading_pair}: 從索引 {negative_to_positive_idx} 開始檢查 MACD/收盤價 > 0.001 且 收盤價/MA233 > 0.96")
            for i in range(indices_from_open.index(negative_to_positive_idx), len(macd_from_open)):
                global_idx = indices_from_open[i]
                try:
                    close_price = float(klines[global_idx][4])
                    ma233_value = ma233[global_idx]
                    macd_value = macd[global_idx]
                    
                    if (macd_value is None or close_price == 0 or 
                        ma233_value is None or ma233_value == 0):
                        logger.debug(f"{trading_pair}: 條件11檢查索引 {global_idx} 跳過，MACD: {macd_value}, 收盤價: {close_price}, MA233: {ma233_value}")
                        continue
                    
                    if macd_value < 0:
                        price_ma233_ratio = close_price / ma233_value
                        logger.info(f"{trading_pair}: 條件11檢查終止，MACD 再次轉負，索引: {global_idx}, 時間: {timestamp_to_taipei(klines[global_idx][0])}, MACD: {macd_value:.6f}, 收盤價: {close_price:.6f}, 收盤價/MA233: {price_ma233_ratio:.6f}")
                        break
                    
                    macd_ratio = macd_value / close_price
                    price_ma233_ratio = close_price / ma233_value
                    open_time_current = klines[global_idx][0]
                    open_time_latest = klines[-1][0]
                    
                    logger.debug(f"{trading_pair}: 條件11檢查索引 {global_idx}, MACD: {macd_value:.6f}, 收盤價: {close_price:.6f}, MA233: {ma233_value:.6f}, MACD/收盤價: {macd_ratio:.6f}, 收盤價/MA233: {price_ma233_ratio:.6f}, 開盤時間: {timestamp_to_taipei(open_time_current)}")
                    
                    if macd_ratio > 0.001 and price_ma233_ratio > 0.96:
                        if open_time_current == open_time_latest:
                            logger.info(f"{trading_pair}: 條件11觸發 - 空方回測續弱（基於長空記錄 at {record_open_time}），索引: {global_idx}, 時間: {timestamp_to_taipei(open_time_current)}, MACD: {macd_value:.6f}, MACD/收盤價: {macd_ratio:.6f}, 收盤價/MA233: {price_ma233_ratio:.6f}")
                            signal = f"{datetime.now(taipei_tz).strftime('%Y-%m-%d %H:%M:%S')} - {trading_pair}: 空方回測續弱"
                            signals.append(signal)
                            signal_types_out.append("空方回測續弱")
                            return signals, signal_types_out
                        else:
                            logger.info(f"{trading_pair}: 條件11未觸發，滿足 MACD/收盤價 > 0.001 且 收盤價/MA233 > 0.96，但開盤時間不匹配，索引: {global_idx}, 時間: {timestamp_to_taipei(open_time_current)}, MACD: {macd_value:.6f}, 收盤價: {close_price:.6f}, 收盤價/MA233: {price_ma233_ratio:.6f}")
                except Exception as e:
                    logger.error(f"{trading_pair}: 條件11檢查索引 {global_idx} 發生錯誤: {e}")
                    continue

    # 條件12：多方回測續強（檢查每一筆長多記錄）
    long_multi_records = [r for r in temporary_db if r["trading_pair"] == trading_pair and "長多" in r["signal_types"]]
    if long_multi_records:
        logger.info(f"{trading_pair}: 找到 {len(long_multi_records)} 筆長多記錄，開始逐一檢查條件12")
        for record in long_multi_records:
            record_open_time = record["open_time"]
            record_open_time_ms = int(record_open_time.astimezone(pytz.UTC).timestamp() * 1000)
            logger.info(f"{trading_pair}: 檢查長多記錄，開盤時間: {record_open_time}")
            
            has_signal, subsequent_record = has_subsequent_signals(
                trading_pair, record_open_time, ["多方回測續強", "長多", "長空"]
            )
            if has_signal:
                logger.info(f"{trading_pair}: 找到後續觸發記錄（{subsequent_record['signal_types']} at {subsequent_record['open_time']}），跳過條件12檢查")
                continue
            
            logger.info(f"{trading_pair}: 無後續觸發記錄，執行條件12檢查")
            dif, dea, macd = calculate_macd(klines)
            if not macd or macd[-1] is None:
                logger.error(f"{trading_pair}: MACD 計算結果無效（長度: {len(macd) if macd else 0}, 最新值: {macd[-1] if macd and macd[-1] is not None else None}），跳過條件12檢查，K線數量: {len(klines)}, 最新K線時間: {timestamp_to_taipei(klines[-1][0]) if klines else '無'}")
                continue
            logger.info(f"{trading_pair}: MACD 計算成功，數據長度: {len(macd)}, 最新值: {macd[-1]:.6f}")
            
            ma233 = calculate_indicators(klines, 233)
            if not ma233 or ma233[-1] is None:
                logger.error(f"{trading_pair}: MA233 計算結果無效（長度: {len(ma233) if ma233 else 0}, 最新值: {ma233[-1] if ma233 and ma233[-1] is not None else None}），跳過條件12檢查")
                continue
            
            macd_from_open = []
            indices_from_open = []
            for i, k in enumerate(klines):
                if int(k[0]) >= record_open_time_ms and macd[i] is not None:
                    macd_from_open.append(macd[i])
                    indices_from_open.append(i)
            
            if len(macd_from_open) < 2:
                logger.error(f"{trading_pair}: 從開盤時間 {record_open_time} 起的 MACD 數據不足（數量: {len(macd_from_open)}），無法檢查條件12")
                continue
            logger.info(f"{trading_pair}: 篩選到 {len(macd_from_open)} 筆從開盤時間 {record_open_time} 起的 MACD 數據")
            
            positive_to_negative_idx = None
            for i in range(1, len(macd_from_open)):
                if macd_from_open[i-1] > 0 and macd_from_open[i] < 0:
                    positive_to_negative_idx = indices_from_open[i]
                    logger.info(f"{trading_pair}: 找到 MACD 正值轉負值，索引: {positive_to_negative_idx}, 時間: {timestamp_to_taipei(klines[positive_to_negative_idx][0])}, MACD[{i-1}]: {macd_from_open[i-1]:.6f}, MACD[{i}]: {macd_from_open[i]:.6f}")
                    break
            
            if positive_to_negative_idx is None:
                logger.info(f"{trading_pair}: 未找到 MACD 正值轉負值，條件12未觸發（開盤時間: {record_open_time}）")
                continue
            
            logger.info(f"{trading_pair}: 從索引 {positive_to_negative_idx} 開始檢查 MACD/收盤價 < -0.001 且 收盤價/MA233 < 1.04")
            for i in range(indices_from_open.index(positive_to_negative_idx), len(macd_from_open)):
                global_idx = indices_from_open[i]
                try:
                    close_price = float(klines[global_idx][4])
                    ma233_value = ma233[global_idx]
                    macd_value = macd[global_idx]
                    
                    if (macd_value is None or close_price == 0 or 
                        ma233_value is None or ma233_value == 0):
                        logger.debug(f"{trading_pair}: 條件12檢查索引 {global_idx} 跳過，MACD: {macd_value}, 收盤價: {close_price}, MA233: {ma233_value}")
                        continue
                    
                    if macd_value > 0:
                        price_ma233_ratio = close_price / ma233_value
                        logger.info(f"{trading_pair}: 條件12檢查終止，MACD 再次轉正，索引: {global_idx}, 時間: {timestamp_to_taipei(klines[global_idx][0])}, MACD: {macd_value:.6f}, 收盤價: {close_price:.6f}, 收盤價/MA233: {price_ma233_ratio:.6f}")
                        break
                    
                    macd_ratio = macd_value / close_price
                    price_ma233_ratio = close_price / ma233_value
                    open_time_current = klines[global_idx][0]
                    open_time_latest = klines[-1][0]
                    
                    logger.debug(f"{trading_pair}: 條件12檢查索引 {global_idx}, MACD: {macd_value:.6f}, 收盤價: {close_price:.6f}, MA233: {ma233_value:.6f}, MACD/收盤價: {macd_ratio:.6f}, 收盤價/MA233: {price_ma233_ratio:.6f}, 開盤時間: {timestamp_to_taipei(open_time_current)}")
                    
                    if macd_ratio < -0.001 and price_ma233_ratio < 1.04:
                        if open_time_current == open_time_latest:
                            logger.info(f"{trading_pair}: 條件12觸發 - 多方回測續強（基於長多記錄 at {record_open_time}），索引: {global_idx}, 時間: {timestamp_to_taipei(open_time_current)}, MACD: {macd_value:.6f}, MACD/收盤價: {macd_ratio:.6f}, 收盤價/MA233: {price_ma233_ratio:.6f}")
                            signal = f"{datetime.now(taipei_tz).strftime('%Y-%m-%d %H:%M:%S')} - {trading_pair}: 多方回測續強"
                            signals.append(signal)
                            signal_types_out.append("多方回測續強")
                            return signals, signal_types_out
                        else:
                            logger.info(f"{trading_pair}: 條件12未觸發，滿足 MACD/收盤價 < -0.001 且 收盤價/MA233 < 1.04，但開盤時間不匹配，索引: {global_idx}, 時間: {timestamp_to_taipei(open_time_current)}, MACD: {macd_value:.6f}, 收盤價: {close_price:.6f}, 收盤價/MA233: {price_ma233_ratio:.6f}")
                except Exception as e:
                    logger.error(f"{trading_pair}: 條件12檢查索引 {global_idx} 發生錯誤: {e}")
                    continue

    return signals, signal_types_out

def process_trading_pair(trading_pair, sheet_client):
    """處理單個交易對（條件1-10）"""
    global new_entries
    logger.info(f"開始處理交易對: {trading_pair}")
    klines_15m = get_klines(trading_pair, "15m", 500)
    
    if not klines_15m:
        logger.warning(f"無法獲取 {trading_pair} 的 15m K 線數據，跳過此交易對")
        return
    
    if len(klines_15m) < 234:
        logger.info(f"{trading_pair} 的 K 線數量 {len(klines_15m)} < 234，跳過此交易對")
        return
    
    current_quote_volume = float(klines_15m[-1][7])
    if current_quote_volume < 10000:
        logger.info(f"{trading_pair} 的成交額 {current_quote_volume} < 10000，跳過此交易對")
        return
    
    price_indicators_15m = {
        "close": [float(k[4]) for k in klines_15m],
        "MA21": calculate_indicators(klines_15m, 21),
        "MA34": calculate_indicators(klines_15m, 34),
        "MA233": calculate_indicators(klines_15m, 233)
    }
    volume_indicators_15m = {
        "quote_volume": [float(k[7]) for k in klines_15m],
        "VOL8": calculate_indicators(klines_15m, 8, index=7),
        "VOL21": calculate_indicators(klines_15m, 21, index=7)
    }
    
    if not volume_indicators_15m["quote_volume"]:
        logger.warning(f"{trading_pair} 的成交額數據為空，跳過此交易對")
        return
    
    current_price_15m = float(klines_15m[-1][4])
    
    signals, signal_types, ma233_angle, ma_angle, price_change_pct, previous_day_amplitude = check_signals(
        trading_pair, price_indicators_15m, volume_indicators_15m, current_price_15m, klines_15m, interval="15m"
    )
    
    if signal_types:
        open_time_15m = timestamp_to_taipei(klines_15m[-1][0])
        if not ("長空" in signal_types or "長多" in signal_types):
            ma233_angle = ""
            ma_angle = ""
        row_data_15m = [
            open_time_15m,
            trading_pair,
            ", ".join(signal_types),
            current_price_15m,
            price_indicators_15m["MA21"][-1],
            price_indicators_15m["MA34"][-1],
            price_indicators_15m["MA233"][-1],
            volume_indicators_15m["quote_volume"][-1],
            volume_indicators_15m["VOL8"][-1],
            volume_indicators_15m["VOL21"][-1],
            ma233_angle if ma233_angle is not None else "",
            ma_angle if ma_angle is not None else "",
            price_change_pct if price_change_pct is not None else "",
            previous_day_amplitude if previous_day_amplitude is not None else "",
            price_indicators_15m["MA34"][-2] if len(price_indicators_15m["MA34"]) >= 2 and price_indicators_15m["MA34"][-2] is not None else "",
            price_indicators_15m["MA233"][-2] if len(price_indicators_15m["MA233"]) >= 2 and price_indicators_15m["MA233"][-2] is not None else ""
        ]
        update_sheet_with_retry(sheet_client, SPREADSHEET_ID, "15min", row_data_15m)
        new_entries += 1
        
        signal_message = f"{open_time_15m} - {trading_pair}: {', '.join(signal_types)}"
        if "長空" in signal_types or "長多" in signal_types:
            if ma233_angle is not None and ma_angle is not None:
                signal_message += f"\nMA233 角度: {ma233_angle}°\nMA34/MA233 夾角: {ma_angle}°"
        if price_change_pct is not None:
            signal_message += f"\n價差比: {price_change_pct}%"
        if previous_day_amplitude is not None:
            signal_message += f"\n前日振幅: {previous_day_amplitude}%"
        send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, signal_message)
    
    latest_kline = klines_15m[-1]
    row_data_record = [
        trading_pair,
        timestamp_to_taipei(latest_kline[0]),
        float(latest_kline[1]),
        float(latest_kline[2]),
        float(latest_kline[3]),
        float(latest_kline[4]),
        float(latest_kline[5]),
        timestamp_to_taipei(latest_kline[6]),
        float(latest_kline[7]),
        latest_kline[8],
        float(latest_kline[9]),
        float(latest_kline[10])
    ]
    update_sheet_with_retry(sheet_client, SPREADSHEET_ID, "record", row_data_record)

def main_task():
    """主要任務"""
    global run_count, new_entries
    run_count += 1
    new_entries = 0
    start_time = datetime.now(pytz.timezone('Asia/Taipei'))
    logger.info(f"開始執行 main_task (Run {run_count}, artifact_id: {ARTIFACT_ID}, version: {ARTIFACT_VERSION}): {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    run_message = f"Run {run_count} started at {start_time.strftime('%Y-%m-%d %H:%M:%S')} (artifact_id: {ARTIFACT_ID}, version: {ARTIFACT_VERSION})"
    send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, run_message)
    
    sheet_client = setup_sheet_client(GOOGLE_SHEET_CREDS_JSON)
    if not sheet_client:
        logger.error("無法設置 Google Sheet 客戶端，任務終止")
        return
    
    clear_record_sheet(sheet_client, SPREADSHEET_ID)
    
    logger.info("開始檢查條件11和12（空方回測續弱和多方回測續強）")
    temporary_db = get_triggered_pairs(sheet_client, SPREADSHEET_ID)
    logger.info(f"臨時資料庫篩選到 {len(temporary_db)} 筆記錄")
    
    for pair_info in temporary_db:
        trading_pair = pair_info["trading_pair"]
        open_time = pair_info["open_time"]
        signal_types = pair_info["signal_types"]
        logger.info(f"檢查交易對 {trading_pair} 的MACD條件，開盤時間: {open_time}, 訊號類型: {signal_types}")
        klines_15m = get_klines(trading_pair, "15m", 500)
        if not klines_15m:
            logger.warning(f"無法獲取 {trading_pair} 的 15m K 線數據，跳過MACD條件檢查")
            continue
        
        signals, macd_signal_types = check_macd_conditions(trading_pair, klines_15m, open_time, signal_types, temporary_db)
        if macd_signal_types:
            current_price_15m = float(klines_15m[-1][4])
            price_indicators_15m = {
                "MA21": calculate_indicators(klines_15m, 21),
                "MA34": calculate_indicators(klines_15m, 34),
                "MA233": calculate_indicators(klines_15m, 233)
            }
            volume_indicators_15m = {
                "quote_volume": [float(k[7]) for k in klines_15m],
                "VOL8": calculate_indicators(klines_15m, 8, index=7),
                "VOL21": calculate_indicators(klines_15m, 21, index=7)
            }
            taipei_time = datetime.now(pytz.timezone('Asia/Taipei'))
            price_change_pct = None
            if price_indicators_15m["MA34"][-1] is not None and price_indicators_15m["MA34"][-1] != 0:
                price_change_pct = (current_price_15m - price_indicators_15m["MA34"][-1]) / price_indicators_15m["MA34"][-1] * 100
                price_change_pct = round(price_change_pct, 2)
            previous_day_amplitude = calculate_previous_day_amplitude(klines_15m, taipei_time)
            
            open_time_15m = timestamp_to_taipei(klines_15m[-1][0])
            row_data_15m = [
                open_time_15m,
                trading_pair,
                ", ".join(macd_signal_types),
                current_price_15m,
                price_indicators_15m["MA21"][-1],
                price_indicators_15m["MA34"][-1],
                price_indicators_15m["MA233"][-1],
                volume_indicators_15m["quote_volume"][-1],
                volume_indicators_15m["VOL8"][-1],
                volume_indicators_15m["VOL21"][-1],
                "", "",
                price_change_pct if price_change_pct is not None else "",
                previous_day_amplitude if previous_day_amplitude is not None else "",
                price_indicators_15m["MA34"][-2] if len(price_indicators_15m["MA34"]) >= 2 and price_indicators_15m["MA34"][-2] is not None else "",
                price_indicators_15m["MA233"][-2] if len(price_indicators_15m["MA233"]) >= 2 and price_indicators_15m["MA233"][-2] is not None else ""
            ]
            update_sheet_with_retry(sheet_client, SPREADSHEET_ID, "15min", row_data_15m)
            new_entries += 1
            
            signal_message = f"{open_time_15m} - {trading_pair}: {', '.join(macd_signal_types)}"
            if price_change_pct is not None:
                signal_message += f"\n價差比: {price_change_pct}%"
            if previous_day_amplitude is not None:
                signal_message += f"\n前日振幅: {previous_day_amplitude}%"
            send_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, signal_message)
        
        logger.info(f"完成交易對 {trading_pair} 的 MACD 檢查，等待 0.5 秒")
        time.sleep(0.5)
    
    trading_pairs = get_trading_pairs()
    total_pairs = len(trading_pairs)
    logger.info(f"開始處理 {total_pairs} 個交易對，預估耗時 {total_pairs * 2 // 60} 分鐘")
    
    batch_size = 60
    for batch_start in range(0, total_pairs, batch_size):
        batch_end = min(batch_start + batch_size, total_pairs)
        logger.info(f"處理批次 {batch_start + 1} 到 {batch_end} / {total_pairs}")
        for i, trading_pair in enumerate(trading_pairs[batch_start:batch_end], batch_start + 1):
            logger.info(f"處理進度: {i}/{total_pairs} 交易對")
            process_trading_pair(trading_pair, sheet_client)
            time.sleep(0.5)
        time.sleep(10)
    
    if new_entries > 0:
        try:
            cleanup_old_data(sheet_client, SPREADSHEET_ID, "15min", new_entries)
        except Exception as e:
            logger.error(f"清理 '15min' 舊數據失敗: {e}")
    
    end_time = datetime.now(pytz.timezone('Asia/Taipei'))
    logger.info(f"完成 main_task (Run {run_count}): {end_time.strftime('%Y-%m-%d %H:%M:%S')}, 耗時 {(end_time - start_time).total_seconds()} 秒, 新增 {new_entries} 筆記錄")

if __name__ == "__main__":
    logger.info(f"程式啟動，artifact_id: {ARTIFACT_ID}, version: {ARTIFACT_VERSION}")
    test_telegram_message(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
    sheet_client = setup_sheet_client(GOOGLE_SHEET_CREDS_JSON)
    if sheet_client:
        test_google_sheet_update(sheet_client, SPREADSHEET_ID)
    else:
        logger.error("無法設置 Google Sheet 客戶端，無法進行更新測試")
    
    schedule.every().hour.at(":05").do(main_task)
    schedule.every().hour.at(":20").do(main_task)
    schedule.every().hour.at(":35").do(main_task)
    schedule.every().hour.at(":50").do(main_task)
    
    main_task()
    
    last_heartbeat = time.time()
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
            if time.time() - last_heartbeat >= 60:
                logger.info("程式運行中，心跳檢查")
                last_heartbeat = time.time()
        except Exception as e:
            logger.error(f"排程執行錯誤: {e}")
            time.sleep(60)
