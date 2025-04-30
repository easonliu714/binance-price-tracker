import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 設定日誌記錄
logger = logging.getLogger(__name__)

# Binance API 備用端點
API_ENDPOINTS = [
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com"
]

def create_session():
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 451, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    })
    return session
def get_trading_pairs():
    """
    從 Binance API 獲取所有 USDT 交易對，支援備用端點和重試
    """
    try:
        logger.info("開始獲取 USDT 交易對")
        session = create_session()
        
        for base_url in API_ENDPOINTS:
            url = f"{base_url}/api/v3/exchangeInfo"
            try:
                response = session.get(url, timeout=10)
                response.raise_for_status()

                if 'application/json' not in response.headers.get('Content-Type', ''):
                    logger.error(f"端點 {base_url} 回應非 JSON 格式: {response.text}")
                    continue

                data = response.json()
                usdt_pairs = [
                    symbol["symbol"]
                    for symbol in data["symbols"]
                    if symbol["symbol"].endswith("USDT") and symbol["status"] == "TRADING"
                ]

                logger.info(f"從 {base_url} 成功獲取 {len(usdt_pairs)} 個 USDT 交易對")
                return usdt_pairs

            except requests.exceptions.RequestException as e:
                logger.warning(f"端點 {base_url} 獲取交易對失敗: {e}, 狀態碼: {response.status_code if 'response' in locals() else '未知'}, 回應: {response.text if 'response' in locals() else '無'}")
                if base_url == API_ENDPOINTS[-1]:
                    logger.error("所有備用端點均無法獲取交易對")
                    return []
                continue

    except Exception as e:
        logger.error(f"獲取交易對時發生未知錯誤: {e}")
        return []

def get_klines(symbol, interval="5m", limit=500):
    """
    從 Binance API 獲取 K 線數據，支援備用端點和重試
    """
    try:
        logger.info(f"獲取 {symbol} 的 {interval} K 線數據")
        session = create_session()
        
        for base_url in API_ENDPOINTS:
            url = f"{base_url}/api/v3/klines"
            params = {
                "symbol": symbol,
                "interval": interval,
                "limit": limit
            }
            try:
                response = session.get(url, params=params, timeout=10)
                response.raise_for_status()

                if 'application/json' not in response.headers.get('Content-Type', ''):
                    logger.error(f"端點 {base_url} 回應非 JSON 格式: {response.text}")
                    continue

                klines = response.json()
                if not klines or not isinstance(klines, list):
                    logger.warning(f"從 {base_url} 獲取 {symbol} K 線返回空數據或格式不正確")
                    continue

                logger.info(f"從 {base_url} 成功獲取 {symbol} 的 {len(klines)} 條 K 線數據")
                return klines

            except requests.exceptions.RequestException as e:
                logger.warning(f"端點 {base_url} 獲取 {symbol} K 線數據失敗: {e}, 狀態碼: {response.status_code if 'response' in locals() else '未知'}, 回應: {response.text if 'response' in locals() else '無'}")
                if base_url == API_ENDPOINTS[-1]:
                    logger.error(f"所有備用端點均無法獲取 {symbol} K 線數據")
                    return []
                continue

    except Exception as e:
        logger.error(f"獲取 {symbol} K 線數據時發生未知錯誤: {e}")
        return []

def get_current_price(symbol):
    """
    從 Binance API 獲取當前價格，支援備用端點和重試
    """
    try:
        logger.info(f"獲取 {symbol} 的當前價格")
        session = create_session()
        
        for base_url in API_ENDPOINTS:
            url = f"{base_url}/api/v3/ticker/price"
            params = {"symbol": symbol}
            try:
                response = session.get(url, params=params, timeout=10)
                response.raise_for_status()

                if 'application/json' not in response.headers.get('Content-Type', ''):
                    logger.error(f"端點 {base_url} 回應非 JSON 格式: {response.text}")
                    continue

                data = response.json()
                price = float(data["price"])

                logger.info(f"從 {base_url} 獲取 {symbol} 當前價格: {price}")
                return price

            except requests.exceptions.RequestException as e:
                logger.warning(f"端點 {base_url} 獲取 {symbol} 價格失敗: {e}, 狀態碼: {response.status_code if 'response' in locals() else '未知'}, 回應: {response.text if 'response' in locals() else '無'}")
                if base_url == API_ENDPOINTS[-1]:
                    logger.error(f"所有備用端點均無法獲取 {symbol} 價格")
                    return None
                continue

    except Exception as e:
        logger.error(f"獲取 {symbol} 價格時發生未知錯誤: {e}")
        return None
