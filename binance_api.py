import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_usdt_pairs():
    logger.info("開始獲取USDT交易對")
    try:
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 451, 500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))
        response = session.get("https://api.binance.com/api/v3/exchangeInfo", timeout=10)
        response.raise_for_status()
        data = response.json()
        pairs = [s["symbol"] for s in data["symbols"] if s["quoteAsset"] == "USDT"]
        logger.info(f"獲取到 {len(pairs)} 個USDT交易對")
        return pairs
    except Exception as e:
        logger.error(f"獲取交易對時發生網路錯誤: {e}, 狀態碼: {response.status_code if 'response' in locals() else '未知'}, 回應: {response.text if 'response' in locals() else '無'}")
        raise
