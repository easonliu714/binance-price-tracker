import logging
     import requests
     from requests.adapters import HTTPAdapter
     from urllib3.util.retry import Retry

     logger = logging.getLogger(__name__)

     FUTURES_API_URL = "https://fapi.binance.com"
     PROXY_URL = "http://220.132.41.160:1088"  # 替換為您測試成功的代理 URL

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
         session.proxies = {
             "http": PROXY_URL,
             "https": PROXY_URL
         }
         return session

     def get_trading_pairs():
         try:
             logger.info("開始獲取 USDT 永續合約交易對")
             session = create_session()
             url = f"{FUTURES_API_URL}/fapi/v1/exchangeInfo"
             response = session.get(url, timeout=10)
             logger.info(f"API 回應狀態碼: {response.status_code}, 回應內容: {response.text[:200]}")
             response.raise_for_status()
             if 'application/json' not in response.headers.get('Content-Type', ''):
                 logger.error(f"端點 {url} 回應非 JSON 格式: {response.text}")
                 return []
             data = response.json()
             usdt_pairs = [
                 symbol["symbol"]
                 for symbol in data["symbols"]
                 if symbol["quoteAsset"] == "USDT" and symbol["status"] == "TRADING" and symbol["contractType"] == "PERPETUAL"
             ]
             logger.info(f"從 {url} 成功獲取 {len(usdt_pairs)} 個 USDT 永續合約交易對")
             return usdt_pairs
         except requests.exceptions.RequestException as e:
             logger.error(f"獲取交易對失敗: {e}, 狀態碼: {response.status_code if 'response' in locals() else '未知'}, 回應: {response.text if 'response' in locals() else '無'}")
             return []
         except Exception as e:
             logger.error(f"獲取交易對時發生未知錯誤: {e}")
             return []

     def get_klines(symbol, interval="5m", limit=430):
         try:
             logger.info(f"獲取 {symbol} 的 {interval} K 線數據")
             session = create_session()
             url = f"{FUTURES_API_URL}/fapi/v1/klines"
             params = {
                 "symbol": symbol,
                 "interval": interval,
                 "limit": limit
             }
             response = session.get(url, params=params, timeout=10)
             logger.info(f"API 回應狀態碼: {response.status_code}, 回應內容: {response.text[:200]}")
             response.raise_for_status()
             if 'application/json' not in response.headers.get('Content-Type', ''):
                 logger.error(f"端點 {url} 回應非 JSON 格式: {response.text}")
                 return []
             klines = response.json()
             if not klines or not isinstance(klines, list):
                 logger.warning(f"從 {url} 獲取 {symbol} K 線返回空數據或格式不正確")
                 return []
             logger.info(f"從 {url} 成功獲取 {symbol} 的 {len(klines)} 條 K 線數據")
             return klines
         except requests.exceptions.RequestException as e:
             logger.error(f"獲取 {symbol} K 線數據失敗: {e}, 狀態碼: {response.status_code if 'response' in locals() else '未知'}, 回應: {response.text if 'response' in locals() else '無'}")
             return []
         except Exception as e:
             logger.error(f"獲取 {symbol} K 線數據時發生未知錯誤: {e}")
             return []

     def get_current_price(symbol):
         try:
             logger.info(f"獲取 {symbol} 的當前價格")
             session = create_session()
             url = f"{FUTURES_API_URL}/fapi/v1/ticker/price"
             params = {"symbol": symbol}
             response = session.get(url, params=params, timeout=10)
             logger.info(f"API 回應狀態碼: {response.status_code}, 回應內容: {response.text[:200]}")
             response.raise_for_status()
             if 'application/json' not in response.headers.get('Content-Type', ''):
                 logger.error(f"端點 {url} 回應非 JSON 格式: {response.text}")
                 return None
             data = response.json()
             price = float(data["price"])
             logger.info(f"從 {url} 獲取 {symbol} 當前價格: {price}")
             return price
         except requests.exceptions.RequestException as e:
             logger.error(f"獲取 {symbol} 價格失敗: {e}, 狀態碼: {response.status_code if 'response' in locals() else '未知'}, 回應: {response.text if 'response' in locals() else '無'}")
             return None
         except Exception as e:
             logger.error(f"獲取 {symbol} 價格時發生未知錯誤: {e}")
             return None
