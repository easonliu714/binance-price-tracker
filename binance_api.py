import logging
import requests
from datetime import datetime

# 設定日誌記錄
logger = logging.getLogger(__name__)

# 幣安API基本URL
BASE_URL = "https://api.binance.com"

def get_trading_pairs():
    """
    從幣安API獲取所有USDT交易對
    
    Returns:
        list: USDT交易對列表
    """
    try:
        logger.info("開始獲取USDT交易對")
        url = f"{BASE_URL}/api/v3/exchangeInfo"
        
        response = requests.get(url)
        response.raise_for_status()  # 檢查請求是否成功
        
        data = response.json()
        
        # 篩選出USDT結尾的交易對
        usdt_pairs = [
            symbol["symbol"] 
            for symbol in data["symbols"] 
            if symbol["symbol"].endswith("USDT") and symbol["status"] == "TRADING"
        ]
        
        logger.info(f"成功獲取 {len(usdt_pairs)} 個USDT交易對")
        return usdt_pairs
        
    except requests.exceptions.RequestException as e:
        logger.error(f"獲取交易對時發生網路錯誤: {e}")
        return []
    except Exception as e:
        logger.error(f"獲取交易對時發生錯誤: {e}")
        return []

def get_klines(symbol, interval="5m", limit=500):
    """
    從幣安API獲取K線數據
    
    Args:
        symbol (str): 交易對，如 BTCUSDT
        interval (str): K線間隔，默認5分鐘
        limit (int): 返回的K線數量，默認500
        
    Returns:
        list: K線數據列表
    """
    try:
        logger.info(f"獲取 {symbol} 的 {interval} K線數據")
        url = f"{BASE_URL}/api/v3/klines"
        
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit
        }
        
        response = requests.get(url, params=params)
        response.raise_for_status()  # 檢查請求是否成功
        
        klines = response.json()
        
        # 驗證數據格式
        if not klines or not isinstance(klines, list):
            logger.warning(f"獲取 {symbol} K線返回空數據或格式不正確")
            return []
            
        logger.info(f"成功獲取 {symbol} 的 {len(klines)} 條K線數據")
        return klines
        
    except requests.exceptions.RequestException as e:
        logger.error(f"獲取 {symbol} K線數據時發生網路錯誤: {e}")
        return []
    except Exception as e:
        logger.error(f"獲取 {symbol} K線數據時發生錯誤: {e}")
        return []

def get_current_price(symbol):
    """
    獲取當前價格
    
    Args:
        symbol (str): 交易對，如 BTCUSDT
        
    Returns:
        float: 當前價格
    """
    try:
        logger.info(f"獲取 {symbol} 的當前價格")
        url = f"{BASE_URL}/api/v3/ticker/price"
        
        params = {
            "symbol": symbol
        }
        
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        data = response.json()
        price = float(data["price"])
        
        logger.info(f"{symbol} 當前價格: {price}")
        return price
        
    except requests.exceptions.RequestException as e:
        logger.error(f"獲取 {symbol} 價格時發生網路錯誤: {e}")
        return None
    except Exception as e:
        logger.error(f"獲取 {symbol} 價格時發生錯誤: {e}")
        return None
