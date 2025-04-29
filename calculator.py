import logging
import numpy as np

# 設定日誌記錄
logger = logging.getLogger(__name__)

def calculate_moving_average(data, window):
    """
    計算移動平均
    
    Args:
        data (list): 數據列表
        window (int): 窗口大小
        
    Returns:
        list: 移動平均列表
    """
    try:
        if len(data) < window:
            logger.warning(f"數據長度 {len(data)} 小於窗口大小 {window}，無法計算移動平均")
            return [None] * len(data)
            
        # 使用numpy計算移動平均
        ret = np.convolve(data, np.ones(window) / window, mode='valid')
        
        # 前綴補充None值，使結果長度與輸入數據相同
        padding = [None] * (len(data) - len(ret))
        result = padding + ret.tolist()
        
        return result
        
    except Exception as e:
        logger.error(f"計算移動平均時發生錯誤: {e}")
        return [None] * len(data)

def calculate_price_indicators(klines):
    """
    計算價格相關指標 (使用1小時K線)
    
    Args:
        klines (list): K線數據列表
        
    Returns:
        dict: 包含價格指標的字典
    """
    try:
        logger.info("開始計算價格指標")
        
        if not klines or len(klines) < 425:  # 確保有足夠的數據計算MA425
            logger.warning(f"K線數據不足，無法計算價格指標: 當前數量 {len(klines)}")
            return None
        
        # 提取收盤價
        close_prices = [float(kline[4]) for kline in klines]  # 第4個元素是收盤價
        
        # 計算價格移動平均
        ma17 = calculate_moving_average(close_prices, 17)
        ma175 = calculate_moving_average(close_prices, 175)
        ma425 = calculate_moving_average(close_prices, 425)
        
        # 驗證計算結果
        if None in [ma17[-1], ma175[-1], ma425[-1]]:
            logger.warning("部分價格指標計算結果為None")
            
        logger.info(f"價格指標計算完成，最後一根K線指標: MA17={ma17[-1]:.8f}, MA175={ma175[-1]:.8f}, MA425={ma425[-1]:.8f}")
        
        return {
            "close": close_prices,
            "MA17": ma17,
            "MA175": ma175,
            "MA425": ma425
        }
        
    except Exception as e:
        logger.error(f"計算價格指標時發生錯誤: {e}")
        return None

def calculate_volume_indicators(klines):
    """
    計算交易量相關指標 (使用5分鐘K線)
    
    Args:
        klines (list): K線數據列表
        
    Returns:
        dict: 包含交易量指標的字典
    """
    try:
        logger.info("開始計算交易量指標")
        
        if not klines or len(klines) < 17:  # 確保有足夠的數據計算VOL17
            logger.warning(f"K線數據不足，無法計算交易量指標: 當前數量 {len(klines)}")
            return None
        
        # 提取交易量
        volumes = [float(kline[5]) for kline in klines]  # 第5個元素是交易量
        
        # 計算交易量移動平均
        vol7 = calculate_moving_average(volumes, 7)
        vol17 = calculate_moving_average(volumes, 17)
        
        # 驗證計算結果
        if None in [vol7[-1], vol17[-1]]:
            logger.warning("部分交易量指標計算結果為None")
            
        logger.info(f"交易量指標計算完成: VOL7={vol7[-1]:.2f}, VOL17={vol17[-1]:.2f}")
        
        return {
            "volume": volumes,
            "VOL7": vol7,
            "VOL17": vol17
        }
        
    except Exception as e:
        logger.error(f"計算交易量指標時發生錯誤: {e}")
        return None

