import logging
import requests

# 設定日誌記錄
logger = logging.getLogger(__name__)

def send_telegram_message(token, chat_id, message):
    """
    發送訊息到Telegram
    
    Args:
        token (str): Telegram Bot Token
        chat_id (str): Telegram Chat ID
        message (str): 要發送的訊息
        
    Returns:
        bool: 訊息是否成功發送
    """
    try:
        logger.info(f"準備發送Telegram訊息: {message[:50]}...")
        
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        
        data = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML"
        }
        
        response = requests.post(url, data=data)
        
        if response.status_code == 200:
            logger.info("Telegram訊息發送成功")
            return True
        else:
            logger.error(f"Telegram訊息發送失敗: {response.status_code} - {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        logger.error(f"發送Telegram訊息時發生網路錯誤: {e}")
        return False
    except Exception as e:
        logger.error(f"發送Telegram訊息時發生錯誤: {e}")
        return False

