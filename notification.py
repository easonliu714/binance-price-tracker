import logging
import requests

# 設定日誌記錄
logger = logging.getLogger(__name__)

def send_telegram_message(token, chat_id, message):
    """
    發送訊息到Telegram
    """
    try:
        logger.info(f"準備發送Telegram訊息: {message[:50]}...")

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML"
        }

        response = requests.post(url, data=data, timeout=10)
        response.raise_for_status()

        if 'application/json' not in response.headers.get('Content-Type', ''):
            logger.error("回應內容不是JSON格式")
            return False

        result = response.json()
        if not result.get('ok'):
            logger.error(f"Telegram訊息發送失敗: {result}")
            return False

        logger.info("Telegram訊息發送成功")
        return True

    except requests.exceptions.RequestException as e:
        logger.error(f"發送Telegram訊息時發生網路錯誤: {e}")
        return False
    except Exception as e:
        logger.error(f"發送Telegram訊息時發生錯誤: {e}")
        return False

