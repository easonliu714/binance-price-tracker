from flask import Flask
from notification import send_telegram_message
from sheet_handler import setup_sheet_client, update_sheet
from binance_api import get_usdt_pairs
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

def analyze_pairs():
    try:
        pairs = get_usdt_pairs()
        # 假設的分析邏輯
        results = [[p, "test", 0, 0, 0, 0, 0, 0, 0, "test"] for p in pairs[:1]]  # 示例數據
        return results
    except Exception as e:
        logger.error(f"Analyze pairs failed: {e}")
        return []

def format_results(results):
    return "\n".join([f"Pair: {r[0]}" for r in results])

def main_task():
    try:
        logger.info(f"TELEGRAM_TOKEN: {os.environ.get('TELEGRAM_TOKEN')}")
        logger.info(f"TELEGRAM_CHAT_ID: {os.environ.get('TELEGRAM_CHAT_ID')}")
        logger.info(f"GOOGLE_SHEET_CREDS_JSON: {os.environ.get('GOOGLE_SHEET_CREDS_JSON')[:50]}...")
        logger.info("Starting main_task")
        results = analyze_pairs()
        logger.info(f"Analysis results: {results}")
        if results:
            message = format_results(results)
            logger.info(f"Sending Telegram message: {message}")
            send_telegram_message(
                os.environ['TELEGRAM_TOKEN'],
                os.environ['TELEGRAM_CHAT_ID'],
                message
            )
            logger.info("Setting up Google Sheets client")
            client = setup_sheet_client(os.environ['GOOGLE_SHEET_CREDS_JSON'])
            logger.info("Updating Google Sheet")
            update_sheet(client, '1Bny_4th50YM2mKSTZDbH7Zqd9Uhl6PHMCCveFMgqMrE', 'Sheet1', results[0])
        else:
            logger.warning("No results to process")
            send_telegram_message(
                os.environ['TELEGRAM_TOKEN'],
                os.environ['TELEGRAM_CHAT_ID'],
                "No trading pairs available due to API error"
            )
    except Exception as e:
        logger.error(f"Error in main_task: {e}")
        send_telegram_message(
            os.environ['TELEGRAM_TOKEN'],
            os.environ['TELEGRAM_CHAT_ID'],
            f"Error in main_task: {e}"
        )
        raise

@app.route('/run')
def run():
    try:
        main_task()
        return "Analysis complete"
    except Exception as e:
        logger.error(f"Error in /run endpoint: {e}")
        return f"Error: {e}", 500

@app.route('/test-telegram')
def test_telegram():
    try:
        logger.info(f"TELEGRAM_TOKEN: {os.environ.get('TELEGRAM_TOKEN')}")
        logger.info(f"TELEGRAM_CHAT_ID: {os.environ.get('TELEGRAM_CHAT_ID')}")
        send_telegram_message(
            os.environ['TELEGRAM_TOKEN'],
            os.environ['TELEGRAM_CHAT_ID'],
            "Test message from Cloud Run"
        )
        logger.info("Test Telegram message sent")
        return "Telegram message sent"
    except Exception as e:
        logger.error(f"Error: {e}")
        return f"Error: {e}", 500

@app.route('/test-sheet')
def test_sheet():
    try:
        logger.info(f"GOOGLE_SHEET_CREDS_JSON: {os.environ.get('GOOGLE_SHEET_CREDS_JSON')[:50]}...")
        client = setup_sheet_client(os.environ['GOOGLE_SHEET_CREDS_JSON'])
        update_sheet(client, '1Bny_4th50YM2mKSTZDbH7Zqd9Uhl6PHMCCveFMgqMrE', 'Sheet1', ['test', 'test', 0, 0, 0, 0, 0, 0, 0, 'test'])
        logger.info("Test sheet updated")
        return "Sheet updated"
    except Exception as e:
        logger.error(f"Error: {e}")
        return f"Error: {e}", 500
