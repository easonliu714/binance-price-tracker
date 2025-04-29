import logging
import json
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import time

# 設定日誌記錄
logger = logging.getLogger(__name__)

def setup_sheet_client(credentials_json_str):
    """
    設置Google Sheet客戶端
    """
    try:
        logger.info("設置Google Sheet客戶端")

        credentials_info = json.loads(credentials_json_str)
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
        client = gspread.authorize(credentials)

        logger.info("Google Sheet客戶端設置成功")
        return client

    except json.JSONDecodeError as e:
        logger.error(f"解析憑證JSON時發生錯誤: {e}")
        return None
    except Exception as e:
        logger.error(f"設置Google Sheet客戶端時發生錯誤: {e}")
        return None

def update_sheet(client, spreadsheet_id, sheet_name, row_data):
    """
    更新Google Sheet
    """
    max_retries = 3
    retry_delay = 2  # 秒

    for attempt in range(max_retries):
        try:
            logger.info(f"更新Google Sheet {spreadsheet_id}, 工作表 {sheet_name}")

            spreadsheet = client.open_by_key(spreadsheet_id)
            try:
                worksheet = spreadsheet.worksheet(sheet_name)
            except gspread.exceptions.WorksheetNotFound:
                logger.info(f"工作表 {sheet_name} 不存在，創建新工作表")
                worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1, cols=len(row_data))
                headers = [
                    "時間", "交易對", "當前價格", "MA17", "MA175", "MA425",
                    "當前交易量", "VOL7", "VOL17", "觸發條件"
                ]
                worksheet.update('A1:J1', [headers])

            worksheet.append_row(row_data)
            logger.info(f"成功更新Google Sheet")
            return True

        except Exception as e:
            logger.error(f"更新Google Sheet時發生錯誤 (嘗試 {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            continue

    logger.error("更新Google Sheet失敗，已達最大重試次數")
    return False

def cleanup_old_data(client, spreadsheet_id, sheet_name, max_rows=10000):
    """
    清理舊數據，保持表格行數不超過max_rows
    """
    max_retries = 3
    retry_delay = 2  # 秒

    for attempt in range(max_retries):
        try:
            logger.info(f"檢查是否需要清理舊數據， 최대行數限制: {max_rows}")

            spreadsheet = client.open_by_key(spreadsheet_id)
            worksheet = spreadsheet.worksheet(sheet_name)
            all_values = worksheet.get_all_values()
            current_rows = len(all_values)

            if current_rows <= max_rows:
                logger.info(f"當前行數 {current_rows} 未超過限制 {max_rows}，無需清理")
                return True

            rows_to_delete = current_rows - max_rows
            logger.info(f"當前行數 {current_rows}，需要刪除 {rows_to_delete} 行舊數據")

            if rows_to_delete > 0:
                worksheet.delete_rows(2, rows_to_delete + 1)
                logger.info(f"成功刪除 {rows_to_delete} 行舊數據")

            return True

        except Exception as e:
            logger.error(f"清理舊數據時發生錯誤 (嘗試 {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            continue

    logger.error("清理舊數據失敗，已達最大重試次數")
    return False

