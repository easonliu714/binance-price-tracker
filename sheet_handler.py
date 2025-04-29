import logging
import json
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime


# 設定日誌記錄
logger = logging.getLogger(__name__)

def setup_sheet_client(credentials_json_str):
    """
    設置Google Sheet客戶端
    
    Args:
        credentials_json_str (str): Google Service Account憑證JSON字符串
        
    Returns:
        gspread.Client: Google Sheet客戶端
    """
    try:
        logger.info("設置Google Sheet客戶端")
        
        # 解析憑證字符串為JSON
        credentials_info = json.loads(credentials_json_str)
        
        # 設置必要的OAuth2範圍
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]
        
        # 從憑證創建認證對象
        credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
        
        # 創建gspread客戶端
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
    
    Args:
        client (gspread.Client): Google Sheet客戶端
        spreadsheet_id (str): Google Sheet ID
        sheet_name (str): 工作表名稱
        row_data (list): 要添加的行數據
        
    Returns:
        bool: 是否成功更新
    """
    try:
        logger.info(f"更新Google Sheet {spreadsheet_id}, 工作表 {sheet_name}")
        
        # 打開電子表格
        spreadsheet = client.open_by_key(spreadsheet_id)
        
        # 獲取或創建工作表
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            logger.info(f"工作表 {sheet_name} 不存在，創建新工作表")
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1, cols=len(row_data))
            
            # 設置標題行
            headers = [
                "時間", "交易對", "當前價格", "MA17", "MA175", "MA425", 
                "當前交易量", "VOL7", "VOL17", "觸發條件"
            ]
            worksheet.update('A1:J1', [headers])
        
        # 添加新行
        worksheet.append_row(row_data)
        
        logger.info(f"成功更新Google Sheet")
        return True
        
    except Exception as e:
        logger.error(f"更新Google Sheet時發生錯誤: {e}")
        return False

def cleanup_old_data(client, spreadsheet_id, sheet_name, max_rows=10000):
    """
    清理舊數據，保持表格行數不超過max_rows
    
    Args:
        client (gspread.Client): Google Sheet客戶端
        spreadsheet_id (str): Google Sheet ID
        sheet_name (str): 工作表名稱
        max_rows (int): 最大行數
        
    Returns:
        bool: 是否成功清理
    """
    try:
        logger.info(f"檢查是否需要清理舊數據，最大行數限制: {max_rows}")
        
        # 打開電子表格
        spreadsheet = client.open_by_key(spreadsheet_id)
        worksheet = spreadsheet.worksheet(sheet_name)
        
        # 獲取當前行數
        all_values = worksheet.get_all_values()
        current_rows = len(all_values)
        
        if current_rows <= max_rows:
            logger.info(f"當前行數 {current_rows} 未超過限制 {max_rows}，無需清理")
            return True
        
        # 計算需要刪除的行數
        rows_to_delete = current_rows - max_rows
        
        logger.info(f"當前行數 {current_rows}，需要刪除 {rows_to_delete} 行舊數據")
        
        # 刪除最舊的數據行（從第2行開始，保留標題行）
        if rows_to_delete > 0:
            worksheet.delete_rows(2, rows_to_delete + 1)  # 2是起始行（第一行為標題），加1是因為gspread範圍是包含的
            
            logger.info(f"成功刪除 {rows_to_delete} 行舊數據")
            
        return True
        
    except Exception as e:
        logger.error(f"清理舊數據時發生錯誤: {e}")
        return False
