# 使用官方 Python 3.11 映像
FROM python:3.11-slim

# 設定工作目錄
WORKDIR /app

# 複製需求檔與程式碼
COPY requirements.txt requirements.txt
COPY . .

# 安裝必要套件
RUN pip install --no-cache-dir -r requirements.txt

# 設定環境變數（可覆蓋）
ENV PORT=8080

# 啟動 Flask 應用（使用 Gunicorn）
CMD ["gunicorn", "-b", "0.0.0.0:8080", "main:app"]
