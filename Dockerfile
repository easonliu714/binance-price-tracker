# 使用多階段構建
FROM python:3.11-slim AS builder

# 設定工作目錄
WORKDIR /app

# 安裝構建依賴
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 複製需求檔並安裝依賴
COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

# 最終階段
FROM python:3.11-slim

# 建立非 root 使用者
RUN useradd -m appuser

# 設定工作目錄
WORKDIR /app

# 從 builder 階段複製已安裝的依賴
COPY --from=builder /root/.local /home/appuser/.local

# 複製應用程式碼
COPY main.py binance_api.py calculator.py notification.py sheet_handler.py ./

# 設定環境變數
ENV PATH=/home/appuser/.local/bin:$PATH
ENV PORT=8080
ENV PYTHONUNBUFFERED=1

# 更改檔案擁有者
RUN chown -R appuser:appuser /app

# 切換到非 root 使用者
USER appuser

# 啟動 Gunicorn
CMD ["gunicorn", "-b", "0.0.0.0:8080", "--workers=2", "--threads=4", "main:app"]
