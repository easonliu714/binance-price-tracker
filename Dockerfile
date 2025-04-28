# 使用 Python 3.9 slim 作為基礎映像
FROM python:3.9-slim

# 設定工作目錄
WORKDIR /app

# 複製 requirements.txt 並安裝依賴
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 複製所有檔案到容器中
COPY . .

# 設定環境變數
ENV PORT=8080

# 使用 Shell 格式的 CMD 指令
CMD gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 app:app

