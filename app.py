import os
from flask import Flask, request
import main
import logging

app = Flask(__name__)

@app.route('/', methods=['GET'])
def home():
    return "幣安價格追蹤器正在運行中！"

@app.route('/run', methods=['POST'])
def run_tracker():
    try:
        result = main.main(request)
        return result, 200
    except Exception as e:
        logging.error(f"執行追蹤任務時發生錯誤: {str(e)}")
        return f"執行錯誤: {str(e)}", 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
