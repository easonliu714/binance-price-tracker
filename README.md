# 幣安價格追蹤器

這是一個使用 Google Cloud Run 部署的幣安加密貨幣價格追蹤器。它會監控 USDT 交易對的價格變動，並根據移動平均線發出交易信號。

## 功能

- 從幣安公開 API 擷取 USDT 交易對
- 計算 175 小時和 425 小時移動平均線
- 根據價格與移動平均線的關係發出交易信號
- 將信號發送到 Telegram
- 將數據保存到 Google Sheets
- 通過 Cloud Scheduler 每 5 分鐘自動執行

## 部署

請參考專案文檔了解如何在 Google Cloud Run 上部署此應用。
