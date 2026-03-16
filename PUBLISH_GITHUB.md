# GitHub 发布说明（纯网页版）

目标仓库：`https://github.com/Jiangzy520/DreamleStockRadar`

## 推荐仓库信息

- Name: `DreamleStockRadar`
- Description: `A-share realtime scanner and web push dashboard powered by AllTick.`
- Topics: `a-share`, `stock`, `scanner`, `alltick`, `flask`, `python`, `dashboard`, `trading-tools`

## 本地发布命令

```bash
cd /home/jzy/桌面/guanlan-quant
git status
git add .
git commit -m "refactor: slim repository to web-only Dreamle Stock Radar"
git push origin main
```

## 首个公开 Release（建议）

Tag: `v0.1.0`  
Title: `Dreamle Stock Radar v0.1.0`

Release Notes:

```text
- Converted repository to web-only edition
- Added realtime push dashboard for A-share scanning
- Added image-based watchlist OCR and push snapshot generation
- Removed desktop/client modules and unrelated legacy directories
- Preserved deployment scripts for web service and scanner workers
```
