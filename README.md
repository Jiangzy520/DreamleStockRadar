# GitHub Public Demo

这是从私有生产项目中整理出来的一份 GitHub 安全公开版，用来配合论坛发帖和产品展示。

这个公开版保留了什么：
- 一个轻量 Flask 网站壳子
- 一个适合展示的公开演示页面
- 通用演示 API 返回
- 最小运行脚本和依赖文件

这个公开版移除了什么：
- 生产策略代码
- 策略定义、触发规则和参数细节
- 私有数据源接入和服务端扫描逻辑
- 服务器专用配置、运行数据、密钥和接口地址

如果你是从论坛帖子点进来的，可以先看这些文档：
- [论坛发布说明](./docs/FORUM_RELEASE_GUIDE.md)
- [论坛配图清单](./docs/SCREENSHOT_CHECKLIST.md)
- [架构说明](./docs/ARCHITECTURE_OVERVIEW.md)
- [股票模拟盘自动化](./docs/STOCK_PAPER_TRADING_AUTOMATION.md)
- [多源数据输入说明](./docs/MARKET_DATA_INPUTS.md)

## 目录说明

```text
webapp/
  server.py
  templates/push.html
docs/
  FORUM_RELEASE_GUIDE.md
  SCREENSHOT_CHECKLIST.md
  ARCHITECTURE_OVERVIEW.md
requirements.txt
start_guanlan_web.sh
LICENSE
```

## 本地运行

```bash
python -m pip install -r requirements.txt
python webapp/server.py --host 127.0.0.1 --port 8768
```

打开：

```text
http://127.0.0.1:8768/push
```

## 公开版定位

这不是线上生产系统的完整开源版本，而是一份适合公开分享的演示仓库，主要用于：
- 配合论坛帖子展示产品形态
- 展示网站结构和前端页面
- 讲解多源输入、监控面板和模拟盘自动化的整体思路

## 股票模拟盘自动化

股票模拟盘自动化这一层，主要解决的是统一信号消费、去重过滤、模拟账户状态读取、下单约束判断和订单状态回写，而不是公开具体策略细节。

详细说明见：
- [股票模拟盘自动化](./docs/STOCK_PAPER_TRADING_AUTOMATION.md)

## 多源数据输入

公开版里可以安全分享的是三路输入的角色分工、接入要求和工程取舍，而不是私有配置或生产环境参数。

详细说明见：
- [多源数据输入说明](./docs/MARKET_DATA_INPUTS.md)

如果后续你准备继续完善公开仓库，建议优先补这几类内容：
- 页面截图
- 架构图
- 论坛帖子链接
- 演示视频或 GIF
