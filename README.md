# GitHub Public Demo

这是从私有生产项目中整理出来的一份 GitHub 安全公开版，用于产品展示、结构说明和公开演示。

这份公开版保留了：
- 一个轻量级 Flask 网站壳子
- 一个适合展示的公开页面
- 通用演示接口返回
- 最小运行脚本和基础依赖

这份公开版移除了：
- 生产策略代码
- 策略定义、触发条件和参数细节
- 私有数据源接入和服务端扫描逻辑
- 服务器专用配置、运行数据、密钥和内部接口地址

## 文档导航

- [环境准备：下载 VS Code 并安装 Codex 扩展](./docs/VSCODE_CODEX_SETUP.md)
- [股票、期货实时数据获取说明](./docs/STOCK_FUTURES_REALTIME_DATA.md)
- [股票模拟盘自动化](./docs/STOCK_PAPER_TRADING_AUTOMATION.md)
- [多源数据输入说明](./docs/MARKET_DATA_INPUTS.md)

## 目录说明

```text
webapp/
  server.py
  templates/push.html
docs/
  VSCODE_CODEX_SETUP.md
  STOCK_FUTURES_REALTIME_DATA.md
  STOCK_PAPER_TRADING_AUTOMATION.md
  MARKET_DATA_INPUTS.md
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
- 展示网站结构和页面形态
- 说明多源输入、监控面板和自动化链路的整体思路
- 作为公开演示、产品说明和后续文档扩展的仓库入口

## 股票模拟盘自动化

股票模拟盘自动化这一层，主要解决的是统一信号消费、去重过滤、模拟账户状态读取、下单约束判断和订单状态回写，而不是公开具体策略细节。

详细说明见：
- [股票模拟盘自动化](./docs/STOCK_PAPER_TRADING_AUTOMATION.md)

## 多源数据输入

公开版里可以安全分享的是多路输入的角色分工、接入要求和工程取舍，而不是私有配置或生产环境参数。

详细说明见：
- [多源数据输入说明](./docs/MARKET_DATA_INPUTS.md)

如果后续准备继续完善公开仓库，建议优先补这些内容：
- 页面截图
- 架构图
- 仓库更新日志
- 演示视频或 GIF
