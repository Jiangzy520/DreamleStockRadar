# 股票、期货实时数据与模拟盘自动化公开版

这是从私有生产项目中整理出来的一份 GitHub 安全公开版，用于产品展示、结构说明、项目演示和对外说明。

这个网站的核心目标，是把股票信号、期货信号、股票桥接、期货桥接和消息推送配置整合成一套统一的可视化工作台，让用户可以更直观地查看多源信号页面、桥接执行入口和通知推送配置结构。

当前公开版对应的线上演示地址：
- 股票信号页：https://dreamle.vip/push
- 期货信号页：https://dreamle.vip/futures
- 股票桥接页：https://dreamle.vip/bridge/
- 推送配置页：https://dreamle.vip/notifications

## 网站说明

这套网站目前主要包含 4 个核心页面：

1. 股票信号页
- 用于展示股票信号、多源切换、自选池导入、截图提取代码和完整信号表结构
- 当前保留了 AllTick、通达信、东方财富 三个版本的界面说明和切换逻辑

2. 期货信号页
- 用于展示期货信号页结构、订盘面板、页面入口和公开说明
- 适合作为期货信号展示与演示入口

3. 股票 / 期货桥接页
- 用于展示桥接服务、执行入口、状态卡片、策略摘要和页面跳转结构
- 公开版保留页面能力和结构，不公开真实策略、真实账户和真实执行记录

4. 推送配置页
- 用于配置飞书、钉钉、企业微信等消息推送接口
- 适合给客户自行填写 Webhook 或 API 地址，并统一接收股票信号、期货信号、股票交易信号和期货交易信号

## 延迟与自动化落地说明

在 AI 驱动策略、自动化扫描和量化工具越来越普及之后，真正影响落地效果的，往往不只是策略本身，而是整条链路的算力预算、网络时延、数据一致性和执行反馈速度。

对很多个人投资者或中小团队来说，自动化/量化的第一道门槛，通常不是“有没有想法”，而是：
- 能不能拿到稳定、可持续的实时数据
- 能不能把多源数据统一成可执行的标准格式
- 能不能把信号、下单、成交回报和页面监控串成闭环
- 能不能把链路延迟控制在可接受范围内，而不是在关键时刻出现级联放大

这也是这个项目持续关注的重点：不仅是把页面做出来，而是持续探索如何在公开数据源、终端接口、桥接执行和状态回写之间，把延迟、稳定性和工程复杂度控制在一个更适合个人自动化落地的范围内。

如果你也在研究低延迟行情接入、自动化执行链路、个人量化工程化落地，欢迎交流。

## 公开版保留内容

这份公开版保留了：
- 一个可运行的 Flask 网站壳子
- 股票信号、期货信号、桥接页、推送配置页的整体结构
- 通用演示接口返回和公开展示数据
- 页面入口、模块划分、文档导航和基础运行方式
- 适合 GitHub 展示和产品说明的页面截图

## 公开版移除内容

这份公开版移除了：
- 生产策略代码
- 策略定义、触发条件和参数细节
- 私有数据源接入和服务端扫描逻辑
- 服务器专用配置、运行数据、密钥和内部接口地址
- 真实账户、真实成交、真实持仓、真实自选池和私有日志记录

## 适用场景

这份仓库更适合用于：
- GitHub 公开展示
- 向客户介绍网站结构和功能模块
- 展示股票、期货、多源信号和桥接系统的整体架构
- 作为后续官网、产品说明页、部署说明页的基础仓库

## 联系方式

如需项目交流、功能咨询、部署协助或合作沟通，可通过以下方式联系：

- 微信：`jzy5408`
- 邮箱：`123619518@qq.com`
- QQ 群：`741809439`

欢迎交流，如有需要可提供更专业化的沟通与支持。

## 文档导航

- [环境准备：下载 VS Code 并安装 Codex 扩展](./docs/VSCODE_CODEX_SETUP.md)
- [股票、期货实时数据获取说明](./docs/STOCK_FUTURES_REALTIME_DATA.md)
- [股票、期货模拟盘账号与接口说明](./docs/STOCK_FUTURES_SIM_ACCOUNT_INTERFACES.md)

## 页面预览

这份公开版当前线上演示地址如下：

- 股票信号页：https://dreamle.vip/push
- 期货信号页：https://dreamle.vip/futures
- 股票桥接页：https://dreamle.vip/bridge/
- 推送配置页：https://dreamle.vip/notifications

### 股票信号页

在线访问：https://dreamle.vip/push

![股票信号页](./docs/assets/screenshots/stock-signals.png)

### 期货信号页

在线访问：https://dreamle.vip/futures

![期货信号页](./docs/assets/screenshots/futures-signals.png)

### 股票桥接页

在线访问：https://dreamle.vip/bridge/

![股票桥接页](./docs/assets/screenshots/stock-bridge.png)

### 推送配置页

在线访问：https://dreamle.vip/notifications

![推送配置页](./docs/assets/screenshots/notifications.png)

## 目录说明

```text
webapp/
  server.py
  templates/push.html
docs/
  assets/public-demo-dashboard.png
  assets/screenshots/stock-signals.png
  assets/screenshots/futures-signals.png
  assets/screenshots/stock-bridge.png
  assets/screenshots/notifications.png
  VSCODE_CODEX_SETUP.md
  STOCK_FUTURES_REALTIME_DATA.md
  STOCK_FUTURES_SIM_ACCOUNT_INTERFACES.md
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

## 股票、期货模拟盘账号与接口

公开版里更适合分享的是模拟盘账号申请路径、接口接入方式和工程分层，而不是私有策略或内部交易参数。

详细说明见：
- [股票、期货模拟盘账号与接口说明](./docs/STOCK_FUTURES_SIM_ACCOUNT_INTERFACES.md)

如果后续准备继续完善公开仓库，建议优先补这些内容：
- 页面截图
- 架构图
- 仓库更新日志
- 演示视频或 GIF
