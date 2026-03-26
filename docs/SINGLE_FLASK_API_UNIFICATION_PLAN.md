# 单 Flask 整合方案与 API 统一管理

## 目标

把当前公开仓库里的两个本地 Flask 服务收敛成一个统一入口：

- 只启动一个 Flask 进程
- 页面入口统一
- API 路由统一
- 保留原有模块拆分，避免后续维护越来越散
- 为后续接入 BigQuant 预留标准位置

这份方案的重点不是“一次性大改完”，而是给出一个能分阶段迁移、同时兼容现有逻辑的收敛路径。

## 当前问题

当前仓库里本地演示至少有两套 Flask 入口：

- 主站：
  - `extra_signal_services/mrj_quant_push_site_current/webapp/server.py`
- 桥接/通知/期货面板：
  - `push_xtp_bridge/dashboard_app.py`

现状会带来 4 个长期问题：

1. 启动复杂
- 本地要记两个命令
- 不同页面分散在不同端口

2. 页面链接维护成本高
- 首页按钮要知道该跳 `8768` 还是 `8792`
- 本地和线上还要分别处理

3. API 入口分裂
- 主站 API 和桥接 API 不在一个服务里
- 后面接 BigQuant 或别的信号源时，很容易继续堆更多分支

4. README 和使用方式不直观
- 别人下载后很容易只启动一半
- 页面打开后再遇到 `ERR_CONNECTION_REFUSED`

## 整合目标结构

建议最终收敛成：

```text
webapp/
  app.py
  server.py
  routes/
    pages.py
    realtime.py
    bridge.py
    notifications.py
    strategies.py
  services/
    runtime.py
    bridge_dashboard.py
    notifications.py
    bigquant_adapter.py
  templates/
    push.html
    futures.html
    bridge_stock.html
    bridge_futures.html
    notifications.html
  static/
    app.css
    app.js
```

核心原则：

- **页面统一放在 `webapp/templates/`**
- **路由统一放在 `webapp/routes/`**
- **业务逻辑统一放在 `webapp/services/`**
- **`server.py` 只负责启动，不再堆全部逻辑**

## 单 Flask 最终入口

最终只保留一个启动入口：

```bash
python webapp/server.py --host 127.0.0.1 --port 8768
```

然后所有页面都走同一个端口。

## 页面路由统一方案

建议统一成下面这组页面路由：

### 页面

- `/`
  - 重定向到 `/push`
- `/push`
  - 股票信号页
- `/futures`
  - 期货信号页
- `/bridge`
  - 股票桥接页
- `/bridge/futures`
  - 期货桥接页
- `/notifications`
  - 推送配置页

这样页面入口统一以后：

- 不再需要本地 `8768` / `8792` 双跳转
- 模板里的链接都可以写成相对路径
- README 也会简单很多

## API 统一管理方案

建议统一成 4 组 API 前缀。

### 1. 站点/基础 API

- `/api/meta`
- `/api/home`
- `/healthz`

用途：
- 页面初始化
- 健康检查
- 环境状态展示

### 2. 实时信号 API

- `/api/realtime/signals`
- `/api/realtime/stream`
- `/api/realtime/push-image`
- `/api/realtime/push-image/generate`
- `/api/realtime/push-image/file`
- `/api/realtime/logs`
- `/api/realtime/watchlist`
- `/api/realtime/watchlist/backup`
- `/api/realtime/watchlist/extract-images`
- `/api/realtime/watchlist/save`

这部分继续保留现有主站逻辑，不动语义。

### 3. 桥接 API

建议统一到：

- `/api/bridge/stock`
- `/api/bridge/futures`
- `/api/bridge/futures-v2`

用途：
- 原来 `dashboard_app.py` 里的 `/api/data`
- 原来 `dashboard_app.py` 里的 `/api/futures`
- 原来 `dashboard_app.py` 里的 `/api/futures-v2`

这样命名更清楚，也更适合后面挂不同来源。

### 4. 通知 API

- `/api/notifications/config`
- `/api/notifications/test`
- `/api/notifications/dispatch`

这部分直接沿用当前 `dashboard_app.py` 的接口语义。

### 5. 策略 API

- `/api/strategies/<kind>`
- `/api/strategies/<kind>/action`

这部分继续保留在主站里。

## 兼容旧接口方案

为了不让现有代码一下子全断，建议保留一层兼容路由：

- `/api/data` -> 代理/别名到 `/api/bridge/stock`
- `/api/futures` -> 代理/别名到 `/api/bridge/futures`
- `/api/futures-v2` -> 代理/别名到 `/api/bridge/futures-v2`

这样好处是：

- 旧的桥接代码先不用马上全改
- README 可以先更新成新接口
- 内部老代码还可以过渡一段时间

## BigQuant 接入的统一位置

后续 BigQuant 不要直接散落到页面层，建议统一放在：

```text
webapp/services/bigquant_adapter.py
```

建议在这一层做两件事：

1. 股票信号标准化
- 把 BigQuant 股票信号转成 117 主站统一格式

2. 期货 ABC 标准化
- 把 BigQuant 期货信号转成当前期货桥能消费的统一字段

建议对外暴露成内部函数：

- `load_bigquant_stock_signals()`
- `load_bigquant_futures_signals()`
- `normalize_bigquant_stock_signal(row)`
- `normalize_bigquant_futures_signal(row)`

后续若要提供统一来源接口，也可以扩展：

- `/api/sources/bigquant/stock`
- `/api/sources/bigquant/futures`

但这两个应当属于“来源层 API”，不要和最终给页面/桥接消费的 API 混在一起。

## 推荐的模块迁移方式

### 第一步：先抽函数，不先改页面

从 `push_xtp_bridge/dashboard_app.py` 抽出：

- `build_dashboard_payload()`
- `build_futures_payload()`
- `build_futures_payload_v2()`
- 通知配置读写
- 通知测试发送
- 通知分发

放到：

```text
webapp/services/bridge_dashboard.py
webapp/services/notifications.py
```

这样先把“逻辑”抽离，再做“页面整合”。

### 第二步：把桥接页和通知页改成主站路由

在主站里新增：

- `/bridge`
- `/bridge/futures`
- `/notifications`

模板建议改成单独文件，而不是继续 `render_template_string`。

### 第三步：保留兼容层

在主站里顺手保留：

- `/api/data`
- `/api/futures`
- `/api/futures-v2`

这些都由主站统一返回。

这样就可以彻底停掉独立的 `dashboard_app.py` 进程。

## 模板管理建议

当前主站已经有：

- `webapp/templates/push.html`

建议把原 `dashboard_app.py` 里字符串模板拆成：

- `webapp/templates/bridge_stock.html`
- `webapp/templates/bridge_futures.html`
- `webapp/templates/notifications.html`

这样后面改样式、改入口、接 BigQuant 都轻松很多。

## 最终端口策略

本地和线上都建议只保留一个对外 Web 端口，例如：

- `8768`

统一后：

- 页面只走一个端口
- API 只走一个端口
- nginx 配置更简单
- 文档更简单

## 给下载者的最终体验

整合完成后，别人下载仓库应该只需要：

```bash
python webapp/server.py --host 127.0.0.1 --port 8768
```

然后直接访问：

- `http://127.0.0.1:8768/push`
- `http://127.0.0.1:8768/futures`
- `http://127.0.0.1:8768/bridge`
- `http://127.0.0.1:8768/bridge/futures`
- `http://127.0.0.1:8768/notifications`

## 我建议的实际实施顺序

1. 先做 API 逻辑抽离
- 不改外观
- 不改业务语义

2. 再把桥接页和通知页搬进主站
- 页面统一
- 端口统一

3. 再移除独立 `dashboard_app.py` 启动依赖

4. 最后接入 BigQuant
- 股票信号接 `/api/realtime/signals`
- 期货信号接 `/api/bridge/futures` 或内部适配层

## 一句话结论

最合适的长期方案不是“双 Flask + 一键启动”，而是：

- **一个 Flask 主服务**
- **模块继续拆分**
- **API 按前缀统一管理**
- **旧接口保留兼容层**
- **BigQuant 只接到统一服务层**

这套方案更适合你后续自己维护，也更适合公开给别人下载直接用。
