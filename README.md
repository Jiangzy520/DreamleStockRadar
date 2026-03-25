# stock-futures-automation

股票、期货实时数据与模拟盘展示项目公开演示版。

本仓库保留页面结构、入口说明、三源支持说明、桥接面板布局与通知配置界面，用于展示项目能力；真实账户、真实策略参数、真实交易记录、真实日志与生产 API 凭据均已清理。

## 文档导航

- [环境准备： 下载 VS Code 并安装 Codex 扩展](docs/setup-codex.md)
- [股票、期货实时数据获取说明](docs/market-data-guide.md)
- [股票、期货模拟盘账号与接口说明](docs/account-api-guide.md)

## 仓库结构

- `extra_signal_services/`：股票信号页与期货信号页
- `push_xtp_bridge/`：股票桥接、期货桥接、通知配置
- `nginx/`：Nginx 配置示例
- `service_units/`：systemd 服务文件示例
- `vntrader/`：公开版保留的连接结构示例

## 说明

- GitHub 仓库首页显示的是代码目录，不是运行后网页。
- 如果需要查看网页效果，请运行对应本地服务或部署到你自己的服务器环境。
- 如果需要查看更详细的使用说明，可直接阅读 [USAGE_GUIDE.md](USAGE_GUIDE.md)。
