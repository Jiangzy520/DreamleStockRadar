#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "ThostFtdcTraderApi.h"

namespace {

struct Options {
    std::string front;
    std::string broker_id;
    std::string user_id;
    std::string password;
    std::string instrument_id;
    std::string app_id;
    std::string auth_code;
    std::string flow_dir = "./ctpmini_flow";
    int timeout_seconds = 15;
};

template <size_t N>
void CopyText(char (&dest)[N], const std::string& src) {
    std::strncpy(dest, src.c_str(), N - 1);
    dest[N - 1] = '\0';
}

std::string SafeText(const char* text) {
    return text == nullptr ? "" : text;
}

bool IsRspSuccess(CThostFtdcRspInfoField* info) {
    return info == nullptr || info->ErrorID == 0;
}

std::string FormatRspInfo(CThostFtdcRspInfoField* info) {
    if (info == nullptr) {
        return "ErrorID=0 ErrorMsg=";
    }
    return "ErrorID=" + std::to_string(info->ErrorID) + " ErrorMsg=" + SafeText(info->ErrorMsg);
}

void PrintUsage(const char* argv0) {
    std::cout
        << "Usage:\n"
        << "  " << argv0
        << " --front tcp://host:port"
        << " --broker BROKER_ID"
        << " --user USER_ID"
        << " --password PASSWORD"
        << " --instrument INSTRUMENT_ID"
        << " [--app-id APP_ID --auth-code AUTH_CODE]"
        << " [--flow-dir ./ctpmini_flow]"
        << " [--timeout 15]\n\n"
        << "Example:\n"
        << "  " << argv0
        << " --front tcp://ctp2-trade-isp1.hxqh168.com:51121"
        << " --broker 9999"
        << " --user your_user"
        << " --password your_password"
        << " --instrument rb2605\n";
}

std::optional<Options> ParseArgs(int argc, char* argv[]) {
    Options options;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto require_value = [&](const std::string& name) -> std::string {
            if (i + 1 >= argc) {
                throw std::runtime_error("missing value for " + name);
            }
            return argv[++i];
        };

        if (arg == "--front") {
            options.front = require_value(arg);
        } else if (arg == "--broker") {
            options.broker_id = require_value(arg);
        } else if (arg == "--user") {
            options.user_id = require_value(arg);
        } else if (arg == "--password") {
            options.password = require_value(arg);
        } else if (arg == "--instrument") {
            options.instrument_id = require_value(arg);
        } else if (arg == "--app-id") {
            options.app_id = require_value(arg);
        } else if (arg == "--auth-code") {
            options.auth_code = require_value(arg);
        } else if (arg == "--flow-dir") {
            options.flow_dir = require_value(arg);
        } else if (arg == "--timeout") {
            options.timeout_seconds = std::stoi(require_value(arg));
        } else if (arg == "-h" || arg == "--help") {
            PrintUsage(argv[0]);
            return std::nullopt;
        } else {
            throw std::runtime_error("unknown argument: " + arg);
        }
    }

    if (options.front.empty() || options.broker_id.empty() || options.user_id.empty() ||
        options.password.empty() || options.instrument_id.empty()) {
        throw std::runtime_error("missing required arguments");
    }
    if (options.app_id.empty() != options.auth_code.empty()) {
        throw std::runtime_error("--app-id and --auth-code must be provided together");
    }
    return options;
}

class TraderSpi final : public CThostFtdcTraderSpi {
public:
    TraderSpi(CThostFtdcTraderApi* api, Options options)
        : api_(api), options_(std::move(options)) {}

    void OnFrontConnected() override {
        std::cout << "[INFO] Front connected\n";
        if (!options_.app_id.empty()) {
            SendAuthenticate();
        } else {
            SendLogin();
        }
    }

    void OnFrontDisconnected(int nReason) override {
        std::cerr << "[ERROR] Front disconnected, reason=" << nReason << '\n';
        Finish(false);
    }

    void OnRspAuthenticate(
        CThostFtdcRspAuthenticateField*,
        CThostFtdcRspInfoField* pRspInfo,
        int nRequestID,
        bool bIsLast) override {
        std::cout << "[INFO] OnRspAuthenticate request_id=" << nRequestID
                  << " last=" << bIsLast << ' ' << FormatRspInfo(pRspInfo) << '\n';
        if (!bIsLast) {
            return;
        }
        if (!IsRspSuccess(pRspInfo)) {
            Finish(false);
            return;
        }
        SendLogin();
    }

    void OnRspUserLogin(
        CThostFtdcRspUserLoginField* pRspUserLogin,
        CThostFtdcRspInfoField* pRspInfo,
        int nRequestID,
        bool bIsLast) override {
        std::cout << "[INFO] OnRspUserLogin request_id=" << nRequestID
                  << " last=" << bIsLast << ' ' << FormatRspInfo(pRspInfo) << '\n';
        if (!bIsLast) {
            return;
        }
        if (!IsRspSuccess(pRspInfo)) {
            Finish(false);
            return;
        }

        if (pRspUserLogin != nullptr) {
            std::cout << "[INFO] TradingDay=" << SafeText(pRspUserLogin->TradingDay)
                      << " LoginTime=" << SafeText(pRspUserLogin->LoginTime)
                      << " FrontID=" << pRspUserLogin->FrontID
                      << " SessionID=" << pRspUserLogin->SessionID << '\n';
        }

        SendQryDepthMarketData();
    }

    void OnRspQryDepthMarketData(
        CThostFtdcDepthMarketDataField* pDepthMarketData,
        CThostFtdcRspInfoField* pRspInfo,
        int nRequestID,
        bool bIsLast) override {
        std::cout << "[INFO] OnRspQryDepthMarketData request_id=" << nRequestID
                  << " last=" << bIsLast << ' ' << FormatRspInfo(pRspInfo) << '\n';

        if (pDepthMarketData != nullptr) {
            got_market_data_ = true;
            std::cout
                << "[MARKET] instrument=" << SafeText(pDepthMarketData->InstrumentID)
                << " exchange=" << SafeText(pDepthMarketData->ExchangeID)
                << " trading_day=" << SafeText(pDepthMarketData->TradingDay)
                << " action_day=" << SafeText(pDepthMarketData->ActionDay)
                << " update_time=" << SafeText(pDepthMarketData->UpdateTime)
                << '.' << pDepthMarketData->UpdateMillisec
                << " last=" << pDepthMarketData->LastPrice
                << " open=" << pDepthMarketData->OpenPrice
                << " high=" << pDepthMarketData->HighestPrice
                << " low=" << pDepthMarketData->LowestPrice
                << " close=" << pDepthMarketData->ClosePrice
                << " volume=" << pDepthMarketData->Volume
                << " open_interest=" << pDepthMarketData->OpenInterest
                << " bid1=" << pDepthMarketData->BidPrice1
                << "@" << pDepthMarketData->BidVolume1
                << " ask1=" << pDepthMarketData->AskPrice1
                << "@" << pDepthMarketData->AskVolume1
                << '\n';
        }

        if (!bIsLast) {
            return;
        }

        if (!IsRspSuccess(pRspInfo) || !got_market_data_) {
            Finish(false);
            return;
        }

        Finish(true);
    }

    void OnRspError(CThostFtdcRspInfoField* pRspInfo, int nRequestID, bool bIsLast) override {
        std::cerr << "[ERROR] OnRspError request_id=" << nRequestID
                  << " last=" << bIsLast << ' ' << FormatRspInfo(pRspInfo) << '\n';
    }

    bool WaitUntilDone(std::chrono::seconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [&] { return done_; });
    }

    bool success() const { return success_.load(); }

private:
    void SendAuthenticate() {
        CThostFtdcReqAuthenticateField request{};
        CopyText(request.BrokerID, options_.broker_id);
        CopyText(request.UserID, options_.user_id);
        CopyText(request.UserProductInfo, "Codex-CTPMini");
        CopyText(request.AuthCode, options_.auth_code);
        CopyText(request.AppID, options_.app_id);

        const int request_id = NextRequestId();
        const int ret = api_->ReqAuthenticate(&request, request_id);
        std::cout << "[REQ] ReqAuthenticate request_id=" << request_id << " ret=" << ret << '\n';
        if (ret != 0) {
            Finish(false);
        }
    }

    void SendLogin() {
        CThostFtdcReqUserLoginField request{};
        CopyText(request.BrokerID, options_.broker_id);
        CopyText(request.UserID, options_.user_id);
        CopyText(request.Password, options_.password);
        CopyText(request.UserProductInfo, "Codex-CTPMini");
        CopyText(request.InterfaceProductInfo, "Codex-CTPMini");
        CopyText(request.ProtocolInfo, "tcp");
        CopyText(request.LoginRemark, "ReqQryDepthMarketData sample");

        const int request_id = NextRequestId();
        const int ret = api_->ReqUserLogin(&request, request_id);
        std::cout << "[REQ] ReqUserLogin request_id=" << request_id << " ret=" << ret << '\n';
        if (ret != 0) {
            Finish(false);
        }
    }

    void SendQryDepthMarketData() {
        CThostFtdcQryDepthMarketDataField request{};
        CopyText(request.InstrumentID, options_.instrument_id);

        const int request_id = NextRequestId();
        const int ret = api_->ReqQryDepthMarketData(&request, request_id);
        std::cout << "[REQ] ReqQryDepthMarketData instrument=" << options_.instrument_id
                  << " request_id=" << request_id << " ret=" << ret << '\n';
        if (ret != 0) {
            Finish(false);
        }
    }

    int NextRequestId() { return ++request_id_; }

    void Finish(bool success) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (done_) {
                return;
            }
            done_ = true;
            success_ = success;
        }
        cv_.notify_all();
    }

    CThostFtdcTraderApi* api_;
    Options options_;
    std::atomic<bool> success_{false};
    std::atomic<bool> got_market_data_{false};
    std::atomic<int> request_id_{0};
    std::mutex mutex_;
    std::condition_variable cv_;
    bool done_ = false;
};

}  // namespace

int main(int argc, char* argv[]) {
    try {
        const auto parsed = ParseArgs(argc, argv);
        if (!parsed.has_value()) {
            return 0;
        }
        const Options options = *parsed;

        std::filesystem::create_directories(options.flow_dir);

        std::cout << "[INFO] CTPMini Trader API version: "
                  << CThostFtdcTraderApi::GetApiVersion() << '\n';

        CThostFtdcTraderApi* api =
            CThostFtdcTraderApi::CreateFtdcTraderApi(options.flow_dir.c_str());
        TraderSpi spi(api, options);

        api->RegisterSpi(&spi);
        api->SubscribePrivateTopic(THOST_TERT_QUICK);
        api->SubscribePublicTopic(THOST_TERT_QUICK);
        api->RegisterFront(const_cast<char*>(options.front.c_str()));
        api->Init();

        const bool finished = spi.WaitUntilDone(std::chrono::seconds(options.timeout_seconds));
        if (!finished) {
            std::cerr << "[ERROR] Timed out waiting for login/query callbacks\n";
            api->Release();
            return 2;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        api->Release();
        return spi.success() ? 0 : 1;
    } catch (const std::exception& ex) {
        std::cerr << "[ERROR] " << ex.what() << '\n';
        PrintUsage(argv[0]);
        return 1;
    }
}
