const state = {
  mode: "unknown",
  envs: [],
  favorites: [],
  home: null,
  pollTimer: null,
};

function showToast(text) {
  const toast = document.getElementById("toast");
  toast.textContent = text;
  toast.classList.add("show");
  setTimeout(() => toast.classList.remove("show"), 2200);
}

async function apiGet(url) {
  const resp = await fetch(url, { headers: { "Content-Type": "application/json" } });
  return resp.json();
}

async function apiPost(url, payload) {
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {}),
  });
  return resp.json();
}

function tableHtml(columns, rows) {
  const head = columns.map((c) => `<th>${c.label}</th>`).join("");
  const body = rows
    .map(
      (row) =>
        `<tr>${columns
          .map((c) => `<td>${row[c.key] === undefined || row[c.key] === null ? "" : row[c.key]}</td>`)
          .join("")}</tr>`
    )
    .join("");
  return `<table class="table"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}

function renderHome(data) {
  state.home = data;
  document.getElementById("runtimeMode").textContent = data.mode.toUpperCase();

  const gatewaySelect = document.getElementById("gatewaySelect");
  gatewaySelect.innerHTML = "";
  (data.gateways || []).forEach((gw) => {
    const op = document.createElement("option");
    op.value = gw;
    op.textContent = gw;
    gatewaySelect.appendChild(op);
  });

  document.getElementById("tab-accounts").innerHTML = tableHtml(
    [
      { key: "accountid", label: "账号" },
      { key: "balance", label: "余额" },
      { key: "frozen", label: "冻结" },
      { key: "available", label: "可用" },
      { key: "gateway_name", label: "账户" },
    ],
    data.accounts || []
  );

  document.getElementById("tab-portfolio").innerHTML =
    `<h4>组合盈亏</h4>` +
    tableHtml(
      [
        { key: "reference", label: "组合" },
        { key: "gateway_name", label: "账户" },
        { key: "trading_pnl", label: "交易盈亏" },
        { key: "holding_pnl", label: "持仓盈亏" },
        { key: "total_pnl", label: "总盈亏" },
        { key: "commission", label: "手续费" },
      ],
      data.portfolio_totals || []
    ) +
    `<h4>合约盈亏</h4>` +
    tableHtml(
      [
        { key: "reference", label: "组合" },
        { key: "vt_symbol", label: "合约" },
        { key: "gateway_name", label: "账户" },
        { key: "last_pos", label: "当前仓位" },
        { key: "total_pnl", label: "总盈亏" },
        { key: "commission", label: "手续费" },
      ],
      data.portfolio_contracts || []
    );

  document.getElementById("tab-risk").innerHTML = tableHtml(
    [
      { key: "name", label: "规则" },
      { key: "parameters", label: "参数" },
      { key: "variables", label: "变量" },
    ],
    (data.risk_rules || []).map((x) => ({
      name: x.name || "",
      parameters: JSON.stringify(x.parameters || {}),
      variables: JSON.stringify(x.variables || {}),
    }))
  );

  document.getElementById("tab-logs").innerHTML = tableHtml(
    [
      { key: "time", label: "时间" },
      { key: "level", label: "级别" },
      { key: "source", label: "来源" },
      { key: "msg", label: "信息" },
    ],
    (data.logs || []).slice().reverse()
  );

  document.getElementById("tab-positions").innerHTML = tableHtml(
    [
      { key: "symbol", label: "代码" },
      { key: "direction", label: "方向" },
      { key: "volume", label: "数量" },
      { key: "frozen", label: "冻结" },
      { key: "price", label: "均价" },
      { key: "pnl", label: "盈亏" },
      { key: "gateway_name", label: "账户" },
    ],
    data.positions || []
  );

  document.getElementById("tab-orders").innerHTML = tableHtml(
    [
      { key: "symbol", label: "代码" },
      { key: "direction", label: "方向" },
      { key: "offset", label: "开平" },
      { key: "price", label: "价格" },
      { key: "volume", label: "数量" },
      { key: "traded", label: "已成交" },
      { key: "status", label: "状态" },
      { key: "datetime", label: "时间" },
      { key: "gateway_name", label: "账户" },
    ],
    data.orders || []
  );

  document.getElementById("tab-trades").innerHTML = tableHtml(
    [
      { key: "symbol", label: "代码" },
      { key: "direction", label: "方向" },
      { key: "offset", label: "开平" },
      { key: "price", label: "价格" },
      { key: "volume", label: "数量" },
      { key: "datetime", label: "时间" },
      { key: "gateway_name", label: "账户" },
    ],
    data.trades || []
  );
}

async function refreshHome() {
  const resp = await apiGet("/api/home");
  if (resp.ok) {
    renderHome(resp.data);
  }
}

function bindTabs() {
  document.querySelectorAll(".tab").forEach((btn) => {
    btn.addEventListener("click", () => {
      const tabName = btn.dataset.tab;
      const parent = btn.closest(".panel");
      parent.querySelectorAll(".tab").forEach((t) => t.classList.remove("active"));
      parent.querySelectorAll(".tab-pane").forEach((pane) => pane.classList.remove("active"));
      btn.classList.add("active");
      const pane = parent.querySelector(`#tab-${tabName}`);
      if (pane) pane.classList.add("active");
    });
  });
}

function activatePanelTab(panel, tabName) {
  if (!panel || !tabName) return;
  const tabBtn = panel.querySelector(`.tab[data-tab="${tabName}"]`);
  const tabPane = panel.querySelector(`#tab-${tabName}`);
  if (!tabBtn || !tabPane) return;
  panel.querySelectorAll(".tab").forEach((t) => t.classList.remove("active"));
  panel.querySelectorAll(".tab-pane").forEach((pane) => pane.classList.remove("active"));
  tabBtn.classList.add("active");
  tabPane.classList.add("active");
}

function bindModuleCards() {
  document.querySelectorAll(".card-btn").forEach((btn) => {
    btn.addEventListener("click", (event) => {
      const href = btn.getAttribute("href") || "";
      if (href && !href.startsWith("#")) {
        const clickStatus = document.getElementById("clickStatus");
        if (clickStatus) {
          clickStatus.textContent = `进入模块 ${btn.textContent.trim()}...`;
        }
        return;
      }

      event.preventDefault();
      const target = btn.dataset.target || "#panel-trading";
      const panel = document.querySelector(target);
      if (!panel) {
        showToast("页面区块不存在");
        return;
      }
      document.querySelectorAll(".card-btn").forEach((x) => x.classList.remove("active"));
      btn.classList.add("active");
      activatePanelTab(panel, btn.dataset.tab || "");
      const hash = target.startsWith("#") ? target : `#${target}`;
      window.location.hash = hash;
      panel.scrollIntoView({ behavior: "smooth", block: "start" });
      panel.classList.remove("flash");
      // Reflow to restart animation.
      // eslint-disable-next-line no-unused-expressions
      panel.offsetHeight;
      panel.classList.add("flash");
      const txt = `已定位到 ${btn.textContent.trim()}`;
      const clickStatus = document.getElementById("clickStatus");
      if (clickStatus) {
        clickStatus.textContent = txt;
      }
      showToast(txt);
    });
  });
}

function bindClock() {
  const el = document.getElementById("runtimeClock");
  setInterval(() => {
    const now = new Date();
    const parts = [now.getHours(), now.getMinutes(), now.getSeconds()].map((x) => String(x).padStart(2, "0"));
    el.textContent = parts.join(":");
  }, 1000);
}

function bindForm() {
  document.getElementById("symbolSelect").addEventListener("change", (e) => {
    const selected = state.favorites.find((x) => x.symbol === e.target.value);
    if (selected) {
      document.getElementById("exchangeInput").value = selected.exchange || "";
    }
  });

  document.getElementById("orderForm").addEventListener("submit", async (e) => {
    e.preventDefault();
    const payload = {
      symbol: document.getElementById("symbolSelect").value,
      exchange: document.getElementById("exchangeInput").value,
      direction: document.getElementById("directionInput").value,
      offset: document.getElementById("offsetInput").value,
      type: document.getElementById("typeInput").value,
      price: Number(document.getElementById("priceInput").value || 0),
      volume: Number(document.getElementById("volumeInput").value || 1),
      gateway: document.getElementById("gatewaySelect").value,
    };
    const resp = await apiPost("/api/order", payload);
    if (resp.ok) {
      showToast(`委托成功: ${resp.vt_orderid || ""}`);
      refreshHome();
      return;
    }
    showToast(`委托失败: ${resp.error || "未知错误"}`);
  });

  document.getElementById("btnCancelAll").addEventListener("click", async () => {
    const resp = await apiPost("/api/order/cancel_all", {});
    if (resp.ok) {
      showToast(`全撤完成: ${resp.count || 0}`);
      refreshHome();
    }
  });
}

function bindConnections() {
  document.getElementById("btnConnect").addEventListener("click", async () => {
    const env = document.getElementById("envSelect").value;
    const resp = await apiPost("/api/connect", { env });
    showToast(resp.ok ? `连接指令已发送: ${env}` : `连接失败: ${resp.error || "未知错误"}`);
    refreshHome();
  });

  document.getElementById("btnDisconnect").addEventListener("click", async () => {
    const env = document.getElementById("envSelect").value;
    const resp = await apiPost("/api/disconnect", { env });
    showToast(resp.ok ? `断开指令已发送: ${env}` : `断开失败: ${resp.error || "未知错误"}`);
    refreshHome();
  });
}

function addBubble(role, text) {
  const box = document.getElementById("aiChatBox");
  const div = document.createElement("div");
  div.className = `bubble ${role}`;
  div.textContent = text;
  box.appendChild(div);
  box.scrollTop = box.scrollHeight;
}

function bindAI() {
  const send = async () => {
    const input = document.getElementById("aiInput");
    const text = input.value.trim();
    if (!text) return;
    const model = document.getElementById("aiModel").value;
    addBubble("user", text);
    input.value = "";
    const resp = await apiPost("/api/ai/chat", { message: text, model });
    if (resp.ok) {
      addBubble("ai", resp.content || "");
    } else {
      addBubble("ai", `AI 调用失败: ${resp.error || "未知错误"}`);
    }
  };

  document.getElementById("aiSend").addEventListener("click", send);
  document.getElementById("aiInput").addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      send();
    }
  });
}

async function bootstrap() {
  bindClock();
  bindTabs();
  bindModuleCards();
  bindForm();
  bindConnections();
  bindAI();

  const meta = await apiGet("/api/meta");
  if (meta.ok) {
    state.mode = meta.mode;
    state.envs = meta.envs || [];
    state.favorites = meta.favorites || [];

    const envSelect = document.getElementById("envSelect");
    envSelect.innerHTML = "";
    state.envs.forEach((env) => {
      const op = document.createElement("option");
      op.value = env;
      op.textContent = env;
      envSelect.appendChild(op);
    });

    const symbolSelect = document.getElementById("symbolSelect");
    symbolSelect.innerHTML = "";
    state.favorites.forEach((item) => {
      const op = document.createElement("option");
      op.value = item.symbol;
      op.textContent = `${item.name} ${item.symbol}`;
      symbolSelect.appendChild(op);
    });

    if (state.favorites.length) {
      document.getElementById("exchangeInput").value = state.favorites[0].exchange || "";
    }
  }

  await refreshHome();
  state.pollTimer = setInterval(refreshHome, 3000);
}

window.addEventListener("error", (e) => {
  showToast(`前端异常: ${e.message}`);
});

bootstrap().catch((err) => {
  // Keep UI usable and show explicit feedback when bootstrap fails.
  // eslint-disable-next-line no-console
  console.error(err);
  showToast(`初始化失败: ${err.message || err}`);
});
