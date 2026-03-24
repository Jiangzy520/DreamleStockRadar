const pageState = {
  slug: "",
  meta: null,
  home: null,
  pollTimer: null,
  strategyRows: [],
  strategyError: "",
};

function showToast(text) {
  const toast = document.getElementById("toast");
  if (!toast) return;
  toast.textContent = text;
  toast.classList.add("show");
  setTimeout(() => toast.classList.remove("show"), 2200);
}

async function apiGet(url) {
  const resp = await fetch(url, { headers: { "Content-Type": "application/json" } });
  let data = {};
  try {
    data = await resp.json();
  } catch (_e) {
    data = { ok: false, error: "响应解析失败" };
  }
  if (!resp.ok && data.ok === undefined) data.ok = false;
  return data;
}

async function apiPost(url, payload) {
  const resp = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {}),
  });
  let data = {};
  try {
    data = await resp.json();
  } catch (_e) {
    data = { ok: false, error: "响应解析失败" };
  }
  if (!resp.ok && data.ok === undefined) data.ok = false;
  return data;
}

function escapeHtml(text) {
  return String(text || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
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

function asJson(value) {
  try {
    return JSON.stringify(value || {});
  } catch (_e) {
    return "{}";
  }
}

function setStat(text) {
  const el = document.getElementById("moduleStat");
  if (el) el.textContent = text;
}

function setControls(html) {
  const el = document.getElementById("moduleControls");
  if (el) el.innerHTML = html;
}

function setContent(html) {
  const el = document.getElementById("moduleHint");
  if (el) el.innerHTML = html;
}

function bindClock() {
  const el = document.getElementById("moduleClock");
  if (!el) return;
  setInterval(() => {
    const now = new Date();
    const parts = [now.getHours(), now.getMinutes(), now.getSeconds()].map((x) => String(x).padStart(2, "0"));
    el.textContent = parts.join(":");
  }, 1000);
}

function optionsFrom(items) {
  return (items || []).map((x) => `<option value="${x}">${x}</option>`).join("");
}

function getValue(id) {
  const el = document.getElementById(id);
  if (!el) return "";
  return el.value || "";
}

function strategyKindFromSlug(slug) {
  if (slug === "cta") return "cta";
  if (slug === "portfolio-strategy") return "portfolio";
  if (slug === "script") return "script";
  return "";
}

async function doStrategyAction(kind, action, strategyName = "") {
  const payload = { action };
  if (strategyName) payload.strategy_name = strategyName;
  const resp = await apiPost(`/api/strategies/${kind}/action`, payload);
  if (!resp.ok) {
    showToast(`操作失败: ${resp.error || "未知错误"}`);
    return;
  }
  showToast("操作已发送");
  await refreshHome();
}

function renderAccounts() {
  const envs = pageState.meta?.envs || [];
  const h = pageState.home || {};

  setControls(`
    <select id="envSelectModule">${optionsFrom(envs)}</select>
    <button id="btnConnectModule" class="btn">连接</button>
    <button id="btnDisconnectModule" class="btn ghost">断开</button>
    <button id="btnRefreshModule" class="btn ghost">刷新</button>
  `);

  setContent(
    `<h3>资金</h3>` +
      tableHtml(
        [
          { key: "accountid", label: "账号" },
          { key: "balance", label: "余额" },
          { key: "frozen", label: "冻结" },
          { key: "available", label: "可用" },
          { key: "gateway_name", label: "账户" },
        ],
        h.accounts || []
      ) +
      `<h3>持仓</h3>` +
      tableHtml(
        [
          { key: "symbol", label: "代码" },
          { key: "direction", label: "方向" },
          { key: "volume", label: "数量" },
          { key: "frozen", label: "冻结" },
          { key: "price", label: "均价" },
          { key: "pnl", label: "盈亏" },
          { key: "gateway_name", label: "账户" },
        ],
        h.positions || []
      )
  );

  const c = document.getElementById("btnConnectModule");
  const d = document.getElementById("btnDisconnectModule");
  const r = document.getElementById("btnRefreshModule");
  const s = document.getElementById("envSelectModule");

  if (c && d && r && s) {
    c.onclick = async () => {
      if (!s.value) {
        showToast("当前没有可连接账户");
        return;
      }
      const resp = await apiPost("/api/connect", { env: s.value });
      showToast(resp.ok ? `已发送连接 ${s.value}` : `连接失败: ${resp.error || ""}`);
      await refreshHome();
    };
    d.onclick = async () => {
      if (!s.value) {
        showToast("当前没有可断开账户");
        return;
      }
      const resp = await apiPost("/api/disconnect", { env: s.value });
      showToast(resp.ok ? `已发送断开 ${s.value}` : `断开失败: ${resp.error || ""}`);
      await refreshHome();
    };
    r.onclick = async () => {
      await refreshHome();
      showToast("已刷新");
    };
  }
}

function renderPaper() {
  const h = pageState.home || {};
  const favorites = pageState.meta?.favorites || [];
  const gateways = h.gateways || [];

  const symbolOps = favorites
    .map((f) => `<option value="${escapeHtml(f.symbol)}" data-exchange="${escapeHtml(f.exchange || "")}">${escapeHtml(f.name)} ${escapeHtml(f.symbol)}</option>`)
    .join("");
  const gwOps = gateways.map((g) => `<option value="${escapeHtml(g)}">${escapeHtml(g)}</option>`).join("");

  setControls(`
    <select id="symbolSelectModule">${symbolOps}</select>
    <input id="exchangeInputModule" value="${escapeHtml(favorites[0]?.exchange || "CFFEX")}" style="width:100px;" />
    <select id="directionInputModule"><option>多</option><option>空</option></select>
    <select id="offsetInputModule"><option>开</option><option>平</option><option>平今</option><option>平昨</option></select>
    <select id="typeInputModule"><option>限价</option><option>市价</option></select>
    <input id="priceInputModule" type="number" step="0.01" value="0" style="width:110px;" />
    <input id="volumeInputModule" type="number" min="1" step="1" value="1" style="width:90px;" />
    <select id="gatewaySelectModule">${gwOps}</select>
    <button id="btnSendModule" class="btn submit">委托</button>
    <button id="btnCancelAllModule" class="btn danger">全撤</button>
  `);

  setContent(
    `<h3>委托</h3>` +
      tableHtml(
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
        h.orders || []
      ) +
      `<h3>成交</h3>` +
      tableHtml(
        [
          { key: "symbol", label: "代码" },
          { key: "direction", label: "方向" },
          { key: "offset", label: "开平" },
          { key: "price", label: "价格" },
          { key: "volume", label: "数量" },
          { key: "datetime", label: "时间" },
          { key: "gateway_name", label: "账户" },
        ],
        h.trades || []
      )
  );

  const sym = document.getElementById("symbolSelectModule");
  const ex = document.getElementById("exchangeInputModule");
  if (sym && ex) {
    sym.addEventListener("change", () => {
      const idx = sym.selectedIndex;
      if (idx >= 0) {
        const op = sym.options[idx];
        ex.value = op.getAttribute("data-exchange") || ex.value;
      }
    });
  }

  const send = document.getElementById("btnSendModule");
  if (send) {
    send.onclick = async () => {
      const payload = {
        symbol: getValue("symbolSelectModule"),
        exchange: getValue("exchangeInputModule"),
        direction: getValue("directionInputModule"),
        offset: getValue("offsetInputModule"),
        type: getValue("typeInputModule"),
        price: Number(getValue("priceInputModule") || 0),
        volume: Number(getValue("volumeInputModule") || 1),
        gateway: getValue("gatewaySelectModule"),
      };
      const resp = await apiPost("/api/order", payload);
      showToast(resp.ok ? `委托成功: ${resp.vt_orderid || ""}` : `委托失败: ${resp.error || ""}`);
      await refreshHome();
    };
  }

  const ca = document.getElementById("btnCancelAllModule");
  if (ca) {
    ca.onclick = async () => {
      const resp = await apiPost("/api/order/cancel_all", {});
      showToast(resp.ok ? `已全撤 ${resp.count || 0}` : "全撤失败");
      await refreshHome();
    };
  }
}

function renderAI() {
  setControls(`
    <select id="aiModelModule">
      <option>deepseek-chat</option>
      <option>deepseek-reasoner</option>
      <option>glm-4-plus</option>
    </select>
    <button id="btnAiSendModule" class="btn submit">发送</button>
  `);

  setContent(`
    <div id="aiChatModule" class="chat-box" style="min-height:320px;"></div>
    <div class="chat-input" style="margin-top:8px;">
      <textarea id="aiInputModule" placeholder="输入问题，Enter发送，Shift+Enter换行"></textarea>
    </div>
  `);

  const addBubble = (role, text) => {
    const box = document.getElementById("aiChatModule");
    const div = document.createElement("div");
    div.className = `bubble ${role}`;
    div.textContent = text;
    box.appendChild(div);
    box.scrollTop = box.scrollHeight;
  };

  const send = async () => {
    const input = document.getElementById("aiInputModule");
    const text = (input.value || "").trim();
    if (!text) return;
    const model = document.getElementById("aiModelModule").value;
    addBubble("user", text);
    input.value = "";
    const resp = await apiPost("/api/ai/chat", { message: text, model });
    addBubble("ai", resp.ok ? resp.content || "" : `调用失败: ${resp.error || ""}`);
  };

  const btn = document.getElementById("btnAiSendModule");
  const input = document.getElementById("aiInputModule");
  if (btn && input) {
    btn.onclick = send;
    input.onkeydown = (e) => {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        send();
      }
    };
  }
}

function renderRisk() {
  const h = pageState.home || {};
  setControls(`<button id="btnRefreshRisk" class="btn ghost">刷新</button>`);
  setContent(
    tableHtml(
      [
        { key: "name", label: "规则" },
        { key: "parameters", label: "参数" },
        { key: "variables", label: "变量" },
      ],
      (h.risk_rules || []).map((x) => ({
        name: x.name || "",
        parameters: asJson(x.parameters),
        variables: asJson(x.variables),
      }))
    ) +
      `<h3>实时日志</h3>` +
      tableHtml(
        [
          { key: "time", label: "时间" },
          { key: "level", label: "级别" },
          { key: "source", label: "来源" },
          { key: "msg", label: "信息" },
        ],
        (h.logs || []).slice().reverse()
      )
  );
  const btn = document.getElementById("btnRefreshRisk");
  if (btn) btn.onclick = async () => refreshHome();
}

function renderCtaStrategies() {
  const rows = pageState.strategyRows || [];
  const error = pageState.strategyError || "";

  setControls(`
    <button id="btnCtaReload" class="btn ghost">刷新策略类</button>
    <button id="btnCtaInitAll" class="btn">全部初始化</button>
    <button id="btnCtaStartAll" class="btn submit">全部启动</button>
    <button id="btnCtaStopAll" class="btn danger">全部停止</button>
    <button id="btnCtaRefresh" class="btn ghost">刷新</button>
  `);

  if (error) {
    setContent(`<div class="bubble ai">CTA 模块加载失败：${escapeHtml(error)}</div>`);
    return;
  }

  if (!rows.length) {
    setContent(`<div class="bubble ai">当前没有 CTA 策略。可在桌面端先添加策略后，这里会自动同步显示。</div>`);
  } else {
    const body = rows
      .map((r) => {
        const name = escapeHtml(r.strategy_name);
        const canInit = !r.inited;
        const canStart = r.inited && !r.trading;
        const canStop = r.trading;
        return `
          <tr>
            <td>${name}</td>
            <td>${escapeHtml(r.class_name)}</td>
            <td>${escapeHtml(r.vt_symbol)}</td>
            <td>${escapeHtml(r.gateway_name)}</td>
            <td>${r.inited ? "是" : "否"}</td>
            <td>${r.trading ? "运行中" : "已停止"}</td>
            <td>
              <button class="btn ghost cta-row-act" data-action="init" data-name="${name}" ${canInit ? "" : "disabled"}>初始化</button>
              <button class="btn submit cta-row-act" data-action="start" data-name="${name}" ${canStart ? "" : "disabled"}>启动</button>
              <button class="btn danger cta-row-act" data-action="stop" data-name="${name}" ${canStop ? "" : "disabled"}>停止</button>
              <button class="btn ghost cta-row-act" data-action="reset" data-name="${name}">重置</button>
            </td>
          </tr>
        `;
      })
      .join("");

    setContent(`
      <h3>CTA 策略列表</h3>
      <table class="table">
        <thead>
          <tr>
            <th>策略名</th><th>类名</th><th>合约</th><th>账户</th><th>已初始化</th><th>状态</th><th>操作</th>
          </tr>
        </thead>
        <tbody>${body}</tbody>
      </table>
      <h3>实时日志</h3>
      ${tableHtml(
        [
          { key: "time", label: "时间" },
          { key: "level", label: "级别" },
          { key: "source", label: "来源" },
          { key: "msg", label: "信息" },
        ],
        (pageState.home?.logs || []).slice().reverse()
      )}
    `);
  }

  const bind = (id, action) => {
    const btn = document.getElementById(id);
    if (btn) btn.onclick = async () => doStrategyAction("cta", action);
  };
  bind("btnCtaReload", "reload");
  bind("btnCtaInitAll", "init_all");
  bind("btnCtaStartAll", "start_all");
  bind("btnCtaStopAll", "stop_all");

  const btnRefresh = document.getElementById("btnCtaRefresh");
  if (btnRefresh) btnRefresh.onclick = async () => refreshHome();

  document.querySelectorAll(".cta-row-act").forEach((btn) => {
    btn.onclick = async () => {
      const action = btn.getAttribute("data-action");
      const name = btn.getAttribute("data-name");
      await doStrategyAction("cta", action, name);
    };
  });
}

function renderPortfolioStrategies() {
  const rows = pageState.strategyRows || [];
  const error = pageState.strategyError || "";
  const h = pageState.home || {};

  setControls(`
    <button id="btnPfReload" class="btn ghost">刷新策略类</button>
    <button id="btnPfInitAll" class="btn">全部初始化</button>
    <button id="btnPfStartAll" class="btn submit">全部启动</button>
    <button id="btnPfStopAll" class="btn danger">全部停止</button>
    <button id="btnPfRefresh" class="btn ghost">刷新</button>
  `);

  if (error) {
    setContent(`<div class="bubble ai">组合策略模块加载失败：${escapeHtml(error)}</div>`);
    return;
  }

  const strategyHtml = !rows.length
    ? `<div class="bubble ai">当前没有组合策略。可在桌面端先添加策略后，这里会自动同步显示。</div>`
    : `
      <table class="table">
        <thead>
          <tr>
            <th>策略名</th><th>类名</th><th>合约组</th><th>账户</th><th>已初始化</th><th>状态</th><th>操作</th>
          </tr>
        </thead>
        <tbody>
          ${rows
            .map((r) => {
              const name = escapeHtml(r.strategy_name);
              const symbols = Array.isArray(r.symbols) ? r.symbols.join(", ") : String(r.symbols || "");
              const canInit = !r.inited;
              const canStart = r.inited && !r.trading;
              const canStop = r.trading;
              return `
                <tr>
                  <td>${name}</td>
                  <td>${escapeHtml(r.class_name)}</td>
                  <td>${escapeHtml(symbols)}</td>
                  <td>${escapeHtml(r.gateway_name)}</td>
                  <td>${r.inited ? "是" : "否"}</td>
                  <td>${r.trading ? "运行中" : "已停止"}</td>
                  <td>
                    <button class="btn ghost pf-row-act" data-action="init" data-name="${name}" ${canInit ? "" : "disabled"}>初始化</button>
                    <button class="btn submit pf-row-act" data-action="start" data-name="${name}" ${canStart ? "" : "disabled"}>启动</button>
                    <button class="btn danger pf-row-act" data-action="stop" data-name="${name}" ${canStop ? "" : "disabled"}>停止</button>
                    <button class="btn ghost pf-row-act" data-action="reset" data-name="${name}">重置</button>
                  </td>
                </tr>
              `;
            })
            .join("")}
        </tbody>
      </table>`;

  setContent(`
    <h3>组合策略列表</h3>
    ${strategyHtml}
    <h3>组合盈亏</h3>
    ${tableHtml(
      [
        { key: "reference", label: "组合" },
        { key: "gateway_name", label: "账户" },
        { key: "trading_pnl", label: "交易盈亏" },
        { key: "holding_pnl", label: "持仓盈亏" },
        { key: "total_pnl", label: "总盈亏" },
        { key: "commission", label: "手续费" },
      ],
      h.portfolio_totals || []
    )}
    <h3>合约盈亏</h3>
    ${tableHtml(
      [
        { key: "reference", label: "组合" },
        { key: "vt_symbol", label: "合约" },
        { key: "gateway_name", label: "账户" },
        { key: "last_pos", label: "当前仓位" },
        { key: "total_pnl", label: "总盈亏" },
        { key: "commission", label: "手续费" },
      ],
      h.portfolio_contracts || []
    )}
  `);

  const bind = (id, action) => {
    const btn = document.getElementById(id);
    if (btn) btn.onclick = async () => doStrategyAction("portfolio", action);
  };
  bind("btnPfReload", "reload");
  bind("btnPfInitAll", "init_all");
  bind("btnPfStartAll", "start_all");
  bind("btnPfStopAll", "stop_all");

  const btnRefresh = document.getElementById("btnPfRefresh");
  if (btnRefresh) btnRefresh.onclick = async () => refreshHome();

  document.querySelectorAll(".pf-row-act").forEach((btn) => {
    btn.onclick = async () => {
      const action = btn.getAttribute("data-action");
      const name = btn.getAttribute("data-name");
      await doStrategyAction("portfolio", action, name);
    };
  });
}

function renderScriptStrategies() {
  const rows = pageState.strategyRows || [];
  const error = pageState.strategyError || "";
  const h = pageState.home || {};

  setControls(`
    <button id="btnScStartAll" class="btn submit">全部启动</button>
    <button id="btnScStopAll" class="btn danger">全部停止</button>
    <button id="btnScRefresh" class="btn ghost">刷新</button>
  `);

  if (error) {
    setContent(`<div class="bubble ai">脚本策略模块加载失败：${escapeHtml(error)}</div>`);
    return;
  }

  const table = !rows.length
    ? `<div class="bubble ai">当前没有脚本策略。可在桌面端先添加脚本，这里会自动同步。</div>`
    : `
      <table class="table">
        <thead>
          <tr><th>脚本名</th><th>路径</th><th>状态</th><th>操作</th></tr>
        </thead>
        <tbody>
          ${rows
            .map((r) => {
              const name = escapeHtml(r.script_name);
              const active = !!r.active;
              return `
                <tr>
                  <td>${name}</td>
                  <td>${escapeHtml(r.script_path)}</td>
                  <td>${active ? "运行中" : "已停止"}</td>
                  <td>
                    <button class="btn submit sc-row-act" data-action="start" data-name="${name}" ${active ? "disabled" : ""}>启动</button>
                    <button class="btn danger sc-row-act" data-action="stop" data-name="${name}" ${active ? "" : "disabled"}>停止</button>
                  </td>
                </tr>
              `;
            })
            .join("")}
        </tbody>
      </table>`;

  setContent(`
    <h3>脚本列表</h3>
    ${table}
    <h3>脚本日志</h3>
    ${tableHtml(
      [
        { key: "time", label: "时间" },
        { key: "level", label: "级别" },
        { key: "source", label: "来源" },
        { key: "msg", label: "信息" },
      ],
      (h.logs || []).slice().reverse()
    )}
  `);

  const startAll = document.getElementById("btnScStartAll");
  const stopAll = document.getElementById("btnScStopAll");
  const refresh = document.getElementById("btnScRefresh");

  if (startAll) startAll.onclick = async () => doStrategyAction("script", "start_all");
  if (stopAll) stopAll.onclick = async () => doStrategyAction("script", "stop_all");
  if (refresh) refresh.onclick = async () => refreshHome();

  document.querySelectorAll(".sc-row-act").forEach((btn) => {
    btn.onclick = async () => {
      const action = btn.getAttribute("data-action");
      const name = btn.getAttribute("data-name");
      await doStrategyAction("script", action, name);
    };
  });
}

function renderChart() {
  const h = pageState.home || {};
  const prices = (h.trades || []).map((x) => Number(x.price || 0)).filter((x) => x > 0).slice(-60);

  const width = 1000;
  const height = 260;
  let svg = `<svg width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" style="width:100%;height:auto;border:1px solid rgba(126,151,215,0.25);border-radius:10px;background:rgba(8,14,30,0.55)">`;
  if (prices.length >= 2) {
    const min = Math.min(...prices);
    const max = Math.max(...prices);
    const pad = 24;
    const span = Math.max(max - min, 1e-6);
    const points = prices
      .map((p, i) => {
        const x = pad + ((width - 2 * pad) * i) / (prices.length - 1);
        const y = height - pad - ((height - 2 * pad) * (p - min)) / span;
        return `${x.toFixed(2)},${y.toFixed(2)}`;
      })
      .join(" ");
    svg += `<polyline fill="none" stroke="#4fd0ff" stroke-width="2" points="${points}" />`;
    svg += `<text x="${pad}" y="${pad}" fill="#99a8d8" font-size="12">近${prices.length}笔成交价格</text>`;
    svg += `<text x="${pad}" y="${height - 6}" fill="#99a8d8" font-size="12">min=${min.toFixed(2)} max=${max.toFixed(2)}</text>`;
  } else {
    svg += `<text x="18" y="28" fill="#99a8d8" font-size="13">成交数据不足，暂无可绘制曲线</text>`;
  }
  svg += "</svg>";

  setControls(`<button id="btnRefreshChart" class="btn ghost">刷新图表</button>`);
  setContent(
    `${svg}<h3>最近成交</h3>` +
      tableHtml(
        [
          { key: "symbol", label: "代码" },
          { key: "price", label: "价格" },
          { key: "volume", label: "数量" },
          { key: "datetime", label: "时间" },
          { key: "gateway_name", label: "账户" },
        ],
        h.trades || []
      )
  );
  const btn = document.getElementById("btnRefreshChart");
  if (btn) btn.onclick = async () => refreshHome();
}

function renderBacktest() {
  const h = pageState.home || {};
  const totalOrders = (h.orders || []).length;
  const totalTrades = (h.trades || []).length;
  const fillRate = totalOrders > 0 ? ((totalTrades / totalOrders) * 100).toFixed(2) : "0.00";

  setControls(`<button id="btnRefreshBacktest" class="btn ghost">刷新统计</button>`);
  setContent(
    `<div class="bubble ai">回测页先提供实时统计与委托样本。下一步可以继续接入参数面板和回测执行器。</div>
     <div class="bubble user">当前统计：委托 ${totalOrders} 笔，成交 ${totalTrades} 笔，成交转化率 ${fillRate}%</div>
     <h3>委托样本</h3>` +
      tableHtml(
        [
          { key: "symbol", label: "代码" },
          { key: "direction", label: "方向" },
          { key: "offset", label: "开平" },
          { key: "price", label: "价格" },
          { key: "status", label: "状态" },
          { key: "datetime", label: "时间" },
        ],
        h.orders || []
      )
  );
  const btn = document.getElementById("btnRefreshBacktest");
  if (btn) btn.onclick = async () => refreshHome();
}

function renderAdvisor() {
  const h = pageState.home || {};
  setControls(`
    <button id="btnAdvisorRefresh" class="btn ghost">刷新</button>
    <button id="btnAdvisorCancelAll" class="btn danger">全撤</button>
  `);
  setContent(
    `<h3>风控摘要</h3>` +
      tableHtml(
        [
          { key: "name", label: "规则" },
          { key: "parameters", label: "参数" },
          { key: "variables", label: "变量" },
        ],
        (h.risk_rules || []).map((x) => ({
          name: x.name || "",
          parameters: asJson(x.parameters),
          variables: asJson(x.variables),
        }))
      ) +
      `<h3>持仓</h3>` +
      tableHtml(
        [
          { key: "symbol", label: "代码" },
          { key: "direction", label: "方向" },
          { key: "volume", label: "数量" },
          { key: "price", label: "均价" },
          { key: "pnl", label: "盈亏" },
        ],
        h.positions || []
      )
  );

  const r = document.getElementById("btnAdvisorRefresh");
  const c = document.getElementById("btnAdvisorCancelAll");
  if (r) r.onclick = async () => refreshHome();
  if (c) {
    c.onclick = async () => {
      const resp = await apiPost("/api/order/cancel_all", {});
      showToast(resp.ok ? `已全撤 ${resp.count || 0}` : "全撤失败");
      await refreshHome();
    };
  }
}

function renderModule() {
  const slug = pageState.slug;
  setStat(`模式: ${(pageState.home?.mode || "mock").toUpperCase()}`);

  if (slug === "accounts") return renderAccounts();
  if (slug === "ai-settings") return renderAI();
  if (slug === "portfolio-strategy") return renderPortfolioStrategies();
  if (slug === "cta") return renderCtaStrategies();
  if (slug === "script") return renderScriptStrategies();
  if (slug === "paper") return renderPaper();
  if (slug === "advisor") return renderAdvisor();
  if (slug === "chart") return renderChart();
  if (slug === "backtest") return renderBacktest();
  if (slug === "risk") return renderRisk();

  setControls("");
  setContent("<div class='bubble ai'>未识别的模块</div>");
}

async function refreshHome() {
  const homeResp = await apiGet("/api/home");
  if (!homeResp.ok) {
    showToast(`首页数据拉取失败: ${homeResp.error || "未知错误"}`);
    return;
  }

  pageState.home = homeResp.data || {};
  pageState.strategyRows = [];
  pageState.strategyError = "";

  const kind = strategyKindFromSlug(pageState.slug);
  if (kind) {
    const strategyResp = await apiGet(`/api/strategies/${kind}`);
    if (strategyResp.ok) {
      pageState.strategyRows = strategyResp.rows || [];
    } else {
      pageState.strategyError = strategyResp.error || "策略接口异常";
    }
  }

  renderModule();
}

async function bootstrap() {
  pageState.slug = document.body.dataset.slug || "";
  bindClock();
  const metaResp = await apiGet("/api/meta");
  if (metaResp.ok) {
    pageState.meta = metaResp;
  }
  await refreshHome();
  pageState.pollTimer = setInterval(refreshHome, 4000);
}

window.addEventListener("error", (e) => {
  showToast(`前端异常: ${e.message}`);
});

bootstrap().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(err);
  showToast(`初始化失败: ${err.message || err}`);
});
