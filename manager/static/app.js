// Script de Controle e Atualização em Tempo Real do Cluster Console
let cropChartInstance = null;
let productivityChartInstance = null;

document.addEventListener("DOMContentLoaded", () => {
    initTabs();
    initCharts();
    setupEventListeners();

    // Polling a cada 1.5s
    setInterval(fetchSystemStatus, 1500);
    setInterval(fetchResults, 2000);
    fetchSystemStatus();
    fetchResults();
});

// Navegação de Abas na Sidebar
function initTabs() {
    const navItems = document.querySelectorAll(".sidebar-nav .nav-item");
    const tabPanes = document.querySelectorAll(".tab-pane");
    const activeTitle = document.getElementById("active-tab-title");

    navItems.forEach(item => {
        item.addEventListener("click", () => {
            const targetTabId = item.getAttribute("data-tab");

            navItems.forEach(n => n.classList.remove("active"));
            tabPanes.forEach(p => p.classList.remove("active"));

            item.classList.add("active");
            const targetPane = document.getElementById(targetTabId);
            if (targetPane) targetPane.classList.add("active");

            // Atualiza breadcrumb
            const label = item.querySelector("span").innerText;
            if (activeTitle) activeTitle.innerText = label;
        });
    });
}

function setupEventListeners() {
    const btnDispatch = document.getElementById("btn-dispatch");
    const btnReset = document.getElementById("btn-reset");

    btnDispatch.addEventListener("click", async () => {
        btnDispatch.disabled = true;
        btnDispatch.innerText = "Distribuindo...";

        try {
            await fetch("/api/jobs/dispatch", { method: "POST" });
        } catch (err) {
            console.error("Falha ao despachar:", err);
        } finally {
            setTimeout(() => {
                btnDispatch.disabled = false;
                btnDispatch.innerText = "Executar Distribuição";
            }, 1200);
        }
    });

    btnReset.addEventListener("click", async () => {
        try {
            await fetch("/api/reset", { method: "POST" });
            resetUI();
        } catch (err) {
            console.error("Falha ao resetar:", err);
        }
    });
}

async function fetchSystemStatus() {
    try {
        const resp = await fetch("/api/status");
        if (!resp.ok) return;
        const data = await resp.json();

        // 1. Indicador do Broker
        const brokerIndicator = document.getElementById("broker-indicator");
        const brokerText = document.getElementById("broker-status-text");
        if (data.broker_connected) {
            brokerIndicator.classList.add("online");
            brokerText.innerText = "RabbitMQ: 5672 (OK)";
        } else {
            brokerIndicator.classList.remove("online");
            brokerText.innerText = "RabbitMQ: Offline";
        }

        // 2. Uptime e KPIs
        document.getElementById("uptime-val").innerText = formatSeconds(data.uptime_seconds);
        document.getElementById("kpi-dispatched").innerText = data.jobs_dispatched;
        document.getElementById("kpi-completed").innerText = data.jobs_completed;

        const progress = data.jobs_dispatched > 0 
            ? Math.round((data.jobs_completed / data.jobs_dispatched) * 100) 
            : 0;
        document.getElementById("kpi-progress").innerText = `Progresso: ${progress}%`;

        // 3. Workers
        updateWorkersView(data.active_workers);

        // 4. Terminal de Logs
        updateTerminalLogs(data.logs);

    } catch (err) {
        console.warn("Erro ao buscar status:", err);
    }
}

async function fetchResults() {
    try {
        const resp = await fetch("/api/results");
        if (!resp.ok) return;
        const data = await resp.json();

        if (data.total_results > 0) {
            updateCharts(data.results);
            updateAuditTable(data.results);
            updateEsgDetails(data.results);

            const avgEsg = (data.results.reduce((acc, curr) => acc + curr.kpis.esg_score_medio, 0) / data.total_results).toFixed(1);
            document.getElementById("kpi-esg").innerText = `${avgEsg} / 100`;
        }
    } catch (err) {
        console.warn("Erro ao buscar resultados:", err);
    }
}

function updateWorkersView(workers) {
    const badge = document.getElementById("registered-workers-badge");
    const container = document.getElementById("workers-list-container");
    if (badge) badge.innerText = `${workers.length} Ativo(s)`;

    if (!container) return;

    if (workers.length === 0) {
        container.innerHTML = `<p class="empty-text">Aguardando registro de respostas via RabbitMQ...</p>`;
        return;
    }

    container.innerHTML = `
        <table class="data-table">
            <thead>
                <tr>
                    <th>Worker ID</th>
                    <th>Pool de Threads</th>
                    <th>Threads Utilizadas</th>
                    <th>Tarefas Concluídas</th>
                    <th>Tempo Médio</th>
                    <th>Último Heartbeat</th>
                </tr>
            </thead>
            <tbody>
                ${workers.map(w => {
                    const avgMs = w.jobs_count > 0 ? (w.total_ms / w.jobs_count).toFixed(1) : 0;
                    return `
                        <tr>
                            <td><strong style="color:#f1f5f9;">${escapeHtml(w.worker_id)}</strong></td>
                            <td>${w.threads_pool} threads</td>
                            <td><span class="badge-tag">${w.threads_used.length} ativas</span></td>
                            <td><strong>${w.jobs_count}</strong></td>
                            <td>${avgMs} ms</td>
                            <td>${w.last_seen}</td>
                        </tr>
                    `;
                }).join("")}
            </tbody>
        </table>
    `;

    // Atualizar contadores na topologia
    const wAlpha = workers.find(w => w.worker_id.includes("alpha") || w.worker_id.includes("1"));
    const wBeta = workers.find(w => w.worker_id.includes("beta") || w.worker_id.includes("2"));
    if (wAlpha) {
        const el = document.getElementById("topo-worker-alpha-jobs");
        if (el) el.innerText = wAlpha.jobs_count;
    }
    if (wBeta) {
        const el = document.getElementById("topo-worker-beta-jobs");
        if (el) el.innerText = wBeta.jobs_count;
    }
}

function updateTerminalLogs(logs) {
    const terminal = document.getElementById("terminal-logs");
    if (!logs || logs.length === 0 || !terminal) return;

    terminal.innerHTML = logs.map(l => {
        let cls = "log-info";
        if (l.category === "SYSTEM") cls = "log-system";
        if (l.category === "SUCCESS") cls = "log-success";
        if (l.category === "WARNING") cls = "log-warning";

        return `<div class="log-line ${cls}">
            <span class="log-time">[${l.timestamp}]</span> ${escapeHtml(l.message)}
        </div>`;
    }).join("");

    terminal.scrollTop = terminal.scrollHeight;
}

function updateAuditTable(results) {
    const tbody = document.getElementById("audit-table-body");
    if (!results || results.length === 0 || !tbody) return;

    tbody.innerHTML = results.map(r => {
        const p = r.parallelism_metrics;
        const k = r.kpis;
        return `
            <tr>
                <td><strong>${escapeHtml(r.cultura_nome)}</strong></td>
                <td><span class="badge-tag">${escapeHtml(r.worker_id)}</span></td>
                <td><span class="badge-tag" style="color:#3b82f6;">${p.active_threads_count} threads (${p.pool_size} pool)</span></td>
                <td>${p.total_processing_ms} ms</td>
                <td>${escapeHtml(k.uf_lider)}</td>
                <td><strong style="color:#10b981;">${k.esg_score_medio} pts</strong></td>
            </tr>
        `;
    }).join("");
}

function updateEsgDetails(results) {
    const container = document.getElementById("esg-details-container");
    if (!container || !results || results.length === 0) return;

    container.innerHTML = `
        <table class="data-table" style="margin-top:1rem;">
            <thead>
                <tr>
                    <th>Cultura</th>
                    <th>Classificação ESG</th>
                    <th>Produtividade Média</th>
                    <th>Área Total Colhida</th>
                    <th>Score de Sustentabilidade</th>
                </tr>
            </thead>
            <tbody>
                ${results.map(r => {
                    const k = r.kpis;
                    return `
                        <tr>
                            <td><strong>${escapeHtml(r.cultura_nome)}</strong></td>
                            <td>Intensificação Sustentável</td>
                            <td>${k.produtividade_media_kg_ha.toLocaleString("pt-BR")} kg/ha</td>
                            <td>${(k.total_area_ha / 1000).toLocaleString("pt-BR")} mil ha</td>
                            <td><strong style="color:#10b981;">${k.esg_score_medio} / 100</strong></td>
                        </tr>
                    `;
                }).join("")}
            </tbody>
        </table>
    `;
}

function initCharts() {
    // 1. Gráfico de Barras - Produção
    const ctxCrop = document.getElementById("cropChart");
    if (ctxCrop) {
        cropChartInstance = new Chart(ctxCrop.getContext("2d"), {
            type: "bar",
            data: {
                labels: [],
                datasets: [{
                    label: "Produção Estimada (Milhões t)",
                    data: [],
                    backgroundColor: "#3b82f6",
                    borderRadius: 4,
                    barPercentage: 0.6
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: {
                        grid: { color: "#202737" },
                        ticks: { color: "#94a3b8" }
                    },
                    x: {
                        grid: { display: false },
                        ticks: { color: "#94a3b8" }
                    }
                },
                plugins: {
                    legend: { display: false }
                }
            }
        });
    }

    // 2. Gráfico de Linha - Produtividade vs ESG
    const ctxProd = document.getElementById("productivityChart");
    if (ctxProd) {
        productivityChartInstance = new Chart(ctxProd.getContext("2d"), {
            type: "line",
            data: {
                labels: [],
                datasets: [
                    {
                        label: "Produtividade (kg/ha)",
                        data: [],
                        borderColor: "#10b981",
                        backgroundColor: "rgba(16, 185, 129, 0.08)",
                        fill: true,
                        tension: 0.2,
                        borderWidth: 2,
                        pointRadius: 3,
                        yAxisID: "y"
                    },
                    {
                        label: "Score ESG (0-100)",
                        data: [],
                        borderColor: "#3b82f6",
                        borderDash: [4, 4],
                        borderWidth: 1.5,
                        pointRadius: 3,
                        yAxisID: "y1"
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: {
                        type: "linear",
                        position: "left",
                        grid: { color: "#202737" },
                        ticks: { color: "#94a3b8" }
                    },
                    y1: {
                        type: "linear",
                        position: "right",
                        grid: { display: false },
                        ticks: { color: "#3b82f6" },
                        min: 50,
                        max: 100
                    },
                    x: {
                        grid: { display: false },
                        ticks: { color: "#94a3b8" }
                    }
                },
                plugins: {
                    legend: { labels: { color: "#94a3b8" } }
                }
            }
        });
    }
}

function updateCharts(results) {
    if (!cropChartInstance || !productivityChartInstance) return;

    const labels = results.map(r => r.cultura_nome);
    const producoesMilhoes = results.map(r => (r.kpis.total_producao_t / 1000000).toFixed(2));
    const produtividades = results.map(r => r.kpis.produtividade_media_kg_ha);
    const esgScores = results.map(r => r.kpis.esg_score_medio);

    cropChartInstance.data.labels = labels;
    cropChartInstance.data.datasets[0].data = producoesMilhoes;
    cropChartInstance.update();

    productivityChartInstance.data.labels = labels;
    productivityChartInstance.data.datasets[0].data = produtividades;
    productivityChartInstance.data.datasets[1].data = esgScores;
    productivityChartInstance.update();
}

function resetUI() {
    document.getElementById("kpi-dispatched").innerText = "0";
    document.getElementById("kpi-completed").innerText = "0";
    document.getElementById("kpi-progress").innerText = "Progresso: 0%";
    document.getElementById("kpi-esg").innerText = "--";

    const topoA = document.getElementById("topo-worker-alpha-jobs");
    const topoB = document.getElementById("topo-worker-beta-jobs");
    if (topoA) topoA.innerText = "0";
    if (topoB) topoB.innerText = "0";

    const tbody = document.getElementById("audit-table-body");
    if (tbody) tbody.innerHTML = `<tr><td colspan="6" class="empty-text">Nenhuma execução registrada. Clique em "Executar Distribuição" no topo.</td></tr>`;

    if (cropChartInstance) {
        cropChartInstance.data.labels = [];
        cropChartInstance.data.datasets[0].data = [];
        cropChartInstance.update();
    }
    if (productivityChartInstance) {
        productivityChartInstance.data.labels = [];
        productivityChartInstance.data.datasets[0].data = [];
        productivityChartInstance.data.datasets[1].data = [];
        productivityChartInstance.update();
    }
}

function formatSeconds(secs) {
    const mins = Math.floor(secs / 60);
    const s = secs % 60;
    return `${mins}m ${s}s`;
}

function escapeHtml(text) {
    if (!text) return "";
    return String(text)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}
