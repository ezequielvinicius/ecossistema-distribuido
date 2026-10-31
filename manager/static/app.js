// Script de Controle e Atualização em Tempo Real do Dashboard Distribuído
let cropChartInstance = null;
let productivityChartInstance = null;

document.addEventListener("DOMContentLoaded", () => {
    initCharts();
    setupEventListeners();
    // Iniciar loop de polling para atualização ao vivo
    setInterval(fetchSystemStatus, 1500);
    setInterval(fetchResults, 2000);
    fetchSystemStatus();
    fetchResults();
});

function setupEventListeners() {
    const btnDispatch = document.getElementById("btn-dispatch");
    const btnReset = document.getElementById("btn-reset");

    btnDispatch.addEventListener("click", async () => {
        btnDispatch.disabled = true;
        btnDispatch.innerHTML = `<span class="btn-icon">⏳</span> Despachando...`;

        try {
            const resp = await fetch("/api/jobs/dispatch", { method: "POST" });
            const data = await resp.json();
            console.log("Jobs despachados:", data);
        } catch (err) {
            console.error("Erro ao despachar tarefas:", err);
        } finally {
            setTimeout(() => {
                btnDispatch.disabled = false;
                btnDispatch.innerHTML = `<span class="btn-icon">⚡</span> Iniciar Processamento Distribuído`;
            }, 1200);
        }
    });

    btnReset.addEventListener("click", async () => {
        try {
            await fetch("/api/reset", { method: "POST" });
            resetUI();
        } catch (err) {
            console.error("Erro ao resetar estado:", err);
        }
    });
}

async function fetchSystemStatus() {
    try {
        const resp = await fetch("/api/status");
        if (!resp.ok) return;
        const data = await resp.json();

        // 1. Atualizar Broker Status
        const brokerBadge = document.getElementById("broker-badge");
        const brokerText = document.getElementById("broker-status-text");
        if (data.broker_connected) {
            brokerBadge.classList.add("online");
            brokerText.innerText = "RabbitMQ: Conectado (AMQP)";
        } else {
            brokerBadge.classList.remove("online");
            brokerText.innerText = "RabbitMQ: Desconectado";
        }

        // 2. Uptime e KPIs
        document.getElementById("uptime-val").innerText = formatSeconds(data.uptime_seconds);
        document.getElementById("kpi-dispatched").innerText = data.jobs_dispatched;
        document.getElementById("kpi-completed").innerText = data.jobs_completed;

        const progress = data.jobs_dispatched > 0 
            ? Math.round((data.jobs_completed / data.jobs_dispatched) * 100) 
            : 0;
        document.getElementById("kpi-progress").innerText = `Progresso: ${progress}%`;

        // 3. Atualizar Nós Workers
        updateWorkersView(data.active_workers);

        // 4. Atualizar Logs do Terminal
        updateTerminalLogs(data.logs);

    } catch (err) {
        console.warn("Erro ao buscar status do sistema:", err);
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

            // Calcular média geral de ESG
            const avgEsg = (data.results.reduce((acc, curr) => acc + curr.kpis.esg_score_medio, 0) / data.total_results).toFixed(1);
            document.getElementById("kpi-esg").innerText = `${avgEsg} / 100`;
        }
    } catch (err) {
        console.warn("Erro ao buscar resultados analíticos:", err);
    }
}

function updateWorkersView(workers) {
    const container = document.getElementById("workers-container");
    const badge = document.getElementById("workers-count-badge");
    badge.innerText = `${workers.length} Ativo(s)`;

    if (workers.length === 0) {
        container.innerHTML = `<div class="worker-item-empty">Aguardando registro dos Workers via RabbitMQ...</div>`;
        return;
    }

    container.innerHTML = workers.map(w => `
        <div class="worker-item">
            <div class="worker-meta">
                <h4>${w.worker_id}</h4>
                <p>Pool: ${w.threads_pool} threads • Ativas: ${w.threads_used.length}</p>
                <p style="font-size:0.7rem; color:#64748b;">Última resposta: ${w.last_seen}</p>
            </div>
            <div class="worker-stats">
                <span class="count" style="color:#10b981;">${w.jobs_count}</span>
                <p style="font-size:0.7rem; color:#94a3b8;">tarefas</p>
            </div>
        </div>
    `).join("");

    // Atualiza contadores específicos nos cards da topologia
    const wAlpha = workers.find(w => w.worker_id.includes("alpha") || w.worker_id.includes("1"));
    const wBeta = workers.find(w => w.worker_id.includes("beta") || w.worker_id.includes("2"));

    if (wAlpha) document.getElementById("worker-alpha-jobs").innerText = wAlpha.jobs_count;
    if (wBeta) document.getElementById("worker-beta-jobs").innerText = wBeta.jobs_count;
}

function updateTerminalLogs(logs) {
    const terminal = document.getElementById("terminal-logs");
    if (!logs || logs.length === 0) return;

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
    if (!results || results.length === 0) return;

    tbody.innerHTML = results.map(r => {
        const p = r.parallelism_metrics;
        const k = r.kpis;
        return `
            <tr>
                <td><strong>${escapeHtml(r.cultura_nome)}</strong></td>
                <td><span class="badge-worker">${escapeHtml(r.worker_id)}</span></td>
                <td><span class="badge-thread">${p.active_threads_count} threads (${p.pool_size} pool)</span></td>
                <td>${p.total_processing_ms} ms</td>
                <td>${escapeHtml(k.uf_lider)}</td>
                <td><span class="badge-esg">${k.esg_score_medio} pts</span></td>
            </tr>
        `;
    }).join("");
}

function initCharts() {
    // 1. Gráfico de Produção por Cultura
    const ctxCrop = document.getElementById("cropChart").getContext("2d");
    cropChartInstance = new Chart(ctxCrop, {
        type: "bar",
        data: {
            labels: [],
            datasets: [{
                label: "Produção Estimada (Milhões de Toneladas)",
                data: [],
                backgroundColor: "rgba(16, 185, 129, 0.75)",
                borderColor: "#10b981",
                borderWidth: 1.5,
                borderRadius: 6
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    grid: { color: "rgba(255, 255, 255, 0.05)" },
                    ticks: { color: "#94a3b8" }
                },
                x: {
                    grid: { display: false },
                    ticks: { color: "#94a3b8" }
                }
            },
            plugins: {
                legend: { labels: { color: "#f8fafc" } }
            }
        }
    });

    // 2. Gráfico de Produtividade vs ESG
    const ctxProd = document.getElementById("productivityChart").getContext("2d");
    productivityChartInstance = new Chart(ctxProd, {
        type: "line",
        data: {
            labels: [],
            datasets: [
                {
                    label: "Produtividade Média (kg/hectare)",
                    data: [],
                    borderColor: "#6366f1",
                    backgroundColor: "rgba(99, 102, 241, 0.15)",
                    fill: true,
                    tension: 0.35,
                    borderWidth: 2,
                    yAxisID: "y"
                },
                {
                    label: "Índice Sustentabilidade ESG (0-100)",
                    data: [],
                    borderColor: "#3b82f6",
                    borderDash: [5, 5],
                    borderWidth: 2,
                    pointRadius: 4,
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
                    grid: { color: "rgba(255, 255, 255, 0.05)" },
                    ticks: { color: "#94a3b8" }
                },
                y1: {
                    type: "linear",
                    position: "right",
                    grid: { drawOnChartArea: false },
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
                legend: { labels: { color: "#f8fafc" } }
            }
        }
    });
}

function updateCharts(results) {
    if (!cropChartInstance || !productivityChartInstance) return;

    const labels = results.map(r => r.cultura_nome);
    // Converter para milhões de toneladas para legibilidade
    const producoesMilhoes = results.map(r => (r.kpis.total_producao_t / 1000000).toFixed(2));
    const produtividades = results.map(r => r.kpis.produtividade_media_kg_ha);
    const esgScores = results.map(r => r.kpis.esg_score_medio);

    // Atualiza Gráfico 1
    cropChartInstance.data.labels = labels;
    cropChartInstance.data.datasets[0].data = producoesMilhoes;
    cropChartInstance.update();

    // Atualiza Gráfico 2
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
    document.getElementById("worker-alpha-jobs").innerText = "0";
    document.getElementById("worker-beta-jobs").innerText = "0";

    const tbody = document.getElementById("audit-table-body");
    tbody.innerHTML = `<tr><td colspan="6" class="text-center empty-state">Nenhum processamento executado. Clique em "Iniciar Processamento Distribuído".</td></tr>`;

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
