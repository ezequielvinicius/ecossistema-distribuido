"""
Nó Manager (Coordenador Distribuído) - FastAPI
Gerencia a topologia, ingestão do SIDRA/IBGE, despacho de tarefas e Dashboard em tempo real.
"""

import os
import time
import logging
from typing import Dict, List, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from sidra_client import SidraLSPAClient
from broker import ManagerBroker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
)
logger = logging.getLogger("manager_app")

# Estado em memória do cluster distribuído
state = {
    "system_start_time": time.time(),
    "sidra_status": "ONLINE (LSPA Tabela 6588)",
    "jobs_dispatched": 0,
    "jobs_completed": 0,
    "active_workers": {},  # worker_id -> { "last_seen", "jobs_count", "threads_used" }
    "results": {},         # job_id -> result_dict
    "logs": []
}

def log_event(message: str, category: str = "INFO"):
    entry = {
        "timestamp": time.strftime("%H:%M:%S"),
        "message": message,
        "category": category
    }
    state["logs"].append(entry)
    # Manter os últimos 40 logs
    if len(state["logs"]) > 40:
        state["logs"].pop(0)

def on_result_received(result: Dict[str, Any]):
    """Callback invocado sempre que um Worker conclui uma tarefa e devolve o resultado."""
    job_id = result.get("job_id")
    worker_id = result.get("worker_id", "unknown-worker")
    cultura_nome = result.get("cultura_nome", "")
    parallelism = result.get("parallelism_metrics", {})

    state["results"][job_id] = result
    state["jobs_completed"] = len(state["results"])

    # Atualiza métricas do nó Worker
    if worker_id not in state["active_workers"]:
        state["active_workers"][worker_id] = {
            "worker_id": worker_id,
            "jobs_count": 0,
            "total_ms": 0,
            "last_seen": time.strftime("%H:%M:%S"),
            "threads_pool": parallelism.get("pool_size", 4),
            "threads_used": []
        }

    w_info = state["active_workers"][worker_id]
    w_info["jobs_count"] += 1
    w_info["total_ms"] += parallelism.get("total_processing_ms", 0)
    w_info["last_seen"] = time.strftime("%H:%M:%S")
    w_info["threads_used"] = list(set(w_info["threads_used"] + parallelism.get("threads_names", [])))

    log_event(
        f"Resultado consolidado recebido do [{worker_id}] para {cultura_nome} "
        f"({parallelism.get('active_threads_count')} threads, {parallelism.get('total_processing_ms')}ms)",
        "SUCCESS"
    )

# Instâncias dos serviços
sidra_client = SidraLSPAClient()
broker = ManagerBroker(on_result_callback=on_result_received)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Inicialização do Manager
    logger.info("Iniciando Nó Manager do Ecossistema Distribuído...")
    log_event("Nó Manager iniciado com sucesso.", "SYSTEM")
    
    # Conecta ao RabbitMQ em thread separada para não bloquear o startup
    def init_broker():
        if broker.connect():
            broker.start_results_consumer()
            log_event("Conectado ao RabbitMQ Broker. Fila de tarefas pronta.", "SYSTEM")
        else:
            log_event("Aviso: Falha temporária de conexão com o RabbitMQ. Retentando em background...", "WARNING")

    import threading
    threading.Thread(target=init_broker, daemon=True).start()

    yield
    # Finalização
    broker.close()
    logger.info("Nó Manager finalizado.")

app = FastAPI(
    title="Ecossistema Distribuído - Manager Agro/ESG",
    description="Sistema Distribuído para processamento da produção agrícola nacional (LSPA/IBGE)",
    version="1.0.0",
    lifespan=lifespan
)

# Mapear diretórios estáticos e templates
manager_dir = os.path.dirname(os.path.abspath(__file__))
app.mount("/static", StaticFiles(directory=os.path.join(manager_dir, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(manager_dir, "templates"))

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard(request: Request):
    """Página inicial com Dashboard Web Completo para demonstração no Pitch."""
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "title": "Ecossistema Distribuído - Agronegócio & ESG"
        }
    )

@app.post("/api/jobs/dispatch")
async def dispatch_jobs():
    """
    Ingere dados do LSPA/SIDRA, particiona os dados por cultura e distribui
    as tarefas na fila AMQP para os nós Workers.
    """
    log_event("Iniciando ingestão e particionamento da pesquisa LSPA (SIDRA/IBGE)...", "INFO")
    jobs = sidra_client.create_distributed_jobs()
    dispatched_count = 0

    for job in jobs:
        if broker.publish_task(job):
            dispatched_count += 1
            state["jobs_dispatched"] += 1
            log_event(f"Tarefa {job['job_id']} despachada para o Broker: {job['cultura_nome']}", "INFO")

    return {
        "status": "success",
        "message": f"{dispatched_count} tarefas particionadas e distribuídas aos Workers.",
        "jobs_count": dispatched_count
    }

@app.get("/api/status")
async def get_system_status():
    """Retorna o estado operacional do cluster distribuído e métricas de nós."""
    uptime_seconds = int(time.time() - state["system_start_time"])
    return {
        "uptime_seconds": uptime_seconds,
        "sidra_status": state["sidra_status"],
        "broker_connected": broker.is_connected,
        "jobs_dispatched": state["jobs_dispatched"],
        "jobs_completed": state["jobs_completed"],
        "active_workers": list(state["active_workers"].values()),
        "total_active_workers": len(state["active_workers"]),
        "logs": state["logs"][-15:]
    }

@app.get("/api/results")
async def get_results():
    """Retorna os resultados analíticos consolidados das culturas processadas."""
    return {
        "total_results": len(state["results"]),
        "results": list(state["results"].values())
    }

@app.post("/api/reset")
async def reset_cluster_state():
    """Limpa os resultados para permitir nova demonstração prática ao vivo."""
    state["jobs_dispatched"] = 0
    state["jobs_completed"] = 0
    state["results"].clear()
    state["active_workers"].clear()
    log_event("Estado do cluster reiniciado para nova execução.", "SYSTEM")
    return {"status": "reset_complete"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=False)
