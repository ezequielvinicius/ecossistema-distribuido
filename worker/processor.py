"""
Módulo de Processamento Analítico dos Nós Workers.
Implementa paralelismo interno efetivo via Multithreading (concurrent.futures.ThreadPoolExecutor)
para cálculo de métricas agronômicas e índices de sustentabilidade ESG.
"""

import time
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any

# Quantidade de threads do pool interno configurável via variável de ambiente
NUM_WORKER_THREADS = int(os.getenv("WORKER_THREADS", "4"))

def _process_uf_record(record: Dict[str, Any], cultura_nome: str) -> Dict[str, Any]:
    """
    Função executada dentro de uma Thread individual do pool interno.
    Calcula indicadores de produtividade, variações sazonais e métricas ESG por UF.
    """
    current_thread = threading.current_thread()
    thread_name = current_thread.name
    thread_ident = current_thread.ident
    
    start_time = time.time()
    
    # Simula pequena latência de cálculo analítico estatístico
    time.sleep(0.08)

    prod_t = record.get("producao_estimada_t", 0)
    prod_ant_t = record.get("producao_anterior_t", 1)
    area_ha = record.get("area_colhida_ha", 1)
    area_ant_ha = record.get("area_anterior_ha", 1)

    # 1. Produtividade em kg/hectare: (toneladas * 1000) / hectares
    produtividade_kg_ha = round((prod_t * 1000) / max(area_ha, 1), 2)
    produtividade_ant_kg_ha = round((prod_ant_t * 1000) / max(area_ant_ha, 1), 2)

    # 2. Variação percentual de produção e área
    var_producao_pct = round(((prod_t - prod_ant_t) / max(prod_ant_t, 1)) * 100, 2)
    var_area_pct = round(((area_ha - area_ant_ha) / max(area_ant_ha, 1)) * 100, 2)
    var_produtividade_pct = round(((produtividade_kg_ha - produtividade_ant_kg_ha) / max(produtividade_ant_kg_ha, 1)) * 100, 2)

    # 3. Análise ESG / Intensificação Sustentável:
    # Produzir mais sem necessariamente expandir a área na mesma proporção (Preservação de Biomas)
    if var_producao_pct > var_area_pct and var_produtividade_pct > 0:
        esg_classificacao = "Excelente (Intensificação Sustentável)"
        esg_score = 95
        esg_comentario = "Crescimento impulsionado por tecnologia e produtividade, desacelerando pressão sobre novas áreas."
    elif var_producao_pct >= 0 and var_area_pct <= 0:
        esg_classificacao = "Ótimo (Otimização Territorial)"
        esg_score = 90
        esg_comentario = "Aumento de safra com redução de área territorial ocupada."
    else:
        esg_classificacao = "Moderado (Expansão Convencional)"
        esg_score = 75
        esg_comentario = "Crescimento alinhado à expansão da área plantada."

    elapsed_ms = round((time.time() - start_time) * 1000, 2)

    return {
        "uf": record.get("uf"),
        "uf_nome": record.get("uf_nome"),
        "regiao": record.get("regiao"),
        "producao_t": prod_t,
        "area_ha": area_ha,
        "produtividade_kg_ha": produtividade_kg_ha,
        "variacao_producao_pct": var_producao_pct,
        "variacao_area_pct": var_area_pct,
        "variacao_produtividade_pct": var_produtividade_pct,
        "esg_classificacao": esg_classificacao,
        "esg_score": esg_score,
        "esg_comentario": esg_comentario,
        # Metadados de paralelismo interno para auditoria
        "processed_by_thread": thread_name,
        "thread_ident": thread_ident,
        "elapsed_ms": elapsed_ms
    }

class CropProcessor:
    """
    Coordenador do processamento analítico paralelo com ThreadPoolExecutor.
    """

    def __init__(self, num_threads: int = NUM_WORKER_THREADS):
        self.num_threads = num_threads

    def process_job(self, job_data: Dict[str, Any], worker_id: str) -> Dict[str, Any]:
        """
        Processa o pacote de trabalho do LSPA utilizando multithreading interno.
        """
        job_id = job_data.get("job_id")
        cultura_nome = job_data.get("cultura_nome", "Desconhecida")
        records = job_data.get("records", [])

        overall_start = time.time()
        processed_uf_results = []
        threads_utilizadas = set()

        # Execução com Paralelismo Interno (ThreadPoolExecutor)
        with ThreadPoolExecutor(max_workers=self.num_threads, thread_name_prefix=f"{worker_id}-th") as executor:
            future_to_record = {
                executor.submit(_process_uf_record, rec, cultura_nome): rec
                for rec in records
            }

            for future in as_completed(future_to_record):
                try:
                    res = future.result()
                    processed_uf_results.append(res)
                    threads_utilizadas.add(res["processed_by_thread"])
                except Exception as e:
                    rec = future_to_record[future]
                    print(f"[{worker_id}] Erro ao processar UF {rec.get('uf')}: {e}")

        # Ordenar os resultados por volume de produção (decrescente)
        processed_uf_results.sort(key=lambda x: x["producao_t"], reverse=True)

        # Agregações Consolidadas da Cultura
        total_producao_t = sum(r["producao_t"] for r in processed_uf_results)
        total_area_ha = sum(r["area_ha"] for r in processed_uf_results)
        produtividade_media = round((total_producao_t * 1000) / max(total_area_ha, 1), 2)
        media_esg_score = round(sum(r["esg_score"] for r in processed_uf_results) / max(len(processed_uf_results), 1), 1)

        total_elapsed_ms = round((time.time() - overall_start) * 1000, 2)

        return {
            "job_id": job_id,
            "cultura_id": job_data.get("cultura_id"),
            "cultura_nome": cultura_nome,
            "categoria": job_data.get("categoria"),
            "worker_id": worker_id,
            "processed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "parallelism_metrics": {
                "multithreading_enabled": True,
                "pool_size": self.num_threads,
                "active_threads_count": len(threads_utilizadas),
                "threads_names": list(threads_utilizadas),
                "total_records_processed": len(processed_uf_results),
                "total_processing_ms": total_elapsed_ms
            },
            "kpis": {
                "total_producao_t": total_producao_t,
                "total_area_ha": total_area_ha,
                "produtividade_media_kg_ha": produtividade_media,
                "esg_score_medio": media_esg_score,
                "uf_lider": processed_uf_results[0]["uf_nome"] if processed_uf_results else "N/A",
                "uf_lider_producao_t": processed_uf_results[0]["producao_t"] if processed_uf_results else 0
            },
            "ufs_detalhes": processed_uf_results
        }
