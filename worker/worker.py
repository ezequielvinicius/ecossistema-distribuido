"""
Nó Worker de Processamento Distribuído.
Consome tarefas da fila AMQP (RabbitMQ), executa análise concorrente com ThreadPool
e publica os resultados consolidados na fila de retorno.
"""

import os
import sys
import json
import time
import signal
import logging
import pika
from processor import CropProcessor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
)
logger = logging.getLogger("worker_node")

WORKER_ID = os.getenv("WORKER_ID", f"worker-{os.getpid()}")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
WORKER_THREADS = int(os.getenv("WORKER_THREADS", "4"))

QUEUE_TASKS = "agro_tasks_queue"
QUEUE_RESULTS = "agro_results_queue"

class WorkerNode:
    def __init__(self):
        self.worker_id = WORKER_ID
        self.processor = CropProcessor(num_threads=WORKER_THREADS)
        self.credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        self.connection = None
        self.channel = None
        self.running = True

    def connect(self, max_retries: int = 20, retry_delay: int = 3):
        """Conecta ao broker RabbitMQ com retentativas."""
        for attempt in range(1, max_retries + 1):
            if not self.running:
                return False
            try:
                logger.info(f"[{self.worker_id}] Conectando ao RabbitMQ ({RABBITMQ_HOST}:{RABBITMQ_PORT}) - Tentativa {attempt}/{max_retries}...")
                parameters = pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    port=RABBITMQ_PORT,
                    credentials=self.credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300
                )
                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()
                
                # Garantir que as filas existem
                self.channel.queue_declare(queue=QUEUE_TASKS, durable=True)
                self.channel.queue_declare(queue=QUEUE_RESULTS, durable=True)
                
                # Fair dispatch: entrega 1 tarefa por vez para cada worker
                self.channel.basic_qos(prefetch_count=1)
                
                logger.info(f"[{self.worker_id}] Conectado com sucesso! Inicializado com pool de {WORKER_THREADS} threads internas.")
                return True
            except Exception as e:
                logger.warning(f"[{self.worker_id}] Falha ao conectar ({e}). Aguardando {retry_delay}s...")
                time.sleep(retry_delay)

        logger.error(f"[{self.worker_id}] Falha crítica ao conectar ao RabbitMQ.")
        return False

    def on_task_received(self, ch, method, properties, body):
        """Callback acionado quando uma nova tarefa chega na fila."""
        try:
            task_data = json.loads(body.decode("utf-8"))
            job_id = task_data.get("job_id")
            cultura = task_data.get("cultura_nome")
            
            logger.info(f"[{self.worker_id}] >>> Nova tarefa recebida: {job_id} | Cultura: {cultura}")
            
            # Processamento paralelo com multithreading interno
            result = self.processor.process_job(task_data, self.worker_id)
            
            # Publica o resultado na fila de retorno para o Manager
            ch.basic_publish(
                exchange="",
                routing_key=QUEUE_RESULTS,
                body=json.dumps(result),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type="application/json"
                )
            )
            
            logger.info(
                f"[{self.worker_id}] <<< Concluído: {job_id} em {result['parallelism_metrics']['total_processing_ms']}ms "
                f"utilizando {result['parallelism_metrics']['active_threads_count']} threads."
            )
            
            # Confirmação do processamento (ACK)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            logger.error(f"[{self.worker_id}] Erro no processamento da tarefa: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def start(self):
        """Loop principal de consumo do Worker."""
        while self.running:
            if not self.connect():
                time.sleep(5)
                continue

            try:
                logger.info(f"[{self.worker_id}] Aguardando tarefas na fila '{QUEUE_TASKS}'...")
                self.channel.basic_consume(
                    queue=QUEUE_TASKS,
                    on_message_callback=self.on_task_received,
                    auto_ack=False
                )
                self.channel.start_consuming()
            except pika.exceptions.ConnectionClosedByBroker:
                logger.warning(f"[{self.worker_id}] Conexão fechada pelo broker. Reconectando...")
            except pika.exceptions.AMQPChannelError as err:
                logger.error(f"[{self.worker_id}] Erro de canal AMQP: {err}. Reconectando...")
            except Exception as e:
                if self.running:
                    logger.warning(f"[{self.worker_id}] Exceção no loop do worker: {e}. Reconectando em 3s...")
                    time.sleep(3)

    def stop(self):
        self.running = False
        try:
            if self.channel and self.channel.is_open:
                self.channel.stop_consuming()
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception:
            pass
        logger.info(f"[{self.worker_id}] Nó finalizado com sucesso.")

def signal_handler(sig, frame):
    logger.info("Sinal de interrupção recebido. Encerrando worker...")
    if 'worker' in globals():
        worker.stop()
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    worker = WorkerNode()
    worker.start()
