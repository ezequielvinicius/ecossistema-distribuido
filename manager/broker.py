"""
Módulo de Integração com Message Broker (RabbitMQ) no Nó Manager.
Gerencia a fila de tarefas distribuídas e o consumo assíncrono de resultados.
"""

import os
import json
import time
import logging
import threading
import pika
from typing import Dict, List, Any, Callable

logger = logging.getLogger("manager_broker")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")

QUEUE_TASKS = "agro_tasks_queue"
QUEUE_RESULTS = "agro_results_queue"

class ManagerBroker:
    def __init__(self, on_result_callback: Callable[[Dict[str, Any]], None] = None):
        self.host = RABBITMQ_HOST
        self.port = RABBITMQ_PORT
        self.credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        self.on_result_callback = on_result_callback
        
        self.connection = None
        self.channel = None
        self.is_connected = False
        self.lock = threading.Lock()
        
        # Thread para escuta de resultados assíncronos
        self.consumer_thread = None
        self.should_stop = False

    def connect(self, max_retries: int = 15, retry_delay: int = 3):
        """Estabelece conexão com o RabbitMQ com política de retentativas."""
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Tentando conectar ao RabbitMQ ({self.host}:{self.port}) - Tentativa {attempt}/{max_retries}...")
                parameters = pika.ConnectionParameters(
                    host=self.host,
                    port=self.port,
                    credentials=self.credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300
                )
                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()
                
                # Declarar filas duráveis
                self.channel.queue_declare(queue=QUEUE_TASKS, durable=True)
                self.channel.queue_declare(queue=QUEUE_RESULTS, durable=True)
                
                self.is_connected = True
                logger.info("Conexão com RabbitMQ estabelecida com sucesso!")
                return True
            except Exception as e:
                logger.warning(f"Falha ao conectar no RabbitMQ ({e}). Nova tentativa em {retry_delay}s...")
                time.sleep(retry_delay)

        logger.error("Não foi possível conectar ao RabbitMQ após todas as tentativas.")
        self.is_connected = False
        return False

    def publish_task(self, task_data: Dict[str, Any]) -> bool:
        """Publica um pacote de processamento na fila de tarefas."""
        with self.lock:
            try:
                if not self.channel or self.channel.is_closed:
                    self.connect()

                message = json.dumps(task_data)
                self.channel.basic_publish(
                    exchange="",
                    routing_key=QUEUE_TASKS,
                    body=message,
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # Mensagem persistente
                        content_type="application/json"
                    )
                )
                logger.info(f"Tarefa enviada: {task_data.get('job_id')} ({task_data.get('cultura_nome')})")
                return True
            except Exception as e:
                logger.error(f"Erro ao publicar tarefa {task_data.get('job_id')}: {e}")
                return False

    def start_results_consumer(self):
        """Inicia thread de segundo plano para processar resultados devolvidos pelos Workers."""
        def consumer_loop():
            logger.info("Thread de consumo de resultados iniciada no Manager.")
            while not self.should_stop:
                try:
                    parameters = pika.ConnectionParameters(
                        host=self.host,
                        port=self.port,
                        credentials=self.credentials,
                        heartbeat=600
                    )
                    conn = pika.BlockingConnection(parameters)
                    ch = conn.channel()
                    ch.queue_declare(queue=QUEUE_RESULTS, durable=True)
                    ch.basic_qos(prefetch_count=1)

                    def callback(ch_inner, method, properties, body):
                        try:
                            result_data = json.loads(body.decode("utf-8"))
                            logger.info(f"Resultado recebido do {result_data.get('worker_id')} para job {result_data.get('job_id')}")
                            if self.on_result_callback:
                                self.on_result_callback(result_data)
                            ch_inner.basic_ack(delivery_tag=method.delivery_tag)
                        except Exception as err:
                            logger.error(f"Erro ao processar callback de resultado: {err}")
                            ch_inner.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

                    ch.basic_consume(queue=QUEUE_RESULTS, on_message_callback=callback)
                    ch.start_consuming()
                except Exception as ex:
                    if not self.should_stop:
                        logger.warning(f"Reconexão necessária no consumer de resultados ({ex}). Aguardando 4s...")
                        time.sleep(4)

        self.consumer_thread = threading.Thread(target=consumer_loop, daemon=True)
        self.consumer_thread.start()

    def close(self):
        self.should_stop = True
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except Exception:
            pass
