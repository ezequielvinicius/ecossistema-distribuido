# Ecossistema Distribuído: Processamento Agrícola & Métricas ESG (LSPA / IBGE)

[![Docker](https://img.shields.io/badge/Docker-20.10+-blue.svg?logo=docker&logoColor=white)](https://www.docker.com/)
[![Docker Compose](https://img.shields.io/badge/Docker%20Compose-v2+-blue.svg?logo=docker&logoColor=white)](https://docs.docker.com/compose/)
[![Python](https://img.shields.io/badge/Python-3.11-yellow.svg?logo=python&logoColor=white)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-green.svg?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com/)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-3--Management-orange.svg?logo=rabbitmq&logoColor=white)](https://www.rabbitmq.com/)

Projeto acadêmico de **Sistemas Distribuídos / Computação Distribuída** desenvolvido com foco na ingestão, particionamento e processamento concorrente de dados públicos da pesquisa **LSPA (Levantamento Sistemático da Produção Agrícola)** do **SIDRA / IBGE (Tabela 6588)**.

O sistema calcula indicadores de volume de safra, produtividade por hectare ($kg/ha$) e métricas de **Intensificação Produtiva Sustentável (Score ESG)**, demonstrando se o aumento da produção decorre de ganhos de produtividade e tecnologia ou da expansão de área territorial plantada.

---

## 📌 Topologia do Sistema Distribuído

O ecossistema é composto por nós com papéis explícitos orquestrados via contêineres Docker:

```text
                                [ API SIDRA / IBGE ]
                                         │ (HTTP LSPA)
                                         ▼
                            ┌────────────────────────┐
                            │   NÓ MANAGER (Coord.)  │ (Port 8000)
                            │   FastAPI + Dashboard  │
                            └───────────┬────────────┘
                                        │ (AMQP Despacho)
                                        ▼
                            ┌────────────────────────┐
                            │     RABBITMQ BROKER    │ (Port 5672 / 15672)
                            │  Filas: tasks & results│
                            └─────┬────────────┬─────┘
                                  │            │
            ┌─────────────────────┘            └─────────────────────┐
            ▼                                                        ▼
┌────────────────────────┐                               ┌────────────────────────┐
│  NÓ WORKER ALPHA (1)   │                               │  NÓ WORKER BETA (2)    │
│  4 Threads Internas    │                               │  4 Threads Internas    │
│ (Produtividade + ESG)  │                               │ (Produtividade + ESG)  │
└────────────────────────┘                               └────────────────────────┘
```

1. **Nó Manager (`manager`)**: Coordenador da arquitetura. Realiza a ingestão e particionamento dos dados do SIDRA, publica as tarefas no broker, consome os resultados assíncronos e disponibiliza a API e o Dashboard web em tempo real.
2. **Message Broker (`rabbitmq`)**: Middleware de mensageria AMQP responsável pelo desacoplamento e entrega balanceada das tarefas (*Competing Consumers Pattern*).
3. **Nós Workers (`worker-alpha` e `worker-beta`)**: Nós de computação distribuída que consomem as tarefas da fila e utilizam **paralelismo interno via Multithreading (`ThreadPoolExecutor`)** para processar simultaneamente as UFs de cada cultura agrícola.

---

## 🚀 POP: Procedimento Operacional Padrão (Passo a Passo de Execução)

Siga os passos abaixo para subir e rodar todo o ecossistema distribuído utilizando o Docker.

### 1. Pré-requisitos
Certifique-se de ter instalado em sua máquina:
* [Docker](https://docs.docker.com/get-docker/) (versão 20.10 ou superior)
* [Docker Compose](https://docs.docker.com/compose/install/) (v2.0+)

Verifique no terminal:
```bash
docker --version
docker compose version
```

---

### 2. Clonar o Repositório
```bash
git clone https://github.com/ezequielvinicius/ecossistema-distribuido.git
cd ecossistema-distribuido
```

---

### 3. Construir e Subir os Contêineres
Execute o comando abaixo na raiz do projeto para compilar as imagens e inicializar os 4 nós:

```bash
docker compose up --build
```

> **Dica**: Para rodar em segundo plano (modo detached), utilize:
> ```bash
> docker compose up -d --build
> ```

---

### 4. Acessar os Serviços

Após a inicialização dos contêineres, os seguintes serviços estarão disponíveis:

| Serviço | URL de Acesso | Descrição / Credenciais |
| :--- | :--- | :--- |
| **Dashboard do Manager** | [http://localhost:8000](http://localhost:8000) | Interface gráfica com monitoramento de nós, gráficos e auditoria de threads |
| **Documentação da API (Swagger)** | [http://localhost:8000/docs](http://localhost:8000/docs) | Especificação OpenAPI interativa de todos os endpoints REST |
| **Painel de Gestão do RabbitMQ** | [http://localhost:15672](http://localhost:15672) | Gestão de filas AMQP (Usuário: `guest` \| Senha: `guest`) |

---

### 5. Como Demonstrar o Funcionamento

1. Abra o navegador em **[http://localhost:8000](http://localhost:8000)**.
2. Observe na tela a **Topologia dos Nós** com status verde do RabbitMQ e tempo de atividade do Manager.
3. Clique no botão verde: **"⚡ Iniciar Processamento Distribuído"**.
4. No terminal de logs em tempo real (lado direito da tela) você verá:
   - O Manager particionando os dados do LSPA do IBGE em Jobs por cultura (Soja, Milho, Cana, Trigo, Café, Algodão, Arroz).
   - O **Worker Alpha** e o **Worker Beta** consumindo as tarefas de forma alternada e balanceada.
   - Cada Worker disparando seu **pool de 4 threads simultâneas** para processar os estados.
5. Os gráficos de **Safra Nacional** e de **Produtividade vs ESG** serão preenchidos automaticamente na tela.
6. A tabela de **Auditoria de Execução** exibe o ID exato do nó e a contagem de threads utilizadas em cada cálculo.

---

### 6. Como Escalar Dinamicamente os Workers (Escalabilidade Horizontal)

Para demonstrar a facilidade de expansão do cluster, você pode instanciar mais nós workers em tempo de execução:

```bash
docker compose up -d --scale worker-alpha=3
```
O RabbitMQ distribuirá a carga automaticamente entre os novos contêineres criados, sem necessidade de reiniciar o Manager!

---

### 7. Parar o Ecossistema
Para encerrar a execução de todos os contêineres:
```bash
docker compose down
```

---

## 🛠️ Endpoints REST da API do Manager

* `GET /`: Interface Web do Dashboard.
* `POST /api/jobs/dispatch`: Dispara o particionamento da base LSPA e publicação na fila AMQP.
* `GET /api/status`: Retorna o estado operacional dos nós, contagem de jobs e nós ativos.
* `GET /api/results`: Retorna os resultados analíticos processados com métricas de paralelismo.
* `POST /api/reset`: Reinicializa as métricas do cluster para uma nova execução limpa.

---

## 📂 Estrutura de Diretórios do Projeto

```text
ecossistema-distribuido/
├── docker-compose.yml          # Orquestração declarativa de todos os contêineres
├── README.md                   # POP e documentação principal do projeto
├── docs/                       # Artefatos da entrega e apresentação
│   ├── arquitetura.md          # Diagramas UML detalhados (Deployment, Componentes, Sequência)
│   └── pitch_roteiro.md        # Roteiro da apresentação de até 10 minutos
├── manager/                    # Nó Coordenador (Manager)
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── app.py                  # Servidor FastAPI e estado do cluster
│   ├── sidra_client.py         # Ingestor e particionador de dados do SIDRA/IBGE
│   ├── broker.py               # Publicador de jobs e consumidor assíncrono AMQP
│   ├── templates/
│   │   └── index.html          # Interface gráfica do Dashboard
│   └── static/
│       ├── style.css           # Estilização moderna em modo escuro
│       └── app.js              # Polling ao vivo e renderização Chart.js
└── worker/                     # Nós de Computação Distribuída (Workers)
    ├── Dockerfile
    ├── requirements.txt
    ├── worker.py               # Consumidor de tarefas da fila RabbitMQ
    └── processor.py            # Paralelismo interno (ThreadPoolExecutor) e cálculos ESG
```

---

## 👥 Composição do Grupo
* [Nome do Aluno 1] - [Matrícula]
* [Nome do Aluno 2] - [Matrícula]
* [Nome do Aluno 3] - [Matrícula]
* [Nome do Aluno 4] - [Matrícula]
* [Nome do Aluno 5] - [Matrícula]
* [Nome do Aluno 6] - [Matrícula]
