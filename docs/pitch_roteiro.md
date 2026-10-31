# Roteiro de Apresentação do Pitch (Duração Máxima: 10 Minutos)

**Tema**: Ecossistema Distribuído para Processamento de Safras Agrícolas e Análise de Eficiência Sustentável (ESG)  
**Fonte de Dados**: IBGE / SIDRA - Pesquisa LSPA (Levantamento Sistemático da Produção Agrícola, Tabela 6588)  
**Integrantes do Grupo**: [Nome dos Integrantes]

---

## Estrutura Temporal dos 10 Minutos

| Bloco | Minutagem | Tópico Principal | Responsável Sugerido |
| :--- | :--- | :--- | :--- |
| **1. Introdução & Problema** | 00:00 - 01:30 | Apresentação da equipe, contexto do Agronegócio e o desafio de dados do IBGE | Integrante 1 |
| **2. Contexto Arquitetural & Topologia** | 01:30 - 03:30 | Explicação do modelo Manager-Worker, Docker e Diagramas UML | Integrante 2 |
| **3. Tecnologias & Computação Distribuída** | 03:30 - 05:30 | RabbitMQ, AMQP, Paralelismo Interno (Multithreading nos Workers) | Integrante 3 |
| **4. Alinhamento Temático: Agro & ESG** | 05:30 - 07:00 | Relação dos dados com a intensificação sustentável e preservação territorial | Integrante 4 |
| **5. Demonstração Prática ao Vivo** | 07:00 - 09:00 | Execução do Docker, disparo dos jobs, visualização dos Workers e Dashboard | Integrante 5 |
| **6. Conclusão & Perguntas** | 09:00 - 10:00 | Benefícios da solução, escalabilidade do cluster e encerramento | Todos |

---

## Roteiro Detalhado - Fala por Fala

### Bloco 1: Introdução e Motivação (0:00 - 1:30)
* **Slide 1: Título e Identificação do Grupo**
* **Fala sugerida**:
  > *"Olá a todos, professores e colegas. Hoje apresentamos o nosso projeto de Computação Distribuída voltado a um dos setores mais vitais da economia brasileira: o Agronegócio e a sua intersecção com a Sustentabilidade (ESG).*
  > *Mensalmente, o IBGE publica através da pesquisa LSPA dados volumosos e detalhados de previsão de safra de grãos, cana, café e fibras em todos os estados do Brasil.*
  > *Processar, consolidar e extrair indicadores em tempo hábil a partir de uma abordagem centralizada e monolítica gera gargalos de CPU e indisponibilidade. Por isso, projetamos uma arquitetura verdadeiramente distribuída e resiliente."*

---

### Bloco 2: Contexto Arquitetural e Topologia Docker (1:30 - 3:30)
* **Slide 2: Diagrama de Topologia de Nós (UML Deployment)**
* **Fala sugerida**:
  > *"Para atender aos requisitos de alta disponibilidade e desacoplamento, adotamos o padrão **Manager-Worker** com topologia declarativa via Docker Compose.*
  > *Nosso ecossistema é composto por 4 contêineres principais:*
  > *1. **Nó Manager (Coordenador)**: Construído em Python com FastAPI, responsável por consumir a API do SIDRA/IBGE, particionar o dataset de safras e despachar os pacotes de trabalho.*
  > *2. **Message Broker (RabbitMQ)**: Atua como a espinha dorsal de comunicação assíncrona, desacoplando o coordenador dos nós de cálculo.*
  > *3 e 4. **Nós Workers (Worker Alpha e Worker Beta)**: Nós especializados em processamento analítico que competem pelas tarefas de forma balanceada.*
  > *A justificativa para essa escolha é a **escalabilidade horizontal**: se o volume de safras triplicar, basta subir mais workers com um comando Docker sem alterar nenhuma linha de código do Manager."*

---

### Bloco 3: Tecnologias e Paralelismo Interno (3:30 - 5:30)
* **Slide 3: Diagrama de Sequência & Arquitetura de Threads**
* **Fala sugerida**:
  > *"No quesito tecnológico, implementamos dois níveis de concorrência:*
  > *Primeiro, a **Computação Distribuída Externa**: Utilizamos o protocolo **AMQP 0-9-1** via RabbitMQ com o padrão **Competing Consumers** e entrega justa (*prefetch count = 1*). O Manager publica as mensagens na fila `agro_tasks_queue` com persistência em disco.*
  > *Segundo, o **Paralelismo Interno no Worker (Critério 3 da avaliação)**: Cada nó Worker não processa os dados de forma sequencial. Ele instancia um pool de threads nativo utilizando o `concurrent.futures.ThreadPoolExecutor` com 4 threads simultâneas.*
  > *Assim, enquanto o container processa a cultura da Soja, ele calcula em paralelo os indicadores dos estados de Mato Grosso, Paraná, Rio Grande do Sul e Goiás, reduzindo drasticamente o tempo de latência de cálculo estatístico."*

---

### Bloco 4: Alinhamento Temático - Agronegócio & ESG (5:30 - 7:00)
* **Slide 4: Métricas ESG & Intensificação Sustentável**
* **Fala sugerida**:
  > *"Um dos pontos centrais da nossa solução é o valor dos dados de negócio. Consumimos a Tabela 6588 do SIDRA focando nas culturas de Soja, Milho, Cana, Café, Trigo, Algodão e Arroz.*
  > *Para além do volume de toneladas, o sistema calcula o **Índice de Produtividade (kg/hectare)** e cruza com a taxa de expansão territorial.*
  > *Isso responde à pergunta crítica de **ESG**: 'O Brasil está produzindo mais porque está derrubando mais mata, ou porque está utilizando biotecnologia e manejo sustentável?'*
  > *Nosso sistema atribui um Score ESG que premia safras onde o ganho de produtividade supera a abertura de novas áreas, demonstrando a **intensificação sustentável** do campo."*

---

### Bloco 5: Demonstração Prática ao Vivo (7:00 - 9:00)
* **Tela Compartilhada**: Terminal e Navegador em `http://localhost:8000`
* **Passos da Demonstração**:
  1. Mostrar o terminal com os 4 containers rodando (`docker ps` ou `docker compose logs`).
  2. Acessar o Dashboard Web do Manager: destacar a topologia visual com os nós Manager, RabbitMQ e os Workers Alpha e Beta.
  3. Clicar no botão **"Iniciar Processamento Distribuído"**.
  4. Mostrar no Terminal de Logs os jobs sendo enviados e consumidos alternadamente entre Worker Alpha e Worker Beta.
  5. Mostrar a **Tabela de Auditoria**: apontar a coluna *"Threads Utilizadas"* comprovando o paralelismo interno de cada Worker.
  6. Mostrar os **Gráficos em tempo real**: volume da safra nacional e curva de sustentabilidade ESG.

---

### Bloco 6: Conclusão e Encerramento (9:00 - 10:00)
* **Slide 5: Resultados, Repositório GitHub e Agradecimentos**
* **Fala sugerida**:
  > *"Em conclusão, conseguimos entregar uma solução completa de ponta a ponta: ingestão resiliente da API do SIDRA, mensageria assíncrona industrial com RabbitMQ, paralelismo interno com multithreading e visualização gerencial em tempo real.*
  > *Todo o projeto é 100% reproduzível via Docker com um único comando, conforme documentado no README do nosso GitHub.*
  > *Agradecemos a atenção da banca e estamos abertos a perguntas!"*
