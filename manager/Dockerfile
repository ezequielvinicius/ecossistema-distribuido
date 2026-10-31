# Dockerfile para o Nó Manager (Coordenador)
FROM python:3.11-slim

# Evita criação de arquivos .pyc e força flush imediato nos logs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Instala dependências do sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copia e instala dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia código-fonte da aplicação
COPY . .

EXPOSE 8000

CMD ["python", "app.py"]
