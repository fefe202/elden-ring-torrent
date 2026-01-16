FROM python:3.11-slim

# -------------------------------------------------------------------
# 🛠 Installazione strumenti di base
# -------------------------------------------------------------------
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# -------------------------------------------------------------------
# 📂 Cartella di lavoro nel container
# -------------------------------------------------------------------
WORKDIR /app

# -------------------------------------------------------------------
# 📦 Installazione dipendenze Python
# Assumiamo che requirements.txt sia dentro la cartella "peer/" insieme a questo Dockerfile
# -------------------------------------------------------------------
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# -------------------------------------------------------------------
# 🔄 Copia tutto il codice Python
# Dato che il build context è la cartella "peer/", copiando "."
# stiamo copiando direttamente main.py, base.py, api.py, ecc. dentro /app/
# -------------------------------------------------------------------
COPY . /app/

# -------------------------------------------------------------------
# 📁 Directory dati
# -------------------------------------------------------------------
RUN mkdir -p /app/data

# -------------------------------------------------------------------
# ⚙️ File Python da eseguire
# CAMBIAMENTO: Puntiamo al nuovo "cervello" main.py
# -------------------------------------------------------------------
ENV PEER_FILE=main.py

# -------------------------------------------------------------------
# 🚀 Avvio
# -------------------------------------------------------------------
CMD ["sh", "-c", "python $PEER_FILE"]