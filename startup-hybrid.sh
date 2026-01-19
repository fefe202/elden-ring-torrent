#!/bin/bash

# --- CONFIGURAZIONE ---
REPO_URL="https://github.com/fefe202/elden-ring-torrent.git"
CONTAINERS_PER_VM=5
TOTAL_WORKERS=4
# ----------------------

# Salva tutti i log in un file per debug facile
exec > >(tee /var/log/p2p-install.log) 2>&1

echo "🚀 Inizio Setup..."

# 1. Installazione Base
apt-get update
apt-get install -y docker.io git python3-pip

# 2. Clona e Build
cd /home
rm -rf p2p-project  # Pulizia per evitare errori se la cartella esiste già
git clone $REPO_URL p2p-project
cd p2p-project

echo "🛠️ Building Docker Image..."
# Cerca il Dockerfile: supporta sia root che cartella peer/
if [ -f "peer/Dockerfile" ]; then
    docker build -t p2p-node -f peer/Dockerfile .
else
    echo "⚠️ Dockerfile non trovato in peer/, provo nella root..."
    docker build -t p2p-node .
fi

# CONTROLLO SICUREZZA: Verifica se l'immagine esiste
if [[ "$(docker images -q p2p-node 2> /dev/null)" == "" ]]; then
  echo "❌ ERRORE CRITICO: Docker build fallita. Lo script si ferma qui."
  exit 1
fi

# 3. Calcola KNOWN_PEERS globale (worker-1:5001 ... worker-4:5005)
ALL_PEERS=""
for w in $(seq 1 $TOTAL_WORKERS); do
  for p in $(seq 1 $CONTAINERS_PER_VM); do
    PORT=$((5000 + p))
    ADDR="worker-$w:$PORT"
    if [ -z "$ALL_PEERS" ]; then
      ALL_PEERS="$ADDR"
    else
      ALL_PEERS="$ALL_PEERS,$ADDR"
    fi
  done
done

MY_HOSTNAME=$(hostname)
# Recupera la modalità dai metadata della VM
PEER_MODE=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/PEER_MODE)

# 4. Lancia 5 container su questa macchina
echo "🐳 Avvio container..."
for i in $(seq 1 $CONTAINERS_PER_VM); do
  PORT=$((5000 + i))
  
  # Ogni container ha il suo volume dati separato
  mkdir -p /var/data/peer-$i

  # Mappiamo la porta Host (es. 5001) sulla porta Container (5000)
  docker run -d \
    --name "peer-$i" \
    --restart always \
    -p $PORT:5000 \
    -e PORT=5000 \
    -e DATA_DIR="/app/data" \
    -e SELF_ID="$MY_HOSTNAME:$PORT" \
    -e KNOWN_PEERS="$ALL_PEERS" \
    -e PEER_MODE="$PEER_MODE" \
    -v /var/data/peer-$i:/app/data \
    p2p-node
done

echo "✅ Setup completato!"