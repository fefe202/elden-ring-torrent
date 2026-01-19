#!/bin/bash

# --- CONFIGURAZIONE ---
REPO_URL="https://github.com/fefe202/elden-ring-torrent.git"
CONTAINERS_PER_VM=5
TOTAL_WORKERS=4
# ----------------------

# Logghiamo tutto
exec > >(tee /var/log/p2p-install.log) 2>&1

echo "🚀 Inizio Setup..."

# 1. Installazione Base
apt-get update
apt-get install -y docker.io git python3-pip

# 2. Clona
cd /home
rm -rf p2p-project
git clone $REPO_URL p2p-project
cd p2p-project

echo "🛠️ Building Docker Image..."

# --- MODIFICA FONDAMENTALE QUI SOTTO ---
if [ -f "peer/Dockerfile" ]; then
    echo "📂 Trovata cartella peer/, entro e costruisco da lì..."
    cd peer
    docker build -t p2p-node .
    cd .. # Torniamo indietro alla root del progetto
elif [ -f "Dockerfile" ]; then
    echo "📂 Dockerfile in root, costruisco qui..."
    docker build -t p2p-node .
else
    echo "❌ ERRORE: Dockerfile non trovato nè in root nè in peer/!"
    ls -R
    exit 1
fi
# ---------------------------------------

# CONTROLLO SICUREZZA
if [[ "$(docker images -q p2p-node 2> /dev/null)" == "" ]]; then
  echo "❌ ERRORE CRITICO: Docker build fallita. Lo script si ferma qui."
  exit 1
fi

# 3. Calcola Network
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
PEER_MODE=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/PEER_MODE)

# 4. Lancia Container
echo "🐳 Avvio container..."
for i in $(seq 1 $CONTAINERS_PER_VM); do
  PORT=$((5000 + i))
  mkdir -p /var/data/peer-$i

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