#!/bin/bash

# --- CONFIGURAZIONE SCALABILITÀ ---
# Se cambi questo numero nello script di creazione, cambialo anche qui!
TOTAL_PEERS=20
REPO_URL="https://github.com/fefe202/elden-ring-torrent.git"
# ----------------------------------

# 1. Installazione Dipendenze
apt-get update
apt-get install -y docker.io git python3-pip

# 2. Setup Progetto
cd /home
# Clona fresco ogni volta per avere l'ultima versione
git clone $REPO_URL p2p-project
cd p2p-project

# 3. Build Docker (Lo facciamo su ogni nodo, ci mette 1 min)
docker build -t p2p-node -f peer/Dockerfile .

# 4. Configurazione Dinamica P2P
MY_HOSTNAME=$(hostname)

# Generazione dinamica della lista KNOWN_PEERS
# Crea una stringa tipo: "peer-1:5000,peer-2:5000,...,peer-20:5000"
ALL_PEERS=""
for i in $(seq 1 $TOTAL_PEERS); do
  if [ "$i" -eq 1 ]; then
    ALL_PEERS="peer-$i:5000"
  else
    ALL_PEERS="$ALL_PEERS,peer-$i:5000"
  fi
done

# 5. Recupero la modalità dai Metadati di Google Cloud
# Questo ci permette di lanciare peer NAIVE o METADATA senza cambiare codice
PEER_MODE=$(curl -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/PEER_MODE)

# 6. Avvio Container
# Usiamo --network host per massimizzare le performance di rete (no bridge overhead)
docker run -d \
  --name p2p-container \
  --network host \
  --restart always \
  -e PORT=5000 \
  -e DATA_DIR=/app/data \
  -e SELF_ID="$MY_HOSTNAME:5000" \
  -e KNOWN_PEERS="$ALL_PEERS" \
  -e PEER_MODE="$PEER_MODE" \
  -v /var/data:/app/data \
  p2p-node