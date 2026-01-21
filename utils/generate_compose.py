import yaml

# Configurazione
NUM_PEERS = 10
BASE_PORT = 5000
HOST_BASE_PORT = 5001

services = {}
# Creiamo la lista completa di tutti i peer per la Full Mesh
all_peers_list = [f"peer{i}:{BASE_PORT}" for i in range(1, NUM_PEERS + 1)]

for i in range(1, NUM_PEERS + 1):
    peer_name = f"peer{i}"
    self_id = f"{peer_name}:{BASE_PORT}"
    
    # Rimuovi se stesso dalla lista known_peers
    known = [p for p in all_peers_list if p != self_id]
    known_str = ",".join(known)
    
    services[peer_name] = {
        "build": {"context": "./peer"},
        "container_name": peer_name,
        "environment": [
            f"PORT={BASE_PORT}",
            "DATA_DIR=/app/data",
            f"KNOWN_PEERS={known_str}",
            f"SELF_ID={self_id}",
            "ISP=isp_a",     # Placeholder per il futuro P4P
            "REGION=region_a", # Placeholder
            
            # --- LA FIX FONDAMENTALE ---
            # Questa sintassi dice: "Prendi PEER_MODE dall'host, se non c'è usa NAIVE"
            "PEER_MODE=${PEER_MODE:-NAIVE}" 
            # ---------------------------
        ],
        "volumes": [f"./data_{peer_name}:/app/data"],
        "ports": [f"{HOST_BASE_PORT + i - 1}:{BASE_PORT}"],
        "networks": ["p2p_net"]
    }

compose_data = {
    "services": services,
    "networks": {"p2p_net": {"driver": "bridge"}}
}

with open("docker-compose.yml", "w") as f:
    yaml.dump(compose_data, f, sort_keys=False)

print(f"✅ Generato docker-compose.yml CORRETTO per {NUM_PEERS} peer.")
print("   - Full Mesh attiva (Known Peers completi)")
print("   - PEER_MODE pass-through attivo")