#!/usr/bin/env python3
import os
import sys
import api

# Importiamo le classi dei Peer

try:
    from naive import NaivePeer
except ImportError as e:
    print(f"⚠️ Warning: NaivePeer import failed: {e}")
    NaivePeer = None
try:
    from metadata import MetadataPeer
except ImportError as e:
    print(f"⚠️ Warning: MetadataPeer import failed: {e}")
    MetadataPeer = None
try:
    from semantic import SemanticPeer
except ImportError as e:
    print(f"⚠️ Warning: SemanticPeer import failed: {e}")
    SemanticPeer = None

def main():
    # ==========================================
    # 1. LETTURA CONFIGURAZIONE (ENV VARS)
    # ==========================================
    # Questi valori vengono passati dal docker-compose.yml

    # Flush immediato dei print per vederli nei log docker
    sys.stdout.reconfigure(line_buffering=True)
    
    DATA_DIR = os.environ.get("DATA_DIR", "/app/data")
    SELF_ID = os.environ.get("SELF_ID", "peer_unknown")
    
    # Parsing della lista dei peer (gestisce spazi e stringhe vuote)
    raw_peers = os.environ.get("KNOWN_PEERS", "")
    KNOWN_PEERS = [p.strip() for p in raw_peers.split(",") if p.strip()]
    
    PORT = int(os.environ.get("PORT", 5000))
    
    # Il flag che decide la logica
    # NAIVE, METADATA, SEMANTIC, P4P
    MODE = os.environ.get("PEER_MODE", "NAIVE").upper()

    print(f"Booting Peer: {SELF_ID}")
    print(f"Mode: {MODE}")
    print(f"Port: {PORT}")
    print(f"Known Peers: {KNOWN_PEERS}")

    # ==========================================
    # 2. FACTORY PATTERN (Istanziazione Classe)
    # ==========================================
    peer_obj = None

    if MODE == "NAIVE":
        print("--> Starting in NAIVE Mode (Flooding Search)")
        if NaivePeer:
            peer_obj = NaivePeer(SELF_ID, KNOWN_PEERS, DATA_DIR)
        else:
            print("ERR: NaivePeer class not found or import failed.")
            sys.exit(1)

    elif MODE == "METADATA":
        print("--> Starting in METADATA-AWARE Mode (GLS Salting)")
        if MetadataPeer:
            peer_obj = MetadataPeer(SELF_ID, KNOWN_PEERS, DATA_DIR)
        else:
            print("ERR: MetadataPeer class not found or import failed.")
            sys.exit(1)
    
    elif MODE == "SEMANTIC":
        print("--> Starting in SEMANTIC PARTITIONING Mode (Document Partitioning)")
        if SemanticPeer:
            peer_obj = SemanticPeer(SELF_ID, KNOWN_PEERS, DATA_DIR)
        else:
            print("ERR: SemanticPeer class not found or import failed.")
            sys.exit(1)

    elif MODE == "P4P":
        print("--> Starting in P4P Mode (Network Optimization)")
        print("ERR: P4P Mode not implemented yet!")
        # from p4p import P4PPeer
        # peer_obj = P4PPeer(SELF_ID, KNOWN_PEERS, DATA_DIR)
        sys.exit(1)

    else:
        print(f"ERR: Unknown mode '{MODE}'. Defaulting to NAIVE.")
        peer_obj = NaivePeer(SELF_ID, KNOWN_PEERS, DATA_DIR)

    # ==========================================
    # 3. AVVIO BACKGROUND TASKS
    # ==========================================
    # Avvia Failure Detector, Gossip, Rejoin, ecc.
    peer_obj.start_background_tasks()

    # ==========================================
    # 4. INIEZIONE DIPENDENZA E AVVIO SERVER
    # ==========================================
    # Collega l'oggetto Peer creato all'API Flask
    api.peer_instance = peer_obj

    print(f"🚀 Server running on 0.0.0.0:{PORT}")
    
    # Disabilita il reloader di Flask in produzione/docker per evitare doppi avvii dei thread
    api.app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

if __name__ == "__main__":
    main()