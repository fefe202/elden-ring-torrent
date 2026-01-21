#!/usr/bin/env python3
import requests
import time
import subprocess
import json
import sys

# Configurazione
PEERS = [f"localhost:{5001 + i}" for i in range(7)]
FILE_NAME = "healing_test_doc.txt"

def get_manifest_locations(filename):
    """Chiede a tutti i peer chi ha il manifest del file"""
    locations = []
    print(f"Scansione rete per '{filename}'...")
    for p in PEERS:
        try:
            # Timeout breve, se un nodo è giù non deve bloccarci
            r = requests.get(f"http://{p}/get_manifest/{filename}", timeout=1)
            if r.status_code == 200:
                locations.append(p)
        except:
            pass
    return locations

def kill_peer(peer_addr):
    """Uccide il container Docker associato a un indirizzo (es. localhost:5002 -> peer2)"""
    # Ricaviamo il nome del container dalla porta
    port = int(peer_addr.split(":")[1])
    peer_num = port - 5000
    container_name = f"peer{peer_num}"
    
    print(f"Killing {container_name} ({peer_addr})...")
    subprocess.run(f"docker stop {container_name}", shell=True, stdout=subprocess.DEVNULL)
    return container_name

def main():
    print("=== TEST: ANTI-ENTROPY & SELF HEALING ===")
    print("=======================================")

    # 1. Caricamento File
    print("Upload file iniziale...")
    upload_peer = PEERS[0]
    # Aggiungiamo 'genre' per supportare anche SemanticPeer adeguatamente
    r = requests.post(f"http://{upload_peer}/store_file", json={
        "filename": f"/app/data/{FILE_NAME}", 
        "metadata": {"type": "test_healing", "genre": "action"}
    })
    
    if r.status_code != 200:
        print("Upload fallito. Test interrotto.")
        sys.exit(1)
    
    time.sleep(2) # Attesa propagazione iniziale
    
    # 2. Verifica Repliche Iniziali
    locs_init = get_manifest_locations(FILE_NAME)
    print(f"Copie trovate su: {locs_init}")
    
    if len(locs_init) < 3:
        print(f"Warning: Ci aspettavamo 3 repliche, ne abbiamo {len(locs_init)}. Continuo comunque.")

    # 3. Identificazione della Vittima
    # Dobbiamo uccidere un nodo che HA il file, ma NON è quello che stiamo usando per il test script
    # e idealmente non il Primary (anche se il protocollo gestisce anche quello), per vedere il repair.
    victim = None
    for p in locs_init:
        if p != upload_peer: # Non uccidiamo il nodo a cui siamo "connessi" mentalmente
            victim = p
            break
            
    if not victim:
        print("Impossibile trovare una vittima valida.")
        sys.exit(1)

    # 4. Sabotaggio
    print(f"Simulazione Guasto su {victim}...")
    container_name = kill_peer(victim)
    
    # Rimuoviamo la vittima dalla lista PEERS per non interrogarla più
    if victim in PEERS: PEERS.remove(victim)

    # 5. Attesa (Failure Detector + Anti-Entropy)
    # Failure timeout = 15s (default in base.py)
    # Anti-entropy sleep = 20-40s (random)
    # Usiamo un ciclo di polling invece di sleep fisso
    max_wait = 90
    print(f"\n Attesa Self-Healing (Polling fino a {max_wait}s)...")
    
    start_time = time.time()
    healed = False
    new_nodes = set()
    locs_final = []

    while time.time() - start_time < max_wait:
        elasped = int(time.time() - start_time)
        print(f"Attesa Self-Healing ({elasped}s)...", end="\r")
        
        current_locs = get_manifest_locations(FILE_NAME)
        # Filtra la vittima se risponde ancora (caching?)
        current_locs = [x for x in current_locs if x != victim]
        
        # Cerca nuovi nodi
        current_new = set(current_locs) - set(locs_init)
        
        # Condizione successo: abbiamo recuperato il numero di repliche O trovato nuovi nodi
        # (Idealmente vogliamo 3 repliche, ma se eravamo partiti con >3, basta tornare a N-1+1)
        if len(current_new) > 0 and len(current_locs) >= 3:
            healed = True
            new_nodes = current_new
            locs_final = current_locs
            break
        
        time.sleep(5)

    print("\n")
    
    # 6. Risultati Finali
    print("Verifica post-guasto...")
    if not locs_final: locs_final = get_manifest_locations(FILE_NAME)

    print(f"Repliche attuali: {locs_final}")
    
    if healed:
        print(f"SUCCESS! Il sistema ha creato nuove repliche su: {new_nodes}")
        print("Il protocollo Anti-Entropy ha rilevato la perdita e riparato il file.")
    elif len(locs_final) < 3:
        print(f"FAIL! Il sistema ha {len(locs_final)} repliche. Non ha riparato il danno dopo {max_wait}s.")
    else:
        print("INCONCLUSIVE. Il numero di repliche è stabile ma non vedo nuovi nodi distinti. (Forse la vittima non era essenziale?)")

    # Cleanup: riavvia il nodo morto per non rompere altri test
    print(f"\n Riavvio {container_name} per pulizia...")
    subprocess.run(f"docker start {container_name}", shell=True, stdout=subprocess.DEVNULL)

if __name__ == "__main__":
    main()