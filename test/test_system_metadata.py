#!/usr/bin/env python3
import os
import requests
import hashlib
import time
import subprocess
import random
import string

# -------------------------------------------------------------------
# 🔧 CONFIGURAZIONE
# -------------------------------------------------------------------
PEERS = [
    "localhost:5001",
    "localhost:5002",
    "localhost:5003",
    "localhost:5004",
    "localhost:5005",
    "localhost:5006",
    "localhost:5007"
]

# Directory locale dove creare i file dummy temporanei
TEST_DATA_DIR = "test_data_gen"

# -------------------------------------------------------------------
# ⚙️ GENERATORE FILE DUMMY
# -------------------------------------------------------------------
def generate_dummy_file(filename, size_mb=1):
    """Crea un file di test con contenuto casuale"""
    if not os.path.exists(TEST_DATA_DIR):
        os.makedirs(TEST_DATA_DIR)
    
    filepath = os.path.join(TEST_DATA_DIR, filename)
    
    # Se esiste già, non ricrearlo per risparmiare tempo (a meno che size non cambi)
    if os.path.exists(filepath):
        return filepath

    print(f"🔨 Generazione file dummy '{filename}' ({size_mb} MB)...")
    with open(filepath, "wb") as f:
        # Scrive byte casuali
        f.write(os.urandom(int(size_mb * 1024 * 1024)))
    return filepath

def cleanup_test_data():
    """Rimuove i file temporanei alla fine"""
    import shutil
    if os.path.exists(TEST_DATA_DIR):
        shutil.rmtree(TEST_DATA_DIR)
        print("🧹 Pulizia file temporanei completata.")

# -------------------------------------------------------------------
# ⚙️ FUNZIONI DI INTERAZIONE P2P
# -------------------------------------------------------------------
def hash_file(path):
    sha = hashlib.sha1()
    with open(path, "rb") as f:
        while chunk := f.read(1024 * 1024):
            sha.update(chunk)
    return sha.hexdigest()

def upload_file(peer_url, filepath, metadata=None):
    filename = os.path.basename(filepath)
    print(f"📤 [UPLOAD] Carico '{filename}' su {peer_url}...")
    
    # Percorso interno al container (simulato mappando la cartella o passando path relativo)
    # Nota: Per far funzionare questo test con Docker, il file deve essere accessibile al container.
    # Se usi volumi mappati, assicurati che TEST_DATA_DIR sia nel volume.
    # PER SEMPLICITÀ IN QUESTO TEST: Assumiamo che upload legga il path locale o invii il contenuto.
    # Ma il tuo codice attuale si aspetta un path locale al peer.
    # Workaround per test locale: Copiamo il file nella cartella data del peer prima dell'upload
    
    # Simuliamo che il file sia già nella cartella del peer (come nel tuo codice originale)
    # Qui usiamo il path assoluto interno al container come facevi tu: /app/data/...
    internal_path = f"/app/data/{filename}"
    
    # Trucco per il test: copiamo fisicamente il file nella cartella montata del peer
    # Assumiamo che la cartella 'data_peer1' corrisponda a peer1 (localhost:5001)
    peer_idx = PEERS.index(peer_url) + 1
    local_mount_dir = f"data_peer{peer_idx}"
    if not os.path.exists(local_mount_dir):
        os.makedirs(local_mount_dir)
    
    # Copia file generato nella cartella del volume del peer
    import shutil
    shutil.copy(filepath, os.path.join(local_mount_dir, filename))
    
    start = time.time()
    payload = {"filename": internal_path}
    if metadata:
        payload["metadata"] = metadata
    
    try:
        r = requests.post(f"http://{peer_url}/store_file", json=payload)
        elapsed = time.time() - start
        if r.status_code == 200:
            print(f"   ✅ OK ({elapsed:.2f}s)")
            return r.json()
        else:
            print(f"   ❌ Errore: {r.status_code} - {r.text}")
    except Exception as e:
        print(f"   ❌ Exception: {e}")
    return None

def search_metadata(peer_url, query, expected_count=None):
    print(f"🔎 [SEARCH] Query su {peer_url}: {query}")
    start = time.time()
    try:
        r = requests.get(f"http://{peer_url}/search", params=query, timeout=5)
        elapsed = time.time() - start
        
        if r.status_code == 200:
            results = r.json().get("results", [])
            count = len(results)
            print(f"   ✅ Trovati {count} risultati in {elapsed:.2f}s")
            for res in results:
                print(f"      - {res['filename']} (Host: {res.get('host', '?')})")
            
            if expected_count is not None:
                if count == expected_count:
                    print(f"   🎯 TARGET RAGGIUNTO: Trovati esattamente {expected_count} file.")
                else:
                    print(f"   ⚠️ WARNING: Attesi {expected_count}, trovati {count}.")
            return results
        else:
            print(f"   ❌ Errore HTTP {r.status_code}")
    except Exception as e:
        print(f"   ❌ Exception search: {e}")
    return []

# -------------------------------------------------------------------
# 🧪 SCENARI DI TEST
# -------------------------------------------------------------------

def test_gsi_salting_stress():
    print("\n" + "="*60)
    print("🧪 TEST 1: GSI SALTING & AGGREGATION (Hotspot Test)")
    print("="*60)
    print("Obiettivo: Caricare 5 file con LO STESSO attore e verificare che la search li trovi tutti.")
    print("Se il Salting funziona, le scritture sono distribuite ma la lettura le aggrega tutte.\n")

    popular_actor = "Brad Pitt"
    files_to_upload = 5
    uploaded_files = []

    # 1. Genera e Carica 5 file diversi
    for i in range(files_to_upload):
        fname = f"movie_brad_{i}.mp4"
        fpath = generate_dummy_file(fname, size_mb=0.1) # Piccoli per velocità
        
        # Carichiamo su peer diversi a rotazione per realismo
        target_peer = PEERS[i % len(PEERS)]
        
        meta = {"actor": popular_actor, "id": str(i)}
        upload_file(target_peer, fpath, meta)
        uploaded_files.append(fname)
        time.sleep(0.5) # Breve pausa per ordine log

    print("\n⏳ Attesa propagazione indici (consistency delay)...")
    time.sleep(2)

    # 2. Search
    print("\n🔍 Eseguo ricerca per l'attore 'hotspot'...")
    # Cerchiamo da un peer che NON ha caricato nulla (es. l'ultimo)
    results = search_metadata(PEERS[-1], {"actor": popular_actor}, expected_count=files_to_upload)

    if results and len(results) == files_to_upload:
        print("\n✅ TEST SALTING PASSATO: Tutti i file distribuiti sono stati aggregati.")
    else:
        print("\n❌ TEST SALTING FALLITO: Alcuni file mancano all'appello.")

def test_multi_attribute_intersection():
    print("\n" + "="*60)
    print("🧪 TEST 2: INTERSEZIONE METADATI (AND Logic)")
    print("="*60)
    print("Obiettivo: Verificare che la ricerca filtri correttamente su più campi.")
    
    # Genera file
    f1 = generate_dummy_file("matrix.avi", 0.1)
    f2 = generate_dummy_file("john_wick.avi", 0.1)
    f3 = generate_dummy_file("notebook.avi", 0.1)

    # Carica con metadati specifici
    # File 1: Keanu + Sci-Fi
    upload_file(PEERS[0], f1, {"actor": "Keanu Reeves", "genre": "Sci-Fi"})
    # File 2: Keanu + Action
    upload_file(PEERS[1], f2, {"actor": "Keanu Reeves", "genre": "Action"})
    # File 3: Ryan + Romance
    upload_file(PEERS[2], f3, {"actor": "Ryan Gosling", "genre": "Romance"})
    
    time.sleep(2)

    print("\n--- Case A: Ricerca attributo singolo (Keanu) ---")
    # Ci aspettiamo 2 file (Matrix e John Wick)
    search_metadata(PEERS[3], {"actor": "Keanu Reeves"}, expected_count=2)

    print("\n--- Case B: Intersezione Corretta (Keanu AND Sci-Fi) ---")
    # Ci aspettiamo solo Matrix
    res = search_metadata(PEERS[3], {"actor": "Keanu Reeves", "genre": "Sci-Fi"}, expected_count=1)
    if res and os.path.basename(res[0]['filename']) == "matrix.avi":
        print("   ✅ Match esatto confermato (Matrix).")
    else:
        print(f"   ⚠️ Match errato: {res}")

    print("\n--- Case C: Intersezione Vuota (Keanu AND Romance) ---")
    # Ci aspettiamo 0 risultati
    search_metadata(PEERS[3], {"actor": "Keanu Reeves", "genre": "Romance"}, expected_count=0)


def main():
    try:
        test_gsi_salting_stress()
        test_multi_attribute_intersection()
    finally:
        print("\n")
        cleanup_test_data()

if __name__ == "__main__":
    main()