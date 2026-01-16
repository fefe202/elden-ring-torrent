#!/usr/bin/env python3
import os
import requests
import hashlib
import time
import subprocess

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

FILE_TO_UPLOAD = "test_file_large.txt"

# -------------------------------------------------------------------
# ⚙️ FUNZIONI DI UTILITÀ
# -------------------------------------------------------------------
def hash_file(path):
    sha = hashlib.sha1()
    with open(path, "rb") as f:
        while chunk := f.read(1024 * 1024):
            sha.update(chunk)
    return sha.hexdigest()


def upload_file(peer_url, filename, metadata=None):
    print(f"\n📤 [UPLOAD] Avvio upload su {peer_url} del file '{filename}'")
    start = time.time()
    internal_path = f"/app/data/{os.path.basename(filename)}"
    payload = {"filename": internal_path}
    if metadata:
        payload["metadata"] = metadata
    r = requests.post(f"http://{peer_url}/store_file", json=payload)
    elapsed = time.time() - start

    if r.status_code == 200:
        print(f"✅ Upload completato in {elapsed:.2f}s")
        return r.json(), elapsed
    else:
        print(f"❌ Errore upload: {r.status_code} - {r.text}")
        return None, elapsed


def download_file(peer_url, filename):
    print(f"\n📥 [DOWNLOAD] Avvio download su {peer_url} del file '{filename}'")
    start = time.time()
    r = requests.post(f"http://{peer_url}/fetch_file", json={"filename": os.path.basename(filename)})
    elapsed = time.time() - start

    if r.status_code == 200:
        rebuilt_filename = os.path.join(f"data_peer2", f"rebuilt_{os.path.basename(filename)}")
        print(f"✅ Download completato in {elapsed:.2f}s -> {rebuilt_filename}")
        return rebuilt_filename, elapsed
    else:
        print(f"❌ Errore download: {r.status_code} - {r.text}")
        return None, elapsed


def verify_integrity(local_original, local_rebuilt):
    print("\n🔍 Verifica integrità...")
    try:
        h1 = hash_file(local_original)
        h2 = hash_file(local_rebuilt)
    except FileNotFoundError:
        print(f"❌ File non trovato per la verifica: {local_rebuilt}")
        return False

    if h1 == h2:
        print(f"🎉 Verifica OK — file identici (SHA1: {h1})")
        return True
    else:
        print(f"⚠️ File diversi!\n  Originale: {h1}\n  Ricostruito: {h2}")
        return False


def search_file(peer_url, query_params):
    print(f"\n🔎 [SEARCH] su {peer_url} con query {query_params}")
    try:
        r = requests.get(f"http://{peer_url}/search", params=query_params, timeout=5)
        if r.status_code == 200:
            results = r.json().get("results", [])
            if results:
                print(f"✅ Trovati {len(results)} risultati:")
                for res in results:
                    print(f"  🎬 {res['filename']} — {res['metadata']} (host: {res['host']})")
            else:
                print("⚠️ Nessun file corrispondente trovato.")
        else:
            print(f"❌ Errore ricerca: HTTP {r.status_code}")
    except Exception as e:
        print(f"❌ Errore richiesta search: {e}")


def get_known_peers(peer_url):
    """Richiede la lista dei known peers da un nodo"""
    try:
        r = requests.get(f"http://{peer_url}/known_peers")
        if r.status_code == 200:
            peers = r.json().get("known_peers", [])
            print(f"🌐 {peer_url} conosce: {peers}")
            return peers
        else:
            print(f"⚠️ Errore nel recupero known peers da {peer_url}")
            return []
    except Exception as e:
        print(f"❌ Errore contattando {peer_url}: {e}")
        return []


def stop_peer(container_name):
    """Ferma un container Docker (simula disconnessione peer)"""
    print(f"\n🧱 Arresto di {container_name}...")
    subprocess.run(["docker", "stop", container_name])
    time.sleep(3)


def start_peer(container_name):
    """Riavvia un container Docker (simula ritorno del peer)"""
    print(f"\n♻️ Riavvio di {container_name}...")
    subprocess.run(["docker", "start", container_name])
    time.sleep(5)

def wait_for_peer(peer_url, target_peer, timeout=60):
    """Aspetta finché target_peer non ricompare nella lista known_peers di peer_url."""
    print(f"⏳ Attendo che {target_peer} sia di nuovo visibile da {peer_url}...")
    start = time.time()
    while time.time() - start < timeout:
        peers = get_known_peers(peer_url)
        if target_peer in peers:
            print(f"✅ {target_peer} riapparso nella rete.")
            return True
        time.sleep(5)
    print(f"⚠️ {target_peer} non è riapparso entro {timeout}s.")
    return False

def check_peer_removed(peer_url, target_peer):
    peers = get_known_peers(peer_url)
    if target_peer not in peers:
        print(f"✅ {target_peer} correttamente rimosso da {peer_url}")
        return True
    else:
        print(f"⚠️ {target_peer} ancora presente in {peer_url}")
        return False

# -------------------------------------------------------------------
# 🚀 TEST COMPLETO
# -------------------------------------------------------------------
def main():
    print("🚀 Test completo rete Peer-to-Peer (upload, download, resilienza)")

    # 1️⃣ Upload iniziale
    metadata = {
        "titolo": "La Trama dei Dati",
        "regista": "Alice Dataweaver",
        "anno": "2025",
        "genere": "Sci-Fi"
    }
    manifest, upload_time = upload_file(PEERS[0], FILE_TO_UPLOAD, metadata)
    if not manifest:
        print("⛔ Upload fallito, test interrotto.")
        return
    time.sleep(3)

    # 2️⃣ Download e integrità
    rebuilt_file, download_time = download_file(PEERS[1], FILE_TO_UPLOAD)
    verify_integrity(FILE_TO_UPLOAD, rebuilt_file)

    # 3️⃣ Ricerca distribuita
    search_file(PEERS[2], {"titolo": "La Trama dei Dati"})

    # 4️⃣ Controllo stato rete
    print("\n🌍 Stato iniziale della rete:")
    for p in PEERS[:4]:
        get_known_peers(p)

    # 5️⃣ Simula disconnessione di un peer
    stop_peer("peer4")
    time.sleep(20)

    print("\n🧩 Stato rete dopo disconnessione di peer4:")
    for p in PEERS[:4]:
        check_peer_removed(p, "peer4")

    # 6️⃣ Simula riavvio di peer4
    start_peer("peer4")
    time.sleep(20)
    wait_for_peer(PEERS[0], "peer4:5000")

    print("\n🔄 Stato rete dopo il riavvio di peer4:")
    for p in PEERS[:4]:
        get_known_peers(p)

    # 7️⃣ Test di disponibilità dati post-rientro

    search_file(PEERS[4], {"regista": "Alice Dataweaver"})
    rebuilt_file, _ = download_file(PEERS[5], FILE_TO_UPLOAD)
    verify_integrity(FILE_TO_UPLOAD, rebuilt_file)

    print("\n📊 RISULTATI FINALI")
    print(f"Tempo upload:   {upload_time:.2f} s")
    print(f"Tempo download: {download_time:.2f} s")
    print("✅ Tutti i test eseguiti correttamente.\n")

# -------------------------------------------------------------------
if __name__ == "__main__":
    main()
