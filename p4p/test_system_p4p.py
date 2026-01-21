#!/usr/bin/env python3
import os
import time
import json
import hashlib
import requests
import statistics
from datetime import datetime

# -------------------------------------------------------------------
# CONFIGURAZIONE
# -------------------------------------------------------------------
PEERS = [
    {"id": "peer1", "url": "localhost:5001", "isp": "ISP_A", "region": "north"},
    {"id": "peer2", "url": "localhost:5002", "isp": "ISP_A", "region": "south"},
    {"id": "peer3", "url": "localhost:5003", "isp": "ISP_B", "region": "north"},
]

FILE_TO_UPLOAD = "test_file_large.txt"
ITRACKER_URL = "http://localhost:6000"
RESULTS_LOG = "metrics_results.json"

# -------------------------------------------------------------------
# FUNZIONI UTILI
# -------------------------------------------------------------------
def hash_file(path):
    """Calcola SHA1 del file locale"""
    sha = hashlib.sha1()
    with open(path, "rb") as f:
        while chunk := f.read(1024 * 1024):
            sha.update(chunk)
    return sha.hexdigest()


def call_peer(peer_url, endpoint, method="GET", **kwargs):
    """Effettua una chiamata HTTP generica al peer"""
    url = f"http://{peer_url}/{endpoint}"
    start = time.time()
    try:
        r = requests.request(method, url, timeout=5, **kwargs)
        latency = time.time() - start
        return r, latency
    except Exception as e:
        print(f"Errore chiamando {url}: {e}")
        return None, None


def upload_file(peer_url, filename):
    print(f"\n Avvio upload su {peer_url} del file '{filename}'")
    start = time.time()
    internal_path = f"/app/data/{os.path.basename(filename)}"
    r = requests.post(f"http://{peer_url}/store_file", json={"filename": internal_path})
    elapsed = time.time() - start

    if r.status_code == 200:
        print(f"Upload completato in {elapsed:.2f}s")
        return r.json(), elapsed
    else:
        print(f"Errore upload: {r.status_code} - {r.text}")
        return None, elapsed


def download_file(peer_url, filename):
    """Simula il download da un peer"""
    print(f"\n Download -> {peer_url}")
    data = {"filename": os.path.basename(filename)}
    r, latency = call_peer(peer_url, "fetch_file", "POST", json=data)
    return (r.json() if r else None), latency


def verify_integrity(original, rebuilt):
    """Verifica che il file ricostruito sia identico all’originale"""
    try:
        h1 = hash_file(original)
        h2 = hash_file(rebuilt)
    except FileNotFoundError:
        return False
    return h1 == h2


def collect_metrics():
    """Scarica metriche aggiornate dall’iTracker (che ora conosce ALTO cost maps)"""
    try:
        r = requests.get(f"{ITRACKER_URL}/metrics")
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"Impossibile ottenere metriche da iTracker: {e}")
    return {}

# -------------------------------------------------------------------
# METRICHE DI RETE (ALTO / P4P)
# -------------------------------------------------------------------
def register_peers(itracker_url, peers):
    """Registra i peer presso l'iTracker per popolare network_map e P4P map."""
    results = []
    for p in peers:
        payload = {"peer_id": p["url"], "isp": p["isp"], "region": p["region"]}
        try:
            r = requests.post(f"{itracker_url}/register_peer", json=payload, timeout=3)
            results.append({"peer": p["url"], "status": r.status_code, "body": r.json() if r.ok else r.text})
        except Exception as e:
            results.append({"peer": p["url"], "error": str(e)})
    return results


def get_network_metrics(itracker_url, isp="ISP_A", region="north"):
    """Recupera metriche ALTO/P4P reali dagli endpoint esposti da iTracker."""
    metrics = {}
    try:
        # 1) P4P legacy peers structure
        r = requests.get(f"{itracker_url}/get_peers", params={"isp": isp, "region": region}, timeout=3)
        if r.status_code == 200:
            data = r.json()
            metrics["preferred_peers_count"] = len(data.get("preferred_peers", []))
            metrics["same_isp_count"] = len(data.get("same_isp", []))
            metrics["total_known_peers"] = len(data.get("all_known", []))
        else:
            metrics["peers_error"] = r.text

        # 2) ALTO network map
        r_net = requests.get(f"{itracker_url}/alto/network_map", timeout=3)
        if r_net.status_code == 200:
            nm = r_net.json().get("network_map", {})
            metrics["network_map_peer_count"] = len(nm)
        else:
            metrics["network_map_error"] = r_net.text

        # 3) ALTO cost map (nota: struttura nidificata location->location->cost)
        r_cost = requests.get(f"{itracker_url}/alto/cost_map", timeout=3)
        if r_cost.status_code == 200:
            cm_body = r_cost.json()
            cmap = cm_body.get("cost_map", {})
            # Flatten dei costi in una lista
            costs = [v for inner in cmap.values() for v in inner.values()]
            if costs:
                metrics["avg_alto_cost"] = sum(costs) / len(costs)
                metrics["max_alto_cost"] = max(costs)
                metrics["min_alto_cost"] = min(costs)
                metrics["alto_units"] = cm_body.get("units", "unknown")
                metrics["alto_updated_at"] = cm_body.get("updated_at")
            else:
                metrics["alto_costs_empty"] = True
        else:
            metrics["alto_cost_map_error"] = r_cost.text
    except Exception as e:
        metrics["error"] = str(e)

    return metrics

# -------------------------------------------------------------------
# TEST PRINCIPALE
# -------------------------------------------------------------------
def main():
    print("Test completo rete P4P + ALTO")

    results = {
        "timestamp": datetime.now().isoformat(),
        "upload_latency": None,
        "download_latency": None,
        "integrity_ok": False,
        "network_metrics": {},
    }

    # Registrazione peer presso iTracker (per popolare mappe)
    reg = register_peers(ITRACKER_URL, PEERS)
    if reg:
        print("Registrazione peer su iTracker:")
        for entry in reg:
            print(" -", entry)

    # Upload file da peer1
    manifest, up_time = upload_file(PEERS[0]["url"], FILE_TO_UPLOAD)
    if not manifest:
        print("Upload fallito")
        return
    print(f"Upload completato in {up_time:.2f}s")

    time.sleep(3)  # attesa propagazione

    # Download file da peer2 (ISP uguale, regione diversa)
    rebuilt_info, down_time = download_file(PEERS[1]["url"], FILE_TO_UPLOAD)
    if not rebuilt_info:
        print("Download fallito")
        return
    print(f"Download completato in {down_time:.2f}s")

    # Verifica integrità
    rebuilt_path = os.path.join("data_peer2", f"rebuilt_{os.path.basename(FILE_TO_UPLOAD)}")
    ok = verify_integrity(FILE_TO_UPLOAD, rebuilt_path)
    print(f"Integrità {'OK' if ok else 'FALLITA'}")

    # Raccolta metriche (iTracker /metrics + ALTO reali)
    itracker_metrics = collect_metrics()
    alto_metrics = get_network_metrics(ITRACKER_URL, isp=PEERS[0]["isp"], region=PEERS[1]["region"])

    if itracker_metrics:
        print("\n Metriche iTracker /metrics:")
        print(json.dumps(itracker_metrics, indent=2))
    if alto_metrics:
        print("\n Metriche ALTO/P4P:")
        print(json.dumps(alto_metrics, indent=2))

    # Salvataggio risultati
    results["upload_latency"] = up_time
    results["download_latency"] = down_time
    results["integrity_ok"] = ok
    results["network_metrics"] = {
        "itracker": itracker_metrics,
        "alto": alto_metrics,
    }

    with open(RESULTS_LOG, "a") as f:
        json.dump(results, f)
        f.write("\n")

    print("\n Test completato — risultati salvati in", RESULTS_LOG)


if __name__ == "__main__":
    main()
