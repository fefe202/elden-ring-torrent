#!/usr/bin/env python3
import os
import time
import json
import random
import statistics
import requests
import math
import sys

# ==============================================================================
# 🌩️ CONFIGURAZIONE CLOUD MASSIVE (HEAVY WORKLOAD)
# ==============================================================================

# Generazione Automatica dei 20 Peer Ibridi (4 Worker x 5 Container)
PEERS = []
for w in range(1, 5): # worker-1 to worker-4
    for p in range(1, 6): # port 5001 to 5005 (mappate su 5000 interne)
        # Nota: Usiamo la porta esterna mappata dallo script startup-hybrid.sh
        PEERS.append(f"worker-{w}:{5000+p}")

OUTPUT_FILE = "benchmark_cloud_results.json"

# --- Parametri di Carico (SCALING REALISTICO) ---
NUM_FILES = 500         # Carico sostanzioso per riempire la DHT/Indici
SEARCH_QUERIES = 1000   # Abbastanza alto per avere code (P99) statisticamente valide
WARMUP_QUERIES = 50     # Query a vuoto per scaldare TCP e Cache
WAIT_AFTER_UPLOAD = 45  # Tempo per la convergenza del Gossip e indicizzazione

# --- Timeout ---
CONN_TIMEOUT = 5        # Timeout connessione
READ_TIMEOUT = 10       # Timeout lettura (alto per evitare falsi positivi sotto stress)

# --- Distribuzione Dati ---
POPULAR_ACTORS = ["Brad Pitt", "Scarlett Johansson", "Leonardo DiCaprio", "Tom Hanks", "Meryl Streep"]
RARE_ACTORS = [f"IndieActor_{i}" for i in range(500)] # Molti più attori rari
GENRES = ["Action", "Sci-Fi", "Drama", "Comedy", "Horror", "Documentary", "Thriller", "Romance"]

# ==============================================================================
# 📊 UTILS STATISTICHE
# ==============================================================================

def calculate_gini(data):
    """Calcola Gini Coefficient per misurare il bilanciamento del carico"""
    if not data: return 0
    sorted_data = sorted(data)
    height, area = 0, 0
    for value in sorted_data:
        height += value
        area += height - value / 2.
    fair_area = height * len(data) / 2.
    if fair_area == 0:
        return 0
    return (fair_area - area) / fair_area

def get_percentile(data, percentile):
    if not data: return 0
    data.sort()
    k = (len(data) - 1) * percentile
    f = math.floor(k)
    c = math.ceil(k)
    if f == c: return data[int(k)]
    d0 = data[int(f)]
    d1 = data[int(c)]
    return d0 + (d1 - d0) * (k - f)

def get_weighted_actor():
    # Distribuzione Zipfian approssimata (80/20)
    if random.random() < 0.8:
        return random.choice(POPULAR_ACTORS)
    else:
        return random.choice(RARE_ACTORS)

# ==============================================================================
# 🩺 HEALTH CHECK
# ==============================================================================

def wait_for_cluster():
    print(f"\n🩺 [INIT] Controllo salute del cluster ({len(PEERS)} nodi)...")
    alive_count = 0
    with requests.Session() as s:
        for p in PEERS:
            try:
                # Chiediamo stats veloci per vedere se è vivo
                r = s.get(f"http://{p}/known_peers", timeout=2)
                if r.status_code == 200:
                    alive_count += 1
            except:
                pass
    
    print(f"✅ Nodi attivi: {alive_count}/{len(PEERS)}")
    if alive_count < len(PEERS) * 0.8: # Se meno dell'80% è vivo, abortiamo
        print("❌ TROPPI NODI MORTI. Controlla i worker!")
        sys.exit(1)
    else:
        print("🚀 Cluster pronto. Inizio Benchmark Massivo.")

# ==============================================================================
# 🔥 FASI DEL TEST
# ==============================================================================

def run_upload_phase(session):
    print(f"\n📤 [PHASE 1] Upload Massivo di {NUM_FILES} file...")
    latencies = []
    errors = 0
    
    for i in range(NUM_FILES):
        filename = f"movie_{i}.bin"
        # Nota: In Cloud non copiamo fisicamente il file (shutil), simuliamo solo la richiesta API.
        # Il Peer riceverà il path e creerà un dummy se necessario, o indicizzerà solo i metadati.
        internal_path = f"/app/data/{filename}" 
        
        metadata = {
            "actor": get_weighted_actor(),
            "genre": random.choice(GENRES),
            "year": random.randint(1980, 2024),
            "size_mb": 1 # Simulato
        }
        
        peer_addr = random.choice(PEERS)
        
        start = time.time()
        try:
            r = session.post(f"http://{peer_addr}/store_file", json={
                "filename": internal_path,
                "metadata": metadata,
                "simulate_content": True # Flag opzionale se il tuo codice lo supporta
            }, timeout=CONN_TIMEOUT)
            
            if r.status_code == 200:
                latencies.append(time.time() - start)
            else:
                errors += 1
        except Exception as e:
            errors += 1

        # Progress bar semplice
        if i > 0 and i % 50 == 0:
            print(f"   ... {i}/{NUM_FILES} (Errors: {errors})")
            
    print(f"✅ Upload completato. Errori totali: {errors}")
    return latencies

def run_search_phase(session):
    print(f"\n🔥 [PHASE 2-A] Warmup ({WARMUP_QUERIES} query non misurate)...")
    # Warmup per aprire connessioni TCP e riempire cache ARP/DNS
    for _ in range(WARMUP_QUERIES):
        try:
            q = {"actor": random.choice(POPULAR_ACTORS)}
            target = random.choice(PEERS)
            session.get(f"http://{target}/search", params=q, timeout=2)
        except: pass

    print(f"🔎 [PHASE 2-B] Stress Test ({SEARCH_QUERIES} query misurate)...")
    latencies = []
    timeouts = 0
    
    for i in range(SEARCH_QUERIES):
        q_type = random.random()
        query = {}
        
        # Mix di query complesse
        if q_type < 0.5:   query = {"actor": random.choice(POPULAR_ACTORS)} # Hotspot
        elif q_type < 0.8: query = {"actor": random.choice(RARE_ACTORS)}    # Scatter-Gather stress
        else:              query = {"genre": random.choice(GENRES), "year": random.randint(2000, 2020)} # Multi-attr
            
        target_peer = random.choice(PEERS)
        
        start = time.time()
        try:
            r = session.get(f"http://{target_peer}/search", params=query, timeout=READ_TIMEOUT)
            elapsed = time.time() - start
            
            if r.status_code == 200:
                latencies.append(elapsed)
            else:
                timeouts += 1 # HTTP Error
        except Exception:
            timeouts += 1 # Network Timeout

        if i > 0 and i % 100 == 0:
            print(f"   ... {i}/{SEARCH_QUERIES}")
            
    print(f"✅ Search completata. Timeout/Errori: {timeouts}")
    return latencies

def collect_metrics(session):
    print("\n📊 [PHASE 3] Raccolta Metriche Cluster...")
    storage_loads = []
    
    for p in PEERS:
        try:
            r = session.get(f"http://{p}/stats", timeout=2)
            if r.status_code == 200:
                data = r.json().get("storage", {})
                load = data.get("chunks_count", 0) + data.get("indexes_count", 0)
                storage_loads.append(load)
            else:
                storage_loads.append(0)
        except:
            storage_loads.append(0)
            
    gini = calculate_gini(storage_loads)
    variance = statistics.variance(storage_loads) if len(storage_loads) > 1 else 0
    
    return {
        "storage_loads": storage_loads,
        "gini": gini,
        "variance": variance
    }

# ==============================================================================
# MAIN
# ==============================================================================

def run_cloud_benchmark(mode_label):
    # Setup Sessione HTTP ottimizzata (Keep-Alive)
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=50)
    session.mount('http://', adapter)

    print("\n" + "█"*60)
    print(f"   CLOUD BENCHMARK SUITE - MODE: {mode_label}")
    print("█"*60)

    # 0. Check
    wait_for_cluster()

    # 1. Upload
    up_lat = run_upload_phase(session)
    
    print(f"💤 Sleeping {WAIT_AFTER_UPLOAD}s (Gossip Convergence)...")
    time.sleep(WAIT_AFTER_UPLOAD)
    
    # 2. Search
    search_lat = run_search_phase(session)
    
    # 3. Stats
    metrics = collect_metrics(session)
    
    # Report
    avg_search = statistics.mean(search_lat) if search_lat else 0
    p95 = get_percentile(search_lat, 0.95)
    p99 = get_percentile(search_lat, 0.99)
    
    # Calcolo Throughput (Client-side observed)
    total_time = sum(search_lat) if search_lat else 1
    throughput = len(search_lat) / total_time if total_time > 0 else 0

    print(f"\n🏆 RISULTATI FINALI [{mode_label}]")
    print(f"   files={NUM_FILES}, queries={SEARCH_QUERIES}, nodes={len(PEERS)}")
    print("-" * 40)
    print(f"   ⏱️  Avg Latency:  {avg_search:.4f} s")
    print(f"   🐢 P99 Latency:  {p99:.4f} s (Tail)")
    print(f"   🚀 Throughput:   {throughput:.2f} req/s")
    print(f"   ⚖️  Gini Coeff:   {metrics['gini']:.4f} (Load Balance)")
    print(f"   DATA: {metrics['storage_loads']}")

    # Save to JSON
    results = {
        "mode": mode_label,
        "config": {"files": NUM_FILES, "queries": SEARCH_QUERIES, "nodes": len(PEERS)},
        "upload_latencies": up_lat,
        "search_latencies": search_lat,
        "metrics": metrics
    }
    
    with open(OUTPUT_FILE, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\n💾 Dati salvati in {OUTPUT_FILE}")

if __name__ == "__main__":
    # In Cloud, lanciamo una modalità alla volta basandoci su come abbiamo avviato le VM
    # Esempio: python3 benchmark_cloud_heavy.py NAIVE
    if len(sys.argv) > 1:
        MODE = sys.argv[1]
    else:
        MODE = "UNKNOWN_MODE" # Fallback
    
    run_cloud_benchmark(MODE)