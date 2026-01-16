#!/usr/bin/env python3
import os
import time
import json
import random
import shutil
import statistics
import subprocess
import requests
import math

# ==============================================================================
# 🔧 CONFIGURAZIONE MASSICCIA
# ==============================================================================
# Definizione dei peer (assicurati che docker-compose esponga queste porte)
NUM_PEERS = 10
PEERS = [f"localhost:{5001 + i}" for i in range(NUM_PEERS)]

OUTPUT_FILE = "benchmark_results.json"
TEMP_GEN_DIR = "bench_temp_gen"

# --- Parametri di Carico (SCALING X10) ---
NUM_FILES = 20          # RIDOTTO PER TEST LOCALE VELOCE (Originale: 100)
FILE_SIZE_MB = 1       # Dimensione piccola per non saturare I/O disco locale
SEARCH_QUERIES = 50     # RIDOTTO PER TEST LOCALE VELOCE (Originale: 200)
WAIT_AFTER_UPLOAD = 10  # Secondi di attesa per la propagazione/indicizzazione

# --- Distribuzione Dati (Simulazione Realistica) ---
# Zipf Law: Pochi attori compaiono in moltissimi film
POPULAR_ACTORS = ["Brad Pitt", "Scarlett Johansson", "Leonardo DiCaprio"]
RARE_ACTORS = [f"IndieActor_{i}" for i in range(100)]
GENRES = ["Action", "Sci-Fi", "Drama", "Comedy", "Horror", "Documentary"]

# ==============================================================================
# 🛠 UTILS & SETUP
# ==============================================================================

def calculate_gini(data):
    """Calcola il coefficiente di Gini (0=equità perfetta, 1=disuguaglianza massima)"""
    if not data: return 0
    sorted_data = sorted(data)
    height, area = 0, 0
    for value in sorted_data:
        height += value
        area += height - value / 2.
    fair_area = height * len(data) / 2.
    return (fair_area - area) / fair_area

def get_percentile(data, percentile):
    """Calcola percentile (es. 0.95 o 0.99) per la Tail Latency"""
    if not data: return 0
    data.sort()
    k = (len(data) - 1) * percentile
    f = math.floor(k)
    c = math.ceil(k)
    if f == c: return data[int(k)]
    d0 = data[int(f)]
    d1 = data[int(c)]
    return d0 + (d1 - d0) * (k - f)

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def clean_local_data():
    """Pulisce i file generati e le cartelle dei volumi locali"""
    print("🧹 Pulizia dati locali pre-test...")
    if os.path.exists(TEMP_GEN_DIR):
        shutil.rmtree(TEMP_GEN_DIR)
    
    # Pulisce le cartelle data_peerX create dai volumi docker
    for i in range(1, NUM_PEERS + 2): # + safe margin
        d = f"data_peer{i}"
        if os.path.exists(d):
            try:
                shutil.rmtree(d)
                os.makedirs(d) # Ricrea vuota
            except Exception as e:
                print(f"Warning cleaning {d}: {e}")

def generate_dummy_file(filename, size_mb):
    """Genera un file binario random"""
    ensure_dir(TEMP_GEN_DIR)
    path = os.path.join(TEMP_GEN_DIR, filename)
    with open(path, "wb") as f:
        f.write(os.urandom(int(size_mb * 1024 * 1024)))
    return path

def get_weighted_actor():
    """Ritorna un attore basato su distribuzione 80/20 (Hotspot simulation)"""
    if random.random() < 0.8: # 80% probabilità di scegliere tra i 3 popolari
        return random.choice(POPULAR_ACTORS)
    else:
        return random.choice(RARE_ACTORS)

# ==============================================================================
# 🐳 GESTIONE DOCKER
# ==============================================================================

def restart_env(mode):
    print(f"\n🔄 [ENV] Riavvio ambiente Docker in modalità: {mode}")
    
    # 1. Shutdown brutale e pulizia volumi
    subprocess.run("docker-compose down -v --remove-orphans", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    # 2. Pulizia cartelle locali host (per evitare residui di test precedenti)
    clean_local_data()
    
    # 3. Setup Variabili Ambiente
    env = os.environ.copy()
    env["PEER_MODE"] = mode
    
    # 4. Avvio con LOGGING SU FILE
    print("🚀 [ENV] Docker Compose Up (Logs -> docker_runtime.log)...")
    
    # Apriamo il file in modalità scrittura (sovrascrive ogni volta)
    with open("docker_runtime.log", "w") as logfile:
        subprocess.Popen("docker-compose up --force-recreate", shell=True, env=env, stdout=logfile, stderr=logfile)
    
    print("⏳ [ENV] Attesa stabilizzazione cluster (30s)...")
    time.sleep(30) 
    
    # Check liveness
    try:
        r = requests.get(f"http://{PEERS[0]}/known_peers", timeout=2)
        print(f"✅ [ENV] Cluster attivo. Nodi noti: {len(r.json().get('known_peers', []))}")
    except Exception as e:
        print(f"⚠️ [ENV] Warning: Cluster non risponde! Controlla docker_runtime.log per errori.")

# ==============================================================================
# 📊 METRICHE E TEST
# ==============================================================================

def run_upload_phase():
    print(f"\n📤 [PHASE 1] Upload di {NUM_FILES} file...")
    latencies = []
    
    for i in range(NUM_FILES):
        # Setup Dati
        filename = f"movie_{i}.bin"
        local_src = generate_dummy_file(filename, FILE_SIZE_MB)
        
        actor = get_weighted_actor()
        genre = random.choice(GENRES)
        metadata = {"actor": actor, "genre": genre, "year": random.randint(1990, 2025)}
        
        # Scelta target (Round Robin)
        peer_addr = PEERS[i % len(PEERS)]
        peer_idx = PEERS.index(peer_addr) + 1 # data_peer1, data_peer2...
        
        # Simulazione: Copia file nella cartella del volume del peer
        # Il peer si aspetta di trovare il file in /app/data/{filename}
        # Noi lo mettiamo in ./data_peerX/{filename} che è mappato
        target_dir = f"data_peer{peer_idx}"
        ensure_dir(target_dir)
        try:
            shutil.copy(local_src, os.path.join(target_dir, filename))
        except Exception as e:
            print(f"⚠️ Copy error for {filename}: {e}")
        
        internal_path = f"/app/data/{filename}"
        
        # Esecuzione Request
        start = time.time()
        try:
            r = requests.post(f"http://{peer_addr}/store_file", json={
                "filename": internal_path,
                "metadata": metadata
            }, timeout=10)
            
            if r.status_code == 200:
                latencies.append(time.time() - start)
            else:
                print(f"❌ Err Upload {filename}: {r.status_code}")
        except Exception as e:
            print(f"❌ Exception Upload {peer_addr}: {e}")

        if i % 20 == 0: print(f"   ... {i}/{NUM_FILES} caricati")
            
    return latencies

def run_search_phase():
    print(f"\n🔎 [PHASE 2] Esecuzione di {SEARCH_QUERIES} query miste...")
    latencies = []
    
    # Tipi di query per stressare diversi aspetti
    # 1. Hotspot (Brad Pitt) -> Stressa Semantic Single Node & GSI Salting Aggregation
    # 2. Rare -> Stressa Scatter-Gather (deve cercare ovunque)
    # 3. Intersection -> Stressa logica di filtraggio
    
    for i in range(SEARCH_QUERIES):
        q_type = random.random()
        query = {}
        
        if q_type < 0.5: # 50% Hotspot Query
            query = {"actor": random.choice(POPULAR_ACTORS)}
        elif q_type < 0.8: # 30% Rare Query
            query = {"actor": random.choice(RARE_ACTORS)}
        else: # 20% Multi-Attribute
            query = {"actor": random.choice(POPULAR_ACTORS), "genre": random.choice(GENRES)}
            
        target_peer = random.choice(PEERS)
        
        start = time.time()
        try:
            r = requests.get(f"http://{target_peer}/search", params=query, timeout=5)
            elapsed = time.time() - start
            
            if r.status_code == 200:
                latencies.append(elapsed)
                # results = r.json().get("results", [])
                # print(f"Found {len(results)}") # Debug verbose off
            
        except Exception:
            pass # Timeout contati come fallimenti o ignorati nel calcolo latenza media

        if i % 50 == 0: print(f"   ... {i}/{SEARCH_QUERIES} query eseguite")
            
    return latencies

def collect_system_metrics():
    print("\n📊 [PHASE 3] Raccolta statistiche nodi...")
    storage_loads = []
    total_files_network = 0
    
    for p in PEERS:
        try:
            r = requests.get(f"http://{p}/stats", timeout=2)
            if r.status_code == 200:
                data = r.json().get("storage", {})
                # Carico = Chunk + Indici (Manifest contano meno, ma li includiamo per completezza)
                load = data.get("chunks_count", 0) + data.get("indexes_count", 0)
                storage_loads.append(load)
                total_files_network += data.get("total_files", 0)
            else:
                storage_loads.append(0)
        except:
            storage_loads.append(0)
            
    # Calcolo Metriche Statistiche
    if storage_loads:
        avg_load = statistics.mean(storage_loads)
        # Varianza alta = Sbilanciamento (Hotspot)
        # Varianza bassa = Buon Load Balancing
        variance = statistics.variance(storage_loads) if len(storage_loads) > 1 else 0
        gini = calculate_gini(storage_loads)
    else:
        avg_load, variance, gini = 0, 0, 0
        
    print(f"   Distribuzione Carico Nodi: {storage_loads}")
    print(f"   Varianza Carico: {variance:.2f}")
    
    return {
        "storage_loads": storage_loads,
        "load_variance": variance,
        "gini_coefficient": gini,
        "total_files": total_files_network
    }

# ==============================================================================
# MAIN ORCHESTRATOR
# ==============================================================================

def run_full_benchmark(modes_to_test):
    final_report = {}
    
    for mode in modes_to_test:
        print("\n" + "="*60)
        print(f"▶️  START BENCHMARK: {mode}")
        print("="*60)
        
        restart_env(mode)
        
        # Struttura risultati
        mode_results = {
            "upload_latency": [],
            "search_latency": [],
            "metrics": {}
        }
        
        try:
            # 1. Write Stress
            mode_results["upload_latency"] = run_upload_phase()
            
            print(f"💤 Pausa {WAIT_AFTER_UPLOAD}s per indicizzazione background...")
            time.sleep(WAIT_AFTER_UPLOAD)
            
            # 2. Read Stress
            mode_results["search_latency"] = run_search_phase()
            
            # 3. System Stats
            mode_results["metrics"] = collect_system_metrics()
            
        except KeyboardInterrupt:
            print("\n🛑 Interruzione manuale...")
            break
        except Exception as e:
            print(f"\n❌ Errore critico nel benchmark {mode}: {e}")
            
        final_report[mode] = mode_results
        
        final_report[mode] = mode_results
        
        # Statistiche Avanzate
        up_lats = mode_results["upload_latency"]
        search_lats = mode_results["search_latency"]
        
        avg_up = statistics.mean(up_lats) if up_lats else 0
        avg_search = statistics.mean(search_lats) if search_lats else 0
        
        # P99 Latency (Il peggior 1% dei casi)
        p99_search = get_percentile(search_lats, 0.99) if search_lats else 0
        
        # Throughput (Richieste / Secondo)
        # Somma di tutti i tempi di latenza (approssimazione client-side)
        # Nota: per throughput reale servirebbe il tempo totale di wall-clock del loop
        total_time_search = sum(search_lats) if search_lats else 1
        throughput = len(search_lats) / total_time_search if total_time_search > 0 else 0

        metrics = mode_results['metrics']
        
        print(f"\n🏁 RIEPILOGO AVANZATO {mode}:")
        print(f"   [Time] Avg Search: {avg_search:.4f}s | P99 Search: {p99_search:.4f}s")
        print(f"   [Perf] Throughput: {throughput:.2f} req/s (Client-side approx)")
        print(f"   [Load] Imbalance:  {metrics.get('load_variance', 0):.2f} (Var)")
        print(f"   [Fair] Gini Coeff: {metrics.get('gini_coefficient', 0):.4f} (0=Perfect, 1=Bad)")

    # Chiusura finale
    print("\n🧹 Spegnimento ambiente...")
    subprocess.run("docker-compose down", shell=True)
    clean_local_data()

    # Salvataggio JSON
    with open(OUTPUT_FILE, "w") as f:
        json.dump(final_report, f, indent=2)
    
    print(f"\n✅ Benchmark completato! Dati salvati in: {os.path.abspath(OUTPUT_FILE)}")
    print("👉 Ora usa 'benchmark_plotter.py' per generare i grafici.")

if __name__ == "__main__":
    # Puoi commentare le modalità che non vuoi testare per risparmiare tempo
    MODES_TO_TEST = [
        "NAIVE",    # Flooding
        "METADATA", # GSI + Salting
        "SEMANTIC"  # Data Locality
    ]
    
    run_full_benchmark(MODES_TO_TEST)