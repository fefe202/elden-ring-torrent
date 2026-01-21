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
# MASSIVE CONFIGURATION
# ==============================================================================
# Peer definition (ensure docker-compose exposes these ports)
NUM_PEERS = 10
PEERS = [f"localhost:{5001 + i}" for i in range(NUM_PEERS)]

OUTPUT_FILE = "benchmark_results.json"
TEMP_GEN_DIR = "bench_temp_gen"

# --- Load Parameters (SCALING X10) ---
NUM_FILES = 20          
FILE_SIZE_MB = 1       
SEARCH_QUERIES = 50     
WAIT_AFTER_UPLOAD = 10  

# --- Data Distribution ---
# Zipf Law: Few actors appear in many movies
POPULAR_ACTORS = ["Brad Pitt", "Scarlett Johansson", "Leonardo DiCaprio"]
RARE_ACTORS = [f"IndieActor_{i}" for i in range(100)]
GENRES = ["Action", "Sci-Fi", "Drama", "Comedy", "Horror", "Documentary"]

# ==============================================================================
# 🛠 UTILS & SETUP
# ==============================================================================

def calculate_gini(data):
    """Calculates Gini Coefficient (0=perfect equity, 1=max inequality)"""
    if not data: return 0
    sorted_data = sorted(data)
    height, area = 0, 0
    for value in sorted_data:
        height += value
        area += height - value / 2.
    fair_area = height * len(data) / 2.
    return (fair_area - area) / fair_area

def get_percentile(data, percentile):
    """Calculates percentile (e.g. 0.95 or 0.99) for Tail Latency"""
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
    """Cleans generated files and local volume folders"""
    print("Pre-test local data cleanup...")
    if os.path.exists(TEMP_GEN_DIR):
        shutil.rmtree(TEMP_GEN_DIR)
    
    # Cleans data_peerX folders created by docker volumes
    for i in range(1, NUM_PEERS + 2): # + safe margin
        d = f"data_peer{i}"
        if os.path.exists(d):
            try:
                shutil.rmtree(d)
                os.makedirs(d) # Recreate empty
            except Exception as e:
                print(f"Warning cleaning {d}: {e}")

def generate_dummy_file(filename, size_mb):
    """Generates a random binary file"""
    ensure_dir(TEMP_GEN_DIR)
    path = os.path.join(TEMP_GEN_DIR, filename)
    with open(path, "wb") as f:
        f.write(os.urandom(int(size_mb * 1024 * 1024)))
    return path

def get_weighted_actor():
    """Returns an actor based on 80/20 distribution (Hotspot simulation)"""
    if random.random() < 0.8: # 80% probability to choose among the 3 popular ones
        return random.choice(POPULAR_ACTORS)
    else:
        return random.choice(RARE_ACTORS)

# ==============================================================================
# DOCKER MANAGEMENT
# ==============================================================================

def restart_env(mode):
    print(f"\n[ENV] Restarting Docker environment in mode: {mode}")
    
    # 1. Brutal shutdown and volume cleanup
    subprocess.run("docker-compose down -v --remove-orphans", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    # 2. Local host folder cleanup (to avoid residuals from previous tests)
    clean_local_data()
    
    # 3. Environment Variables Setup
    env = os.environ.copy()
    env["PEER_MODE"] = mode
    
    # 4. Start with LOGGING TO FILE
    print("[ENV] Docker Compose Up (Logs -> docker_runtime.log)...")
    
    # Open file in write mode (overwrites every time)
    with open("docker_runtime.log", "w") as logfile:
        subprocess.Popen("docker-compose up --force-recreate", shell=True, env=env, stdout=logfile, stderr=logfile)
    
    print("waiting for cluster stabilization (30s)...")
    time.sleep(30) 
    
    # Check liveness
    try:
        r = requests.get(f"http://{PEERS[0]}/known_peers", timeout=2)
        print(f"[ENV] Cluster active. Known nodes: {len(r.json().get('known_peers', []))}")
    except Exception as e:
        print(f"[ENV] Warning: Cluster not responding! Check docker_runtime.log for errors.")

# ==============================================================================
# METRICS AND TESTS
# ==============================================================================

def run_upload_phase():
    print(f"\n[PHASE 1] Upload of {NUM_FILES} files...")
    latencies = []
    
    for i in range(NUM_FILES):
        # Setup Data
        filename = f"movie_{i}.bin"
        local_src = generate_dummy_file(filename, FILE_SIZE_MB)
        
        actor = get_weighted_actor()
        genre = random.choice(GENRES)
        metadata = {"actor": actor, "genre": genre, "year": random.randint(1990, 2025)}
        
        # Target Selection (Round Robin)
        peer_addr = PEERS[i % len(PEERS)]
        peer_idx = PEERS.index(peer_addr) + 1 # data_peer1, data_peer2...
        
        # Simulation: Copy file to peer volume folder
        # Peer expects to find file in /app/data/{filename}
        # We put it in ./data_peerX/{filename} which is mapped
        target_dir = f"data_peer{peer_idx}"
        ensure_dir(target_dir)
        try:
            shutil.copy(local_src, os.path.join(target_dir, filename))
        except Exception as e:
            print(f"Copy error for {filename}: {e}")
        
        internal_path = f"/app/data/{filename}"
        
        # Request Execution (Content Simulation for max performance)
        start = time.time()
        try:
            r = requests.post(f"http://{peer_addr}/store_file", json={
                "filename": f"/app/data/simulated_{filename}", # Virtual path
                "metadata": {**metadata, "size_mb": FILE_SIZE_MB}, # Merge size
                "simulate_content": True
            }, timeout=10)
            
            if r.status_code == 200:
                latencies.append(time.time() - start)
            else:
                print(f"[ERR] Err Upload {filename}: {r.status_code}")
        except Exception as e:
            print(f"[ERR] Exception Upload {peer_addr}: {e}")
        except Exception as e:
            print(f"[ERR] Exception Upload {peer_addr}: {e}")

        if i % 20 == 0: print(f"   ... {i}/{NUM_FILES} uploaded")
            
    return latencies

def run_search_phase():
    print(f"\n[PHASE 2] Executing {SEARCH_QUERIES} mixed queries...")
    latencies = []
    
    # Query types to stress different aspects
    # 1. Hotspot (Brad Pitt) -> Stresses Semantic Single Node & GSI Salting Aggregation
    # 2. Rare -> Stresses Scatter-Gather (must search everywhere)
    # 3. Intersection -> Stresses filtering logic
    
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
            pass # Timeouts counted as failures or ignored in avg latency calculation

        if i % 50 == 0: print(f"   ... {i}/{SEARCH_QUERIES} queries executed")
            
    return latencies

def collect_system_metrics():
    print("\n[PHASE 3] Collecting node stats...")
    storage_loads = []
    total_files_network = 0
    
    for p in PEERS:
        try:
            r = requests.get(f"http://{p}/stats", timeout=2)
            if r.status_code == 200:
                data = r.json().get("storage", {})
                # Load = Chunk + Indexes (Manifest count less, but we include them for completeness)
                load = data.get("chunks_count", 0) + data.get("indexes_count", 0)
                storage_loads.append(load)
                total_files_network += data.get("total_files", 0)
            else:
                storage_loads.append(0)
        except:
            storage_loads.append(0)
            
    # Calculate Statistical Metrics
    if storage_loads:
        avg_load = statistics.mean(storage_loads)
        # High Variance = Imbalance (Hotspot)
        # Low Variance = Good Load Balancing
        variance = statistics.variance(storage_loads) if len(storage_loads) > 1 else 0
        gini = calculate_gini(storage_loads)
    else:
        avg_load, variance, gini = 0, 0, 0
        
    print(f"   Node Load Distribution: {storage_loads}")
    print(f"   Load Variance: {variance:.2f}")
    
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
        print(f"START BENCHMARK: {mode}")
        print("="*60)
        
        restart_env(mode)
        
        # Results structure
        mode_results = {
            "upload_latency": [],
            "search_latency": [],
            "metrics": {}
        }
        
        try:
            # 1. Write Stress
            mode_results["upload_latency"] = run_upload_phase()
            
            print(f"Pause {WAIT_AFTER_UPLOAD}s for background indexing...")
            time.sleep(WAIT_AFTER_UPLOAD)
            
            # 2. Read Stress
            mode_results["search_latency"] = run_search_phase()
            
            # 3. System Stats
            mode_results["metrics"] = collect_system_metrics()
            
        except KeyboardInterrupt:
            print("\n[STOP] Manual interruption...")
            break
        except Exception as e:
            print(f"\n[ERR] Critical error in benchmark {mode}: {e}")
            
        final_report[mode] = mode_results
        
        final_report[mode] = mode_results
        
        # Advanced Statistics
        up_lats = mode_results["upload_latency"]
        search_lats = mode_results["search_latency"]
        
        avg_up = statistics.mean(up_lats) if up_lats else 0
        avg_search = statistics.mean(search_lats) if search_lats else 0
        
        # P99 Latency (The worst 1% of cases)
        p99_search = get_percentile(search_lats, 0.99) if search_lats else 0
        
        # Throughput (Requests / Second)
        # Sum of all latency times (client-side approximation)
        # For real throughput, total wall-clock time of the loop would be needed
        total_time_search = sum(search_lats) if search_lats else 1
        throughput = len(search_lats) / total_time_search if total_time_search > 0 else 0

        metrics = mode_results['metrics']
        metrics['throughput'] = throughput # Saturation (Req/s)
        
        print(f"\nADVANCED SUMMARY {mode}:")
        print(f"   [Time] Avg Search: {avg_search:.4f}s | P99 Search: {p99_search:.4f}s")
        print(f"   [Perf] Throughput: {throughput:.2f} req/s (Client-side approx)")
        print(f"   [Load] Imbalance:  {metrics.get('load_variance', 0):.2f} (Var)")
        print(f"   [Fair] Gini Coeff: {metrics.get('gini_coefficient', 0):.4f} (0=Perfect, 1=Bad)")

    # Final teardown
    print("\nShutting down environment...")
    subprocess.run("docker-compose down", shell=True)
    clean_local_data()

    # Save JSON
    with open(OUTPUT_FILE, "w") as f:
        json.dump(final_report, f, indent=2)
    
    print(f"\n[OK] Benchmark completed! Data saved in: {os.path.abspath(OUTPUT_FILE)}")
    print("Now use 'benchmark_plotter.py' to generate plots.")

if __name__ == "__main__":
    # You can comment out modes you don't want to test to save time
    MODES_TO_TEST = [
        "NAIVE",    # Flooding
        "METADATA", # GSI + Salting
        "SEMANTIC"  # Data Locality
    ]
    
    run_full_benchmark(MODES_TO_TEST)