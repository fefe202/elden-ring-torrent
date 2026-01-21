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
# MASSIVE CLOUD CONFIGURATION (HEAVY WORKLOAD)
# ==============================================================================

# Automatic Generation of 30 Hybrid Peers (5 Workers x 6 Containers)
PEERS = []
for w in range(1, 6): # worker-1 to worker-5
    for p in range(1, 7): # port 5001 to 5005 (map to internal 5000)
        # Use external port mapped by startup-hybrid.sh
        PEERS.append(f"worker-{w}:{5000+p}")

OUTPUT_FILE = "benchmark_cloud_results.json"

# --- Load Parameters (MASSIVE GOOGLE CLOUD SCALING) ---
NUM_FILES = 5000         # 10x: Heavy load to fill DHT and test storage
SEARCH_QUERIES = 5000    # 5x: Prolonged stress test for queues and concurrency
WARMUP_QUERIES = 200     # More warmup to stabilize routing at large scale
WAIT_AFTER_UPLOAD = 60   # Extra time for Gossip convergence with many files

# --- Timeout ---
CONN_TIMEOUT = 5        # Connection timeout
READ_TIMEOUT = 10       # Read timeout (high to avoid false positives under stress)

# --- Data Distribution ---
POPULAR_ACTORS = ["Brad Pitt", "Scarlett Johansson", "Leonardo DiCaprio", "Tom Hanks", "Meryl Streep"]
RARE_ACTORS = [f"IndieActor_{i}" for i in range(500)] # Many more rare actors
GENRES = ["Action", "Sci-Fi", "Drama", "Comedy", "Horror", "Documentary", "Thriller", "Romance"]

# ==============================================================================
# STATS UTILS
# ==============================================================================

def calculate_gini(data):
    """Calculates Gini Coefficient to measure load balancing"""
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
    # Approximated Zipfian Distribution (80/20)
    if random.random() < 0.8:
        return random.choice(POPULAR_ACTORS)
    else:
        return random.choice(RARE_ACTORS)

# ==============================================================================
# HEALTH CHECK
# ==============================================================================

def wait_for_cluster():
    print(f"\n[INIT] Cluster health check ({len(PEERS)} nodes)...")
    alive_count = 0
    with requests.Session() as s:
        for p in PEERS:
            try:
                # Ask for quick stats to see if it is alive
                r = s.get(f"http://{p}/known_peers", timeout=2)
                if r.status_code == 200:
                    alive_count += 1
            except:
                pass
    
    print(f"Active nodes: {alive_count}/{len(PEERS)}")
    if alive_count < len(PEERS) * 0.8: # If less than 80% is alive, abort
        print("[ERR] TOO MANY DEAD NODES. Check workers!")
        sys.exit(1)
    else:
        print("Cluster ready. Starting Massive Benchmark.")

# ==============================================================================
# TEST PHASES
# ==============================================================================

def run_upload_phase(session):
    print(f"\n[PHASE 1] Massive Upload of {NUM_FILES} files...")
    latencies = []
    errors = 0
    
    for i in range(NUM_FILES):
        filename = f"movie_{i}.bin"
        # In Cloud we do not physically copy the file (shutil), we only simulate the API request.
        # The Peer will receive the path and create a dummy if needed, or index only metadata.
        internal_path = f"/app/data/{filename}" 
        
        metadata = {
            "actor": get_weighted_actor(),
            "genre": random.choice(GENRES),
            "year": random.randint(1980, 2024),
            "size_mb": 1 # Simulated
        }
        
        peer_addr = random.choice(PEERS)
        
        start = time.time()
        try:
            r = session.post(f"http://{peer_addr}/store_file", json={
                "filename": internal_path,
                "metadata": metadata,
                "simulate_content": True # Optional flag if your code supports it
            }, timeout=CONN_TIMEOUT)
            
            if r.status_code == 200:
                latencies.append(time.time() - start)
            else:
                errors += 1
        except Exception as e:
            errors += 1

        # Simple progress bar
        if i > 0 and i % 50 == 0:
            print(f"   ... {i}/{NUM_FILES} (Errors: {errors})")
            
    print(f"Upload completed. Total errors: {errors}")
    return latencies

def run_search_phase(session):
    print(f"\n[PHASE 2-A] Warmup ({WARMUP_QUERIES} unmeasured queries)...")
    # Warmup to open TCP connections and fill ARP/DNS cache
    for _ in range(WARMUP_QUERIES):
        try:
            q = {"actor": random.choice(POPULAR_ACTORS)}
            target = random.choice(PEERS)
            session.get(f"http://{target}/search", params=q, timeout=2)
        except: pass

    print(f"[PHASE 2-B] Stress Test ({SEARCH_QUERIES} measured queries)...")
    latencies = []
    timeouts = 0
    
    for i in range(SEARCH_QUERIES):
        q_type = random.random()
        query = {}
        
        # Complex query mix
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
            
    print(f"Search completed. Timeouts/Errors: {timeouts}")
    return latencies

def collect_metrics(session):
    print("\n[PHASE 3] Collecting Cluster Metrics...")
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
    # Optimized HTTP Session Setup (Keep-Alive)
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
    
    print(f"Sleeping {WAIT_AFTER_UPLOAD}s (Gossip Convergence)...")
    time.sleep(WAIT_AFTER_UPLOAD)
    
    # 2. Search
    search_lat = run_search_phase(session)
    
    # 3. Stats
    metrics = collect_metrics(session)
    
    # Report
    avg_search = statistics.mean(search_lat) if search_lat else 0
    p95 = get_percentile(search_lat, 0.95)
    p99 = get_percentile(search_lat, 0.99)
    
    # Calculate Throughput (Client-side observed)
    total_time = sum(search_lat) if search_lat else 1
    throughput = len(search_lat) / total_time if total_time > 0 else 0

    print(f"\nFINAL RESULTS [{mode_label}]")
    print(f"   files={NUM_FILES}, queries={SEARCH_QUERIES}, nodes={len(PEERS)}")
    print("-" * 40)
    print(f"   Avg Latency:  {avg_search:.4f} s")
    print(f"   P99 Latency:  {p99:.4f} s (Tail)")
    print(f"   Throughput:   {throughput:.2f} req/s")
    print(f"   Gini Coeff:   {metrics['gini']:.4f} (Load Balance)")
    print(f"   DATA: {metrics['storage_loads']}")

    # Save to JSON
    results = {
        "mode": mode_label,
        "config": {"files": NUM_FILES, "queries": SEARCH_QUERIES, "nodes": len(PEERS)},
        "upload_latencies": up_lat,
        "search_latencies": search_lat,
        "metrics": metrics
    }

    dynamic_filename = f"benchmark_results_{mode_label}.json"
    
    with open(dynamic_filename, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nData saved in {dynamic_filename}")

if __name__ == "__main__":
    # In Cloud, we launch one mode at a time based on how we started the VMs
    # Example: python3 benchmark_cloud_heavy.py NAIVE
    if len(sys.argv) > 1:
        MODE = sys.argv[1]
    else:
        MODE = "UNKNOWN_MODE" # Fallback
    
    run_cloud_benchmark(MODE)