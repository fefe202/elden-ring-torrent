#!/usr/bin/env python3
import os
import time
import json
import hashlib
import requests
import random
import matplotlib.pyplot as plt
import numpy as np

# -------------------------------------------------------------------
# CONFIGURAZIONE
# -------------------------------------------------------------------
# Mapping Host -> Docker ports
PEER1 = "localhost:5001" # ISP_A (Source)
PEER2 = "localhost:5002" # ISP_A (CLIENT)
PEER3 = "localhost:5003" # ISP_B (Source)
PEER4 = "localhost:5004" # ISP_A (Source)
PEER5 = "localhost:5005" # ISP_B (Source)
PEER6 = "localhost:5006" # ISP_A (Source)
PEER7 = "localhost:5007" # ISP_B (Source)
ITRACKER = "localhost:6000"

# Nomi interni Docker p4p
PEER1_INT = "peer1_p4p:5000"
PEER2_INT = "peer2_p4p:5000"
PEER3_INT = "peer3_p4p:5000"
PEER4_INT = "peer4_p4p:5000"
PEER5_INT = "peer5_p4p:5000"
PEER6_INT = "peer6_p4p:5000"
PEER7_INT = "peer7_p4p:5000"

SOURCES_HOST = [PEER1, PEER3, PEER4, PEER5, PEER6, PEER7]
SOURCES_INT  = [PEER1_INT, PEER3_INT, PEER4_INT, PEER5_INT, PEER6_INT, PEER7_INT]

# Parametri Benchmark
SIZE_MB = 5
NUM_ITERATIONS = 20

# -------------------------------------------------------------------
# UTILS
# -------------------------------------------------------------------

def generate_random_file(filename, size_mb):
    with open(filename, "wb") as f:
        f.write(os.urandom(int(size_mb * 1024 * 1024)))

def split_file_into_chunks(filename, chunk_size=1024*1024):
    chunks = []
    with open(filename, "rb") as f:
        while True:
            data = f.read(chunk_size)
            if not data: break
            ch_hash = hashlib.sha1(data).hexdigest()
            chunks.append((ch_hash, data))
    return chunks

def call_api(url, endpoint, method="GET", json_data=None, files=None):
    try:
        full_url = f"http://{url}/{endpoint}"
        if method == "POST":
            r = requests.post(full_url, json=json_data, files=files, timeout=30)
        else:
            r = requests.get(full_url, params=json_data, timeout=30)
        return r
    except Exception as e:
        return None

def upload_chunks_to_peers(chunks, peers):
    for ch_hash, data in chunks:
        for p_url in peers:
            files = {'chunk': (ch_hash, data)}
            call_api(p_url, "store_chunk", "POST", files=files)
            
def upload_manifest_flood(manifest, peers):
    for p_url in peers:
        call_api(p_url, "store_manifest", "POST", json_data=manifest)

def get_cost_between(src_peer_int, dst_peer_int, cost_map, peer_locs):
    src_loc = peer_locs.get(src_peer_int)
    dst_loc = peer_locs.get(dst_peer_int)
    if not src_loc or not dst_loc: return 999.0
    try:
        cost = cost_map.get(src_loc, {}).get(dst_loc, 999.0)
        return float(cost)
    except:
        return 999.0

# -------------------------------------------------------------------
# MAIN BENCHMARK
# -------------------------------------------------------------------

def main():
    print("P4P vs Legacy Benchmark Started (7-Node System)")
    print(f"Structure: 1 Client, {len(SOURCES_HOST)} Sources (Mixed ISPs)")
    print(f"   Iterations: {NUM_ITERATIONS}")
    
    # 0. Topology Discovery WITH WAIT LOOP
    print("\n🌍 Fetching Network Topology...")
    peer_locs = {}
    
    # Wait until ALL known peers are registered
    REQUIRED_PEERS = {PEER1_INT, PEER2_INT, PEER3_INT, PEER4_INT, PEER5_INT, PEER6_INT, PEER7_INT}
    
    for attempt in range(30):
        r = call_api(ITRACKER, "alto/network_map")
        if r:
            peer_locs = r.json().get("network_map", {})
            registered = set(peer_locs.keys())
            missing = REQUIRED_PEERS - registered
            
            if not missing:
                print(f"   All {len(REQUIRED_PEERS)} peers online.")
                break
            else:
                print(f"   Waiting for peers... Missing: {len(missing)} ({int(attempt * 2)}s)")
        else:
            print("   iTracker unreachable...")
            
        time.sleep(2)
    else:
        print("Timeout waiting for peers. Did you run 'docker-compose up'?")
        return

    r_altocost = call_api(ITRACKER, "alto/cost_map")
    cost_map = r_altocost.json().get("cost_map", {})
    
    client_loc = peer_locs.get(PEER2_INT, "???")
    print(f"   Client ({PEER2_INT}) Location: {client_loc}")
    
    print("   Sources Analysis:")
    for s_int in SOURCES_INT:
        loc = peer_locs.get(s_int, "???")
        cost = get_cost_between(PEER2_INT, s_int, cost_map, peer_locs)
        print(f"    - {s_int} ({loc}) -> Cost to Client: {cost}")

    # Storage for results
    results_agg = {
        "Legacy": {"time": [], "cost": [], "peers": []},
        "P4P": {"time": [], "cost": [], "peers": []}
    }

    # =================================================================
    # LOOP BENCHMARK
    # =================================================================
    
    for i in range(1, NUM_ITERATIONS + 1):
        # 1. Prepare Unique Files
        file_leg = f"bench_leg_{i}.bin"
        file_p4p = f"bench_p4p_{i}.bin"
        
        generate_random_file(file_leg, SIZE_MB)
        generate_random_file(file_p4p, SIZE_MB)
        
        chunks_leg = split_file_into_chunks(file_leg)
        chunks_p4p = split_file_into_chunks(file_p4p)
        
        # 2. Deploy Content to ALL Sources
        upload_chunks_to_peers(chunks_leg, SOURCES_HOST)
        upload_chunks_to_peers(chunks_p4p, SOURCES_HOST)
        
        def make_manifest(fname, chs):
            return {
                "filename": fname,
                "chunks": [{"hash": h, "peers": SOURCES_INT} for h, _ in chs], # All sources have chunks
                "metadata": {"simulated": True},
                "size": len(chs) * 1024 * 1024,
                "updated_at": time.time()
            }
            
        man_leg = make_manifest(file_leg, chunks_leg)
        man_p4p = make_manifest(file_p4p, chunks_p4p)
        
        # Flood Manifests to Client + Sources
        all_hosts = SOURCES_HOST + [PEER2]
        upload_manifest_flood(man_leg, all_hosts)
        upload_manifest_flood(man_p4p, all_hosts)
        
        # 3. Run Tests
        def run_single_test(mode, fname, strategy):
            r = call_api(PEER2, "fetch_file", "POST", json_data={"filename": fname, "strategy": strategy})
            if not r or r.status_code != 200:
                print(f"   {mode} Failed")
                return None
            
            data = r.json()
            peers = data.get("peers_involved", [])
            elapsed = r.elapsed.total_seconds()
            
            # Calculate Cost
            total_cost = 0
            for p in peers:
                c = get_cost_between(PEER2_INT, p, cost_map, peer_locs)
                total_cost += c
            
            avg_cost = total_cost / len(peers) if peers else 0
            return {"time": elapsed, "cost": avg_cost, "peers": peers}

        print(f"\n🔄 Iteration {i}/{NUM_ITERATIONS}")

        # LEGACY RUN
        res_leg = run_single_test("Legacy", file_leg, "random")
        if res_leg:
            results_agg["Legacy"]["time"].append(res_leg["time"])
            results_agg["Legacy"]["cost"].append(res_leg["cost"])
            results_agg["Legacy"]["peers"].extend(res_leg["peers"])
            print(f"   Legacy: Cost={res_leg['cost']:.2f}")

        # P4P RUN
        res_p4p = run_single_test("P4P", file_p4p, "p4p")
        if res_p4p:
            results_agg["P4P"]["time"].append(res_p4p["time"])
            results_agg["P4P"]["cost"].append(res_p4p["cost"])
            results_agg["P4P"]["peers"].extend(res_p4p["peers"])
            print(f"   P4P:    Cost={res_p4p['cost']:.2f}")

        # Cleanup
        try:
            os.remove(file_leg)
            os.remove(file_p4p)
        except: pass

    # =================================================================
    # REPORTING & VISUALIZATION
    # =================================================================
    print("\n" + "="*40)
    print("BENCHMARK AGGREGATED REPORT")
    print("="*40)
    
    def get_stats(data):
        if not data: return 0, 0
        return np.mean(data), np.std(data)

    l_cost_m, l_cost_s = get_stats(results_agg["Legacy"]["cost"])
    p_cost_m, p_cost_s = get_stats(results_agg["P4P"]["cost"])
    l_time_m, l_time_s = get_stats(results_agg["Legacy"]["time"])
    p_time_m, p_time_s = get_stats(results_agg["P4P"]["time"])

    print(f"Legacy Cost: {l_cost_m:.4f} ± {l_cost_s:.4f}")
    print(f"P4P Cost:    {p_cost_m:.4f} ± {p_cost_s:.4f}")

    # Visualization: BAR CHART WITH ERROR BARS
    try:
        labels = ['Avg Network Cost', 'Latency (s)']
        
        means_leg = [l_cost_m, l_time_m]
        stds_leg  = [l_cost_s, l_time_s]
        
        means_p4p = [p_cost_m, p_time_m]
        stds_p4p  = [p_cost_s, p_time_s]
        
        x = np.arange(len(labels))
        width = 0.35
        
        fig, ax = plt.subplots(figsize=(10, 7))
        
        # Error Bars (capsize adds the "wiskers")
        rects1 = ax.bar(x - width/2, means_leg, width, yerr=stds_leg, capsize=10, 
                        label='Legacy (Random)', color='gray', alpha=0.8, error_kw=dict(lw=2, capthick=2))
        
        rects2 = ax.bar(x + width/2, means_p4p, width, yerr=stds_p4p, capsize=10, 
                        label='P4P (ALTO)', color='green', alpha=0.9, error_kw=dict(lw=2, capthick=2))
        
        ax.set_ylabel('Score (Lower is Better)')
        ax.set_title(f'P4P Stability Analysis ({NUM_ITERATIONS} Iterations)\n7-Node System (6 Sources, 1 Client)')
        ax.set_xticks(x)
        ax.set_xticklabels(labels)
        ax.legend()
        ax.grid(axis='y', linestyle='--', alpha=0.5)
        
        def autolabel(rects):
            for rect in rects:
                height = rect.get_height()
                ax.annotate(f'{height:.2f}',
                            xy=(rect.get_x() + rect.get_width() / 2, height),
                            xytext=(0, 3), 
                            textcoords="offset points",
                            ha='center', va='bottom', fontweight='bold')
        
        autolabel(rects1)
        autolabel(rects2)
        
        plt.tight_layout()
        plt.savefig('p4p_benchmark_plot.png')
        print("\n Plot with Error Bars saved to p4p_benchmark_plot.png")
    except Exception as e:
        print(f"\n Plot error: {e}")

if __name__ == "__main__":
    main()
