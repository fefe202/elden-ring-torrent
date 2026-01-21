#!/usr/bin/env python3
import os
import time
import sys
import subprocess
import requests
import random
import threading
import shutil

# ====================================================
# ELDEN RING TORRENT - DEMO SCRIPT
# ====================================================

DOCKER_COMPOSE_FILE = "docker-compose.yaml"
PEER_PORT_START = 5001
PEER_API_START = 5000
NUM_PEERS = 7
LOCAL_DATA_DIR = "demo_data_gen"

def print_header(text):
    print(f"\n{'='*60}")
    print(f" {text}")
    print(f"{'='*60}")

def check_docker():
    """Checks if docker is running"""
    try:
        subprocess.check_output(["docker", "--version"])
        subprocess.check_output(["docker-compose", "--version"])
        print("[OK] Docker check passed.")
    except Exception as e:
        print("[ERR] Docker or Docker Compose not found! Please install them.")
        sys.exit(1)

def start_environment(mode="NAIVE"):
    """Starts the Docker environment with the specified mode"""
    print_header(f"STARTING ENVIRONMENT (MODE: {mode})")
    
    # 1. Tearing down old containers
    print("> Tearing down old environment...")
    subprocess.run("docker-compose down -v --remove-orphans", shell=True, check=False)
    
    # 2. Cleanup local folders
    print("> Cleaning local data folders...")
    for item in os.listdir("."):
        if item.startswith("data_peer"):
            try:
                shutil.rmtree(item)
            except:
                pass
    if os.path.exists(LOCAL_DATA_DIR):
        shutil.rmtree(LOCAL_DATA_DIR)
    os.makedirs(LOCAL_DATA_DIR)

    # 3. Start Docker Compose
    print("> Starting containers...")
    env = os.environ.copy()
    env["PEER_MODE"] = mode
    
    # Run in background but wait for it
    process = subprocess.Popen("docker-compose up -d --remove-orphans", shell=True, env=env)
    process.wait()
    
    if process.returncode != 0:
        print("[ERR] Failed to start environment.")
        sys.exit(1)
        
    print("> Waiting 20s for peers to initialize...")
    time.sleep(20)
    print("[OK] Environment Ready!")

def generate_demo_file(filename, size_mb=1):
    path = os.path.join(LOCAL_DATA_DIR, filename)
    with open(path, "wb") as f:
        f.write(os.urandom(int(size_mb * 1024 * 1024)))
    return path

def run_demo_workflow():
    print_header("RUNNING DEMO WORKFLOW")
    
    peers = [f"localhost:{PEER_PORT_START + i}" for i in range(NUM_PEERS)]
    files = ["elden_ring_trailer.mp4", "gameplay_4k.mkv", "patch_notes.txt", "soundtrack.mp3"]
    
    # 1. Upload Files
    print("\n[STEP 1] Uploading Files...")
    uploaded_meta = []
    
    for i, filename in enumerate(files):
        target_peer = peers[i % len(peers)]
        file_path = generate_demo_file(filename, size_mb=0.5) # Small files for demo
        
        # Determine peer folder name (e.g., data_peer1) to copy file into volume
        peer_idx = peers.index(target_peer) + 1
        peer_data_folder = f"data_peer{peer_idx}"
        if not os.path.exists(peer_data_folder):
            os.makedirs(peer_data_folder)
        
        shutil.copy(file_path, os.path.join(peer_data_folder, filename))
        
        # Tag Mapping for Search
        tag_map = {
            "elden_ring_trailer.mp4": "elden",
            "gameplay_4k.mkv": "gameplay",
            "patch_notes.txt": "patch",
            "soundtrack.mp3": "soundtrack"
        }

        metadata = {
            "title": filename,
            "genre": "GameMedia",
            "size": "500KB",
            "uploader": f"Peer_{peer_idx}",
            "tag": tag_map.get(filename, "misc")
        }
        
        start = time.time()
        try:
            # Internal path in container is /app/data/...
            r = requests.post(f"http://{target_peer}/store_file", json={
                "filename": f"/app/data/{filename}",
                "metadata": metadata
            })
            if r.status_code == 200:
                print(f"   [OK] Uploaded {filename} to {target_peer} ({time.time()-start:.2f}s)")
                uploaded_meta.append(metadata)
            else:
                print(f"   [ERR] Failed to upload {filename}: {r.text}")
        except Exception as e:
            print(f"   [ERR] Error uploading {filename}: {e}")
            
    # 2. Search Files
    print("\n[STEP 2] Searching Files...")
    print("Waiting 6s for index propagation...")
    time.sleep(6) 
    
    search_queries = ["elden", "gameplay", "patch", "soundtrack"]
    for q in search_queries:
        searcher_peer = random.choice(peers)
        
        # Retry logic
        found = False
        for attempt in range(3):
            try:
                # We search by 'tag' because Peers use exact match on metadata fields
                r = requests.get(f"http://{searcher_peer}/search", params={"tag": q}, timeout=5)
                results = r.json().get('results', [])
                
                if results:
                    print(f"   Query '{q}' on {searcher_peer} -> Found {len(results)} results")
                    for res in results:
                        print(f"      - {res['filename']} (Hosted by: {res.get('host', 'unknown')})")
                    found = True
                    break
                else:
                    time.sleep(1) # Wait a bit and retry
            except Exception as e:
                print(f"   [ERR] Search attempt {attempt+1} error: {e}")
        
        if not found:
            print(f"   Query '{q}' on {searcher_peer} -> Found 0 results (after retries)")

    # 3. Download Simulation
    print("\n[STEP 3] Downloading File...")
    if uploaded_meta:
        target_file = files[0]
        downloader_peer = peers[-1] # Pick the last peer
        print(f"   Downloading {target_file} from {downloader_peer}...")
        
        start = time.time()
        try:
            r = requests.post(f"http://{downloader_peer}/fetch_file", json={"filename": target_file})
            elapsed = time.time() - start
            if r.status_code == 200:
                print(f"   [OK] Download successful in {elapsed:.2f}s")
            else:
                print(f"   [ERR] Download failed: {r.text}")
        except Exception as e:
             print(f"   [ERR] Download exception: {e}")

def run_gui():
    print_header("LAUNCHING GUI")
    print("Launching Peer Monitor GUI... (Close the GUI window to stop the demo)")
    try:
        # Assuming the GUI is in peer/peer_gui.py and we are in root
        # We need to make sure python path includes current directory
        env = os.environ.copy()
        env["PYTHONPATH"] = os.getcwd()
        subprocess.run([sys.executable, "peer/peer_gui.py"], env=env)
    except KeyboardInterrupt:
        print("\nGUI Interrupted.")

def main():
    print_header("ELDEN RING TORRENT - ALL-IN-ONE DEMO")
    print("This script will set up the environment, run a test workflow, and launch the GUI.\n")
    
    check_docker()
    
    mode = input("Select Mode [NAIVE / METADATA / SEMANTIC] (default: NAIVE): ").upper().strip()
    if not mode: mode = "NAIVE"
    
    start_environment(mode)
    
    try:
        run_demo_workflow()
        
        ask_gui = input("\nDo you want to launch the GUI? [Y/n]: ").strip().lower()
        if ask_gui in ["", "y", "yes"]:
            run_gui()
            
    except KeyboardInterrupt:
        print("\n[STOP] Interrupted by user.")
    finally:
        print("\nCleaning up...")
        subprocess.run("docker-compose down", shell=True)
        print("Demo Complete. Bye!")

if __name__ == "__main__":
    main()
