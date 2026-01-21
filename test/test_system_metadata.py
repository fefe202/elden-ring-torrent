#!/usr/bin/env python3
import os
import requests
import hashlib
import time
import subprocess
import random
import string

# -------------------------------------------------------------------
# CONFIGURATION
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

# Local directory where to create temporary dummy files
TEST_DATA_DIR = "test_data_gen"

# -------------------------------------------------------------------
# DUMMY FILE GENERATOR
# -------------------------------------------------------------------
def generate_dummy_file(filename, size_mb=1):
    """Creates a test file with random content"""
    if not os.path.exists(TEST_DATA_DIR):
        os.makedirs(TEST_DATA_DIR)
    
    filepath = os.path.join(TEST_DATA_DIR, filename)
    
    # If it already exists, do not recreate it to save time (unless size changes)
    if os.path.exists(filepath):
        return filepath

    print(f"Dummy file generation '{filename}' ({size_mb} MB)...")
    with open(filepath, "wb") as f:
        # Writes random bytes
        f.write(os.urandom(int(size_mb * 1024 * 1024)))
    return filepath

def cleanup_test_data():
    """Removes temporary files at the end"""
    import shutil
    if os.path.exists(TEST_DATA_DIR):
        shutil.rmtree(TEST_DATA_DIR)
        print("Temporary files cleanup completed.")

# -------------------------------------------------------------------
# P2P INTERACTION FUNCTIONS
# -------------------------------------------------------------------
def hash_file(path):
    sha = hashlib.sha1()
    with open(path, "rb") as f:
        while chunk := f.read(1024 * 1024):
            sha.update(chunk)
    return sha.hexdigest()

def upload_file(peer_url, filepath, metadata=None):
    filename = os.path.basename(filepath)
    print(f"[UPLOAD] Uploading '{filename}' to {peer_url}...")
    
    # Internal path to the container (simulated by mapping the folder or passing relative path)
    # Ensure the file is accessible to the container (e.g., via mounted volumes).
    # For this test: We verify local path or ensure content is sent.
    # Workaround for local test: Copy file to the peer's data folder before upload
    
    # We simulate that the file is already in the peer folder (as in your original code)
    # Here we use the internal absolute path to the container as you did: /app/data/...
    internal_path = f"/app/data/{filename}"
    
    # Trick for test: physically copy the file into the peer mounted folder
    # Assume 'data_peer1' folder corresponds to peer1 (localhost:5001)
    peer_idx = PEERS.index(peer_url) + 1
    local_mount_dir = f"data_peer{peer_idx}"
    if not os.path.exists(local_mount_dir):
        os.makedirs(local_mount_dir)
    
    # Copy generated file into peer volume folder
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
            print(f"   OK ({elapsed:.2f}s)")
            return r.json()
        else:
            print(f"   Error: {r.status_code} - {r.text}")
    except Exception as e:
        print(f"   Exception: {e}")
    return None

def search_metadata(peer_url, query, expected_count=None):
    print(f"Query su {peer_url}: {query}")
    start = time.time()
    try:
        r = requests.get(f"http://{peer_url}/search", params=query, timeout=5)
        elapsed = time.time() - start
        
        if r.status_code == 200:
            results = r.json().get("results", [])
            count = len(results)
            print(f"Found {count} results in {elapsed:.2f}s")
            for res in results:
                print(f"- {res['filename']} (Host: {res.get('host', '?')})")
            
            if expected_count is not None:
                if count == expected_count:
                    print(f"TARGET REACHED: Found exactly {expected_count} files.")
                else:
                    print(f"WARNING: Expected {expected_count}, found {count}.")
            return results
        else:
            print(f"HTTP Error {r.status_code}")
    except Exception as e:
        print(f"Exception search: {e}")
    return []

# -------------------------------------------------------------------
# TEST SCENARIOS
# -------------------------------------------------------------------

def test_gsi_salting_stress():
    print("\n" + "="*60)
    print("TEST 1: GSI SALTING & AGGREGATION (Hotspot Test)")
    print("="*60)
    print("Objective: Upload 5 files with THE SAME actor and verify that search finds them all.")
    print("If Salting works, writes are distributed but reading aggregates them all.\n")

    popular_actor = "Brad Pitt"
    files_to_upload = 5
    uploaded_files = []

    # 1. Generate and Upload 5 different files
    for i in range(files_to_upload):
        fname = f"movie_brad_{i}.mp4"
        fpath = generate_dummy_file(fname, size_mb=0.1) # Small for speed
        
        # We upload to different peers in rotation for realism
        target_peer = PEERS[i % len(PEERS)]
        
        meta = {"actor": popular_actor, "id": str(i)}
        upload_file(target_peer, fpath, meta)
        uploaded_files.append(fname)
        time.sleep(0.5) # Short pause for log order

    print("\nWaiting for index propagation (consistency delay)...")
    time.sleep(2)

    # 2. Search
    print("\nPerforming search for 'hotspot' actor...")
    # Searching from a peer that has NOT uploaded anything (e.g. the last one)
    results = search_metadata(PEERS[-1], {"actor": popular_actor}, expected_count=files_to_upload)

    if results and len(results) == files_to_upload:
        print("\nSALTING TEST PASSED: All distributed files were aggregated.")
    else:
        print("\nSALTING TEST FAILED: Some files are missing.")

def test_multi_attribute_intersection():
    print("\n" + "="*60)
    print("TEST 2: METADATA INTERSECTION (AND Logic)")
    print("="*60)
    print("Objective: Verify that search correctly filters on multiple fields.")
    
    # Generate files
    f1 = generate_dummy_file("matrix.avi", 0.1)
    f2 = generate_dummy_file("john_wick.avi", 0.1)
    f3 = generate_dummy_file("notebook.avi", 0.1)

    # Upload with specific metadata
    # File 1: Keanu + Sci-Fi
    upload_file(PEERS[0], f1, {"actor": "Keanu Reeves", "genre": "Sci-Fi"})
    # File 2: Keanu + Action
    upload_file(PEERS[1], f2, {"actor": "Keanu Reeves", "genre": "Action"})
    # File 3: Ryan + Romance
    upload_file(PEERS[2], f3, {"actor": "Ryan Gosling", "genre": "Romance"})
    
    time.sleep(2)

    print("\n--- Case A: Single attribute search (Keanu) ---")
    # We expect 2 files (Matrix and John Wick)
    search_metadata(PEERS[3], {"actor": "Keanu Reeves"}, expected_count=2)

    print("\n--- Case B: Correct Intersection (Keanu AND Sci-Fi) ---")
    # We expect only Matrix
    res = search_metadata(PEERS[3], {"actor": "Keanu Reeves", "genre": "Sci-Fi"}, expected_count=1)
    if res and os.path.basename(res[0]['filename']) == "matrix.avi":
        print("   Exact match confirmed (Matrix).")
    else:
        print(f"   Incorrect match: {res}")

    print("\n--- Case C: Empty Intersection (Keanu AND Romance) ---")
    # We expect 0 results
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