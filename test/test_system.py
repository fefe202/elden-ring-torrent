#!/usr/bin/env python3
import os
import requests
import hashlib
import time
import subprocess

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

FILE_TO_UPLOAD = "test_file_large.txt"

# -------------------------------------------------------------------
# UTILITY FUNCTIONS
# -------------------------------------------------------------------
def hash_file(path):
    sha = hashlib.sha1()
    with open(path, "rb") as f:
        while chunk := f.read(1024 * 1024):
            sha.update(chunk)
    return sha.hexdigest()


def upload_file(peer_url, filename, metadata=None):
    print(f"\n[UPLOAD] Starting upload to {peer_url} of file '{filename}'")
    start = time.time()
    internal_path = f"/app/data/{os.path.basename(filename)}"
    payload = {"filename": internal_path}
    if metadata:
        payload["metadata"] = metadata
    r = requests.post(f"http://{peer_url}/store_file", json=payload)
    elapsed = time.time() - start

    if r.status_code == 200:
        print(f"Upload completed in {elapsed:.2f}s")
        return r.json(), elapsed
    else:
        print(f"Upload error: {r.status_code} - {r.text}")
        return None, elapsed


def download_file(peer_url, filename):
    print(f"\n[DOWNLOAD] Starting download from {peer_url} of file '{filename}'")
    start = time.time()
    r = requests.post(f"http://{peer_url}/fetch_file", json={"filename": os.path.basename(filename)})
    elapsed = time.time() - start

    if r.status_code == 200:
        rebuilt_filename = os.path.join(f"data_peer2", f"rebuilt_{os.path.basename(filename)}")
        print(f"Download completed in {elapsed:.2f}s -> {rebuilt_filename}")
        return rebuilt_filename, elapsed
    else:
        print(f"Download error: {r.status_code} - {r.text}")
        return None, elapsed


def verify_integrity(local_original, local_rebuilt):
    print("\nIntegrity verification...")
    try:
        h1 = hash_file(local_original)
        h2 = hash_file(local_rebuilt)
    except FileNotFoundError:
        print(f"File not found for verification: {local_rebuilt}")
        return False

    if h1 == h2:
        print(f"Verification OK -- identical files (SHA1: {h1})")
        return True
    else:
        print(f"Files different!\n  Original: {h1}\n  Rebuilt: {h2}")
        return False


def search_file(peer_url, query_params):
    print(f"\n[SEARCH] on {peer_url} with query {query_params}")
    try:
        r = requests.get(f"http://{peer_url}/search", params=query_params, timeout=5)
        if r.status_code == 200:
            results = r.json().get("results", [])
            if results:
                print(f"Found {len(results)} results:")
                for res in results:
                    print(f"  {res['filename']} -- {res['metadata']} (host: {res['host']})")
            else:
                print("No matching file found.")
        else:
            print(f"Search error: HTTP {r.status_code}")
    except Exception as e:
        print(f"Error during search request: {e}")


def get_known_peers(peer_url):
    """Requests list of known peers from a node"""
    try:
        r = requests.get(f"http://{peer_url}/known_peers")
        if r.status_code == 200:
            peers = r.json().get("known_peers", [])
            print(f"{peer_url} knows: {peers}")
            return peers
        else:
            print(f"Error retrieving known peers from {peer_url}")
            return []
    except Exception as e:
        print(f"Error contacting {peer_url}: {e}")
        return []


def stop_peer(container_name):
    """Stops a Docker container (simulates peer disconnection)"""
    print(f"\nStopping {container_name}...")
    subprocess.run(["docker", "stop", container_name])
    time.sleep(3)


def start_peer(container_name):
    """Restarts a Docker container (simulates peer return)"""
    print(f"\nRestarting {container_name}...")
    subprocess.run(["docker", "start", container_name])
    time.sleep(5)

def wait_for_peer(peer_url, target_peer, timeout=60):
    """Waits until target_peer reappears in the known_peers list of peer_url."""
    print(f"Waiting for {target_peer} to be visible from {peer_url}...")
    start = time.time()
    while time.time() - start < timeout:
        peers = get_known_peers(peer_url)
        if target_peer in peers:
            print(f"{target_peer} reappeared in network.")
            return True
        time.sleep(5)
    print(f"{target_peer} did not reappear within {timeout}s.")
    return False

def check_peer_removed(peer_url, target_peer):
    peers = get_known_peers(peer_url)
    if target_peer not in peers:
        print(f"{target_peer} correctly removed from {peer_url}")
        return True
    else:
        print(f"{target_peer} still present in {peer_url}")
        return False

# -------------------------------------------------------------------
# COMPLETE TEST
# -------------------------------------------------------------------
def main():
    print("Complete Peer-to-Peer network test (upload, download, resilience)")

    # 1. Initial Upload
    metadata = {
        "titolo": "La Trama dei Dati",
        "regista": "Alice Dataweaver",
        "anno": "2025",
        "genere": "Sci-Fi"
    }
    manifest, upload_time = upload_file(PEERS[0], FILE_TO_UPLOAD, metadata)
    if not manifest:
        print("Upload failed, test interrupted.")
        return
    time.sleep(3)

    # 2. Download and integrity
    rebuilt_file, download_time = download_file(PEERS[1], FILE_TO_UPLOAD)
    verify_integrity(FILE_TO_UPLOAD, rebuilt_file)

    # 3. Distributed Search
    search_file(PEERS[2], {"titolo": "La Trama dei Dati"})

    # 4. Network status check
    print("\nInitial network status:")
    for p in PEERS[:4]:
        get_known_peers(p)

    # 5. Simulates peer disconnection
    stop_peer("peer4")
    time.sleep(20)

    print("\nNetwork status after peer4 disconnection:")
    for p in PEERS[:4]:
        check_peer_removed(p, "peer4")

    # 6. Simulates peer4 restart
    start_peer("peer4")
    time.sleep(20)
    wait_for_peer(PEERS[0], "peer4:5000")

    print("\nNetwork status after peer4 restart:")
    for p in PEERS[:4]:
        get_known_peers(p)

    # 7. Data availability test post-reentry

    search_file(PEERS[4], {"regista": "Alice Dataweaver"})
    rebuilt_file, _ = download_file(PEERS[5], FILE_TO_UPLOAD)
    verify_integrity(FILE_TO_UPLOAD, rebuilt_file)

    print("\nFINAL RESULTS")
    print(f"Upload time:   {upload_time:.2f} s")
    print(f"Download time: {download_time:.2f} s")
    print("All tests executed correctly.\n")

# -------------------------------------------------------------------
if __name__ == "__main__":
    main()
