#!/usr/bin/env python3
import os
import json
import hashlib
import requests
import threading
import random
import time
from base import BasePeer

class NaivePeer(BasePeer):
    """
    Naive implementation of the P2P protocol.
    
    Features:
    - Upload: Distributes chunks via Consistent Hashing (DHT).
    - Search: FLOODING. Queries all known peers to find a file.
      Does not use distributed indices.
    """

    def upload_file(self, filepath, metadata=None, simulate_content=False):
        """
        Uploads a file to the network:
        1. Split file into chunks.
        2. Distribute chunks to responsible nodes (DHT).
        3. Creation and distribution of Manifest (replicated on k nodes).
        """
        if not simulate_content and not os.path.exists(filepath):
            return {"error": "file inesistente", "status": "failed"}

        # 1. Chunk Preparation
        if simulate_content:
            # Generate dummy chunks based on metadata (e.g. size_mb)
            size_mb = metadata.get("size_mb", 1) if metadata else 1
            chunks = list(self._generate_dummy_chunks(size_mb))
            # Calculate simulated size
            file_size = size_mb * 1024 * 1024
        else:
            chunks = self.storage.split_file(filepath)
            file_size = os.path.getsize(filepath)

        peers_map = {}
        chunks_info = []

        # 2. Chunk Distribution
        for idx, ch_hash, data in chunks:
            responsible_node = self.ring.get_node(ch_hash)
            peers_map[ch_hash] = responsible_node
            
            # Info for manifest
            chunks_info.append({"hash": ch_hash, "peers": [responsible_node]})

            # Physical data transmission
            if responsible_node == self.self_id:
                self.storage.save_chunk(ch_hash, data)
            else:
                self._send_chunk(responsible_node, ch_hash, data)
        
        manifest = {
            "filename": os.path.basename(filepath),
            "chunks": chunks_info,
            "metadata": metadata or {},
            "size": file_size,
            "updated_at": time.time()
        }



        # 4. Manifest Distribution (Replication Factor = 3)
        # Filename hash to decide where to place manifest
        manifest_hash = hashlib.sha1(manifest["filename"].encode()).hexdigest()
        responsible_peers = self.ring.get_successors(manifest_hash, count=3)

        for peer_target in responsible_peers:
            if peer_target == self.self_id:
                self.storage.save_manifest(manifest)
                print(f"[Peer:{self.self_id}] Manifest saved locally")
            else:
                self._send_manifest(peer_target, manifest)

        return {
            "status": "stored",
            "manifest": manifest,
            "replicas": responsible_peers
        }

    def search(self, query):
        """
        NAIVE Search (Flooding / Scatter-Gather).
        
        1. Search in locally saved manifests.
        2. Sends HTTP request to ALL known peers.
        3. Aggregates results.
        
        Complexity: O(N) where N is number of known peers.
        """
        results = []
        seen_keys = set() # To avoid duplicates if multiple peers have the same file

        # --- STEP 1: Local Search ---
        local_results = self._search_local_storage(query)
        for res in local_results:
            key = f"{res['filename']}_{self.self_id}"
            if key not in seen_keys:
                results.append(res)
                seen_keys.add(key)

        # --- STEP 2: Remote Search (Flooding) ---
        # Queries only neighbors (1-hop flooding).
        is_partial = False
        
        for peer_addr in self.known_peers:
            if peer_addr == self.self_id:
                continue
            
            try:
                # Call neighbor's specific local search endpoint
                # (See api.py: /search_local)
                url = f"http://{peer_addr}/search_local"
                r = requests.get(url, params=query, timeout=2) # Low timeout to avoid blocking
                
                if r.status_code == 200:
                    remote_data = r.json().get("results", [])
                    for item in remote_data:
                        # Add host if missing, to know who to contact
                        if "host" not in item:
                            item["host"] = peer_addr
                        
                        # Deduplica
                        key = f"{item['filename']}_{item['host']}"
                        if key not in seen_keys:
                            results.append(item)
                            seen_keys.add(key)
            except Exception as e:
                # If a peer is down during search, we note it but continue (Partial Result)
                # print(f"Peer {peer_addr} non risponde alla search: {e}")
                is_partial = True

        # --- STEP 3: Conflict Resolution (LWW) ---
        # Deduplicate by filename, keeping the one with most recent timestamp
        final_results = self._resolve_conflicts(results)
        
        return {
            "results": final_results,
            "partial_result": is_partial
        }

    def _resolve_conflicts(self, raw_results):
        """
        Handles Read Repair conflicts implementing Last Write Wins (LWW).
        1. Identifies winning version (highest timestamp).
        2. Returns only the winner.
        3. (Read Repair) Updates stale nodes in background.
        """
        grouped = {}
        
        # Group by filename
        for item in raw_results:
            fname = item["filename"]
            if fname not in grouped:
                grouped[fname] = []
            grouped[fname].append(item)
            
        final_list = []
        
        for fname, versions in grouped.items():
            # Find version with highest timestamp
            winner = max(versions, key=lambda x: x.get("updated_at", 0))
            final_list.append(winner)
            
            # Read Repair: If there are losing versions, update them
            winner_ts = winner.get("updated_at", 0)
            winner_manifest = winner.get("manifest")
            
            if not winner_manifest: continue # Cannot repair without manifest
            
            for v in versions:
                v_ts = v.get("updated_at", 0)
                if v_ts < winner_ts:
                    loser_host = v.get("host")
                    print(f"Found stale version on {loser_host} (ts={v_ts} < {winner_ts}). Repairing...")
                    # Launch repair in separate thread to avoid blocking search
                    threading.Thread(target=self._send_manifest, args=(loser_host, winner_manifest)).start()

        return final_list

    def start_background_tasks(self):
        """Override to add Anti-Entropy to base tasks"""
        super().start_background_tasks()
        
        # Start repair thread
        t = threading.Thread(target=self.anti_entropy_loop, daemon=True)
        t.start()
        print(f"[Peer:{self.self_id}] Anti-Entropy Protocol Started")
    
    def anti_entropy_loop(self):
        """
        Infinite loop checking replica health.
        Runs every 20-40 seconds (randomized to avoid global synchronization).
        """
        while True:
            sleep_time = random.randint(20, 40)
            time.sleep(sleep_time)
            
            try:
                self._repair_manifests()
            except Exception as e:
                print(f"[Anti-Entropy] ⚠️ Error in loop: {e}")

    def _generate_dummy_chunks(self, size_mb):
        """Generates random chunks to simulate load."""
        num_chunks = int(size_mb) # Assume 1MB per chunk
        if num_chunks < 1: num_chunks = 1
        
        for i in range(num_chunks):
            # Create random data (to avoid trivial compression or dedup)
            # data = os.urandom(1024 * 1024) # Too slow for massive benchmark
            # Use repeated but unique data per chunk (fast)
            prefix = f"chunk_{i}".encode()
            padding = b'x' * (1024 * 1024 - len(prefix))
            data = prefix + padding
            
            chunk_hash = hashlib.sha1(data).hexdigest()
            yield (i, chunk_hash, data)

    # --- Helper Methods ---

    def _send_chunk(self, target, ch_hash, data):
        """Helper to send a chunk via HTTP"""
        try:
            url = f"http://{target}/store_chunk"
            requests.post(url, files={"chunk": data}, timeout=5)
        except Exception as e:
            print(f"Error sending chunk {ch_hash} to {target}: {e}")

    def _send_manifest(self, target, manifest):
        """Helper to send a manifest via HTTP"""
        try:
            url = f"http://{target}/store_manifest"
            requests.post(url, json=manifest, timeout=3)
            print(f"[Peer:{self.self_id}] Manifest replicated on {target}")
        except Exception as e:
            print(f"Error sending manifest to {target}: {e}")

    def _search_local_storage(self, query):
        """
        Search among manifests present on this node's disk.
        Used both internally by search() and by /search_local API.
        """
        matches = []
        local_manifests = self.storage.list_local_manifests()
        
        for m in local_manifests:
            metadata = m.get("metadata", {})
            # Match: all query fields must be present and equal (case-insensitive)
            is_match = True
            for k, v in query.items():
                # Special handling for filename search
                if k == "filename":
                    if m["filename"].lower() != str(v).lower():
                        is_match = False
                        break
                    continue

                meta_val = str(metadata.get(k, ""))
                if meta_val.lower() != str(v).lower():
                    is_match = False
                    break
            
            if is_match:
                matches.append({
                    "filename": m["filename"],
                    "metadata": metadata,
                    "host": self.self_id,
                    "updated_at": m.get("updated_at", 0),
                    "manifest": m
                })
        return matches

    def _repair_manifests(self):
        """
        Scans local files. If I am the Primary, 
        I ensure my Neighbors (Successors) have a copy.
        """
        local_manifests = self.storage.list_local_manifests()
        if not local_manifests:
            return

        print(f"Checking replication for {len(local_manifests)} files...")

        for manifest in local_manifests:
            filename = manifest["filename"]
            # 1. STORAGE HASH (How it is saved on disk)
            manifest_hash = hashlib.sha1(filename.encode()).hexdigest()
            
            # 2. ROUTING HASH (Where it should go)
            placement_key = self._get_placement_key(manifest)
            placement_hash = hashlib.sha1(placement_key.encode()).hexdigest()
            
            # Check responsibility using ROUTING hash
            primary_node = self.ring.get_node(placement_hash)
            
            if primary_node != self.self_id:
                continue

            # Find neighbors using ROUTING hash
            replicas = self.ring.get_successors(placement_hash, count=2)
            
            for replica in replicas:
                if replica == self.self_id: continue 
                
                # FIX: Check existence using STORAGE hash!
                self._ensure_replica_has_file(replica, manifest, manifest_hash)

    def _get_placement_key(self, manifest):
        """
        Determines key used to place file in Ring.
        - Naive/Metadata: uses filename.
        - Semantic: uses semantic key (e.g. genre).
        """
        return manifest["filename"]

    def _ensure_replica_has_file(self, target_peer, manifest, manifest_hash):
        """Asks peer if it has the file. If not, sends it."""
        try:
            # Ask only for manifest for now (light check)
            payload = {"manifests": [manifest_hash], "chunks": []}
            
            r = requests.post(
                f"http://{target_peer}/check_existence", 
                json=payload, 
                timeout=2
            )
            
            if r.status_code == 200:
                data = r.json()
                missing = data.get("missing_manifests", [])
                
                if manifest_hash in missing:
                    print(f"REPAIR: Sending '{manifest['filename']}' to {target_peer}")
                    # Send Manifest
                    self._send_manifest(target_peer, manifest)
                    
                    # (Optional) We could send chunks too if missing,
                    # but for now we repair metadata.
                    # To repair chunks, we would need to iterate manifest['chunks']
                    # and send those this node is responsible for.
            
        except Exception as e:
            # If the peer is down, the base failure detector will remove it.
            # Anti-entropy will choose a new successor in the next round.
            pass