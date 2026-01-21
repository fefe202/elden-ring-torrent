#!/usr/bin/env python3
import os
import hashlib
import requests
import json
import concurrent.futures
from naive import NaivePeer

class SemanticPeer(NaivePeer):
    """
    SEMANTIC PARTITIONING (Document Partitioning) implementation.
    
    Concept:
    - Data Locality: Chunks, Manifests, and Indices of the same file reside 
      ENTIRELY on the same node (or its replicas).
    - Partition Key: Uses 'genre' to decide the responsible node.
    """

    def upload_file(self, filepath, metadata=None, simulate_content=False):
        """
        Upload che forza la co-locazione dei dati.
        """
        if not metadata: metadata = {}
        
        # 1. Determina il Nodo Responsabile (Partition Key)
        # Se c'è il genere, usalo. Altrimenti usa il filename (fallback).
        partition_key = metadata.get("genre", "").lower().strip()
        if not partition_key:
            partition_key = metadata.get("titolo", "").lower().strip() or "unknown"
        
        # Hash della chiave semantica (NON del contenuto del chunk!)
        placement_hash = hashlib.sha1(partition_key.encode()).hexdigest()
        primary_node = self.ring.get_node(placement_hash)
        
        print(f"Placement: '{partition_key}' -> {primary_node}")

        # 2. File Split
        if simulate_content:
            size_mb = metadata.get("size_mb", 1)
            chunks = list(self._generate_dummy_chunks(size_mb))
            # _generate_dummy_chunks is inherited from NaivePeer
        else:
            chunks = self.storage.split_file(filepath)
        
        chunks_info = []

        # 3. Chunk Distribution (ALL TO THE SAME NODE)
        # We sacrifice storage load balancing for access speed.
        for idx, ch_hash, data in chunks:
            chunks_info.append({"hash": ch_hash, "peers": [primary_node]})
            
            # Physical transmission
            if primary_node == self.self_id:
                self.storage.save_chunk(ch_hash, data)
            else:
                self._send_chunk(primary_node, ch_hash, data)

        # 4. Manifest Creation
        manifest = {
            "filename": os.path.basename(filepath),
            "chunks": chunks_info,
            "metadata": metadata,
            "placement_key": partition_key
        }

        # 5. Manifest Transmission (To the same node as chunks)
        if primary_node == self.self_id:
            self.storage.save_manifest(manifest)
            # Local Indexing (Document Index)
            self._local_index(manifest)
        else:
            self._send_manifest(primary_node, manifest)
            # Ask remote node to index it locally
            self._remote_index_request(primary_node, manifest)

        return {"status": "stored", "manifest": manifest, "strategy": "semantic_locality"}

    def search(self, query):
        """
        Search optimized for Partition Key.
        """
        print(f"Search Query: {query}")
        
        # CASE 1: Query contains Partition Key (e.g. Genre)
        # We can go directly (Direct Routing O(1))
        if "genre" in query:
            partition_key = query["genre"].lower().strip()
            target_hash = hashlib.sha1(partition_key.encode()).hexdigest()
            target_node = self.ring.get_node(target_hash)
            
            print(f"   -> Direct routing to node {target_node} (Key: {partition_key})")
            return self._query_node(target_node, query)

        # CASE 2: Query without Partition Key (e.g. only Actor)
        # Must do Broadcast / Scatter-Gather on ALL nodes.
        # Network expensive, but necessary in this architecture.
        print(f"   -> Broadcast Search (No Partition Key)")
        results = []
        
        # Parallelize requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(self._query_node, p, query): p for p in self.known_peers}
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    res = future.result()
                    results.extend(res)
                except Exception:
                    pass
        
        # Add local results too (self)
        local_res = self._search_local_storage(query) # Inherited method from NaivePeer
        results.extend(local_res)
        
        return results

    # ==========================
    # Helper Methods
    # ==========================

    def _local_index(self, manifest):
        """
        Saves a local index. In this model, every node has ITS OWN index
        of the files it hosts. There is no global index.
        """
        # Actually, in semantic partitioning, index is implicit in local manifests.
        # NaivePeer's _search_local_storage method already iterates over local manifests.
        # To optimize, we could create a LOCAL inverted index on file,
        # but for now reusing _search_local_storage is fine.
        pass

    def _remote_index_request(self, target, manifest):
        """
        In this model, sending separate indices is not needed, 
        because the manifest already resides on target.
        """
        pass

    def _query_node(self, node, query):
        """Queries a remote node (uses existing search_local API)"""
        if node == self.self_id:
            return self._search_local_storage(query)
        
        try:
            # Use local search endpoint that looks only in node's disk
            r = requests.get(f"http://{node}/search_local", params=query, timeout=2)
            if r.status_code == 200:
                return r.json().get("results", [])
        except Exception:
            pass
        return []

    def _get_placement_key(self, manifest):
        """
        Override for Semantic Partitioning.
        Placement key is given by 'placement_key' saved in manifest
        (which corresponds to genre or title).
        """
        # If no placement_key, fallback to filename (safe behavior)
        return manifest.get("placement_key", manifest["filename"])