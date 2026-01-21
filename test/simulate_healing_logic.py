#!/usr/bin/env python3
import hashlib
import bisect
import random

# --- Mocks ---

class MockRing:
    def __init__(self, nodes, replicas=3):
        self.replicas = replicas
        self.ring = {}
        self.sorted_keys = []
        for node in nodes:
            self.add_node(node)

    def add_node(self, node):
        for i in range(self.replicas):
            key = f"{node}:{i}"
            h = hashlib.sha1(key.encode()).hexdigest()
            self.ring[h] = node
            self.sorted_keys.append(h)
        self.sorted_keys.sort()

    def remove_node(self, node):
        keys_to_remove = [k for k, v in self.ring.items() if v == node]
        for k in keys_to_remove:
            del self.ring[k]
            self.sorted_keys.remove(k)

    def get_node(self, key_hash):
        if not self.ring: return None
        idx = bisect.bisect(self.sorted_keys, key_hash)
        if idx == len(self.sorted_keys): idx = 0
        return self.ring[self.sorted_keys[idx]]

    def get_successors(self, key_hash, count=2):
        if not self.ring: return []
        idx = bisect.bisect(self.sorted_keys, key_hash)
        unique_nodes = []
        attempts = 0
        total_virtual = len(self.sorted_keys)
        
        while len(unique_nodes) < count and attempts < total_virtual:
            if idx == len(self.sorted_keys): idx = 0
            node = self.ring[self.sorted_keys[idx]]
            if node not in unique_nodes:
                unique_nodes.append(node)
            idx += 1
            attempts += 1
        return unique_nodes

class MockPeer:
    def __init__(self, peer_id, all_nodes):
        self.self_id = peer_id
        self.ring = MockRing(all_nodes)
        self.local_manifests = []

    def _repair_manifests(self):
        print(f"[{self.__class__.__name__}:{self.self_id}] Running repair on {len(self.local_manifests)} files...")
        repairs_triggered = 0
        for manifest in self.local_manifests:
            filename = manifest["filename"]
            
            # --- THE LOGIC UNDER TEST (UPDATED) ---
            placement_key = self._get_placement_key(manifest)
            placement_hash = hashlib.sha1(placement_key.encode()).hexdigest()
            
            primary_node = self.ring.get_node(placement_hash)
            
            if primary_node != self.self_id:
                # print(f"  -> Skipping {filename} (Primary is {primary_node}, not me)")
                continue

            print(f"  -> I AM PRIMARY for {filename} (Key: {placement_key}). Checking replicas...")
            replicas = self.ring.get_successors(placement_hash, count=2)
            for r in replicas:
                if r == self.self_id: continue
                # print(f"     -> Checking replica {r}...")
                repairs_triggered += 1
        return repairs_triggered

    def _get_placement_key(self, manifest):
        return manifest["filename"]

class MockSemanticPeer(MockPeer):
    def _get_placement_key(self, manifest):
        return manifest.get("placement_key", manifest["filename"])

# --- Simulation ---

def run_simulation():
    nodes = ["node1", "node2", "node3", "node4", "node5"]
    
    # Setup Naive Scenarion
    print("\n--- TEST 1: Naive Peer Logic ---")
    p1 = MockPeer("node1", nodes)
    
    # Simulate a file that hashes to node1
    # Find a filename that hashes to node1
    target_file = None
    for i in range(100):
        fname = f"file_{i}.txt"
        h = hashlib.sha1(fname.encode()).hexdigest()
        if p1.ring.get_node(h) == "node1":
            target_file = fname
            break
            
    print(f"File '{target_file}' hashes natively to node1.")
    p1.local_manifests.append({"filename": target_file})
    
    # Run repair - should trigger
    trigger_count = p1._repair_manifests()
    print(f"Result: Triggered {trigger_count} repairs (Expected > 0)")
    
    if trigger_count > 0:
        print("Naive Logic OK")
    else:
        print("Naive Logic FAILED")

    # Setup Semantic Scenario
    print("\n--- TEST 2: Semantic Peer Logic (BROKEN) ---")
    s1 = MockSemanticPeer("node1", nodes)
    
    # Find a placement_key that DOES hash to node1
    placement_key = None
    for i in range(100):
        pk = f"genre_{i}"
        h = hashlib.sha1(pk.encode()).hexdigest()
        if s1.ring.get_node(h) == "node1":
            placement_key = pk
            break
            
    print(f"Found placement key '{placement_key}' that hashes to node1.")

    # Find a filename that DOES NOT hash to node1 (natural hash)
    semantic_file = "action_movie.txt"
    while True:
        f_hash = hashlib.sha1(semantic_file.encode()).hexdigest()
        natural_node = s1.ring.get_node(f_hash)
        if natural_node != "node1":
            break
        semantic_file += "x"
    
    print(f"File '{semantic_file}' naturally hashes to {natural_node} (NOT node1).")
    print(f"But node1 holds it because of Placement Key '{placement_key}'.")
    
    s1.local_manifests.append({
        "filename": semantic_file, 
        "metadata": {"genre": placement_key},
        "placement_key": placement_key 
    })
    
    # Run repair
    trigger_count = s1._repair_manifests()
    print(f"Result: Triggered {trigger_count} repairs (Expected > 0 for FIXED)")
    
    if trigger_count > 0:
        print("Semantic Logic WORKING (Fix Verified)")
    else:
        print("Semantic Logic STILL BROKEN")

    # Setup Metadata Scenario
    print("\n--- TEST 3: Metadata Peer Logic ---")
    # MetadataPeer inherits from NaivePeer and uses filename hashing for storage (same as Naive).
    # It should work out of the box with the base implementation.
    m1 = MockPeer("node1", nodes) # MockPeer uses Naive logic by default
    
    # Reuse target_file from Test 1 (hashes to node1)
    m1.local_manifests.append({"filename": target_file})
    
    trigger_count = m1._repair_manifests()
    print(f"Result: Triggered {trigger_count} repairs (Expected > 0)")
    
    if trigger_count > 0:
        print("Metadata Logic OK (Inherited from Naive)")
    else:
        print("Metadata Logic FAILED")

if __name__ == "__main__":
    run_simulation()
