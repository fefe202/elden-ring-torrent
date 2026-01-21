#!/usr/bin/env python3
import hashlib
from flask import Flask, request, jsonify, Response
import requests
import os

# ==============================================================================
# GLOBAL CONFIGURATION
# ==============================================================================
app = Flask(__name__)

# Instance populated by main.py at startup
# Contains the concrete instance (NaivePeer, MetadataPeer, etc.)
peer_instance = None 

# ==============================================================================
# CLIENT API (User Interaction)
# ==============================================================================

@app.route("/store_file", methods=["POST"])
def store_file():
    """
    User wants to upload a file to the network.
    Payload: {"filename": "/path/to/file", "metadata": {...}}
    """
    try:
        body = request.get_json(force=True)
        filepath = body.get("filename")
        metadata = body.get("metadata", {})

        simulate_content = body.get("simulate_content", False)

        # Polymorphism: calls the method of the specific class in use
        result = peer_instance.upload_file(filepath, metadata, simulate_content=simulate_content)

        status_code = 200 if result.get("status") != "failed" else 400
        return jsonify(result), status_code
    except Exception as e:
        return jsonify({"error": str(e), "status": "failed"}), 500

@app.route("/fetch_file", methods=["POST"])
def fetch_file():
    """
    User wants to download a file.
    Payload: {"filename": "video.mp4"}
    """
    try:
        body = request.get_json(force=True)
        filename = body.get("filename")
        strategy = body.get("strategy", None) # "legacy", "p4p", "random"
        
        # Calls common download logic (BasePeer)
        result = peer_instance.download_file(filename, strategy=strategy)
        
        status_code = 200 if result.get("status") in ["fetched", "stored"] else 404
        return jsonify(result), status_code
    except Exception as e:
        return jsonify({"error": str(e), "status": "failed"}), 500

@app.route("/search", methods=["GET"])
def search():
    """
    User searches for files by metadata.
    Query Params: ?actor=Brad Pitt&genre=Action
    """
    query = request.args.to_dict()
    # Polymorphism: 
    # - NaivePeer will do flooding
    # - MetadataPeer will do distributed index lookup
    search_data = peer_instance.search(query)
    
    # Structured handling (Partial Results) vs Legacy (List)
    if isinstance(search_data, dict) and "results" in search_data:
        return jsonify(search_data)
    else:
        # Fallback if search method still returns only a list
        return jsonify({"results": search_data, "partial_result": False})

@app.route("/leave", methods=["POST"])
def leave():
    """
    Manual command to gracefully remove peer from network.
    """
    body = request.get_json(force=True)
    req_peer_id = body.get("peer_id")

    # Basic security check
    if req_peer_id != peer_instance.self_id:
        return jsonify({"error": "Unauthorized leave request for other peer"}), 403

    result = peer_instance.graceful_shutdown()
    
    # Notify removal from ring
    peer_instance._remove_peer(peer_instance.self_id)
    
    return jsonify(result)


# ==============================================================================
# P2P API (Peer-to-Peer Interaction)
# ==============================================================================

@app.route("/ping")
def ping():
    """Health check used by Failure Detector."""
    return "pong", 200

# --- Data Management (Chunks and Manifests) ---

@app.route("/store_chunk", methods=["POST"])
def store_chunk():
    """Receives a physical chunk from another peer and saves it to disk."""
    f = request.files.get("chunk")
    if not f:
        return jsonify({"error": "no chunk provided"}), 400
    
    data = f.read()
    # Calculate hash here for security/verification
    ch_hash = hashlib.sha1(data).hexdigest()
    peer_instance.storage.save_chunk(ch_hash, data)
    
    return jsonify({"status": "chunk_saved", "chunk_hash": ch_hash})

@app.route("/store_manifest", methods=["POST"])
def store_manifest():
    """Receives a manifest (JSON) to host."""
    manifest = request.get_json(force=True)
    peer_instance.storage.save_manifest(manifest)
    return jsonify({"status": "manifest_saved", "filename": manifest["filename"]})

@app.route("/get_chunk/<chunk_hash>")
def get_chunk(chunk_hash):
    """Provides binary content of a chunk."""
    data = peer_instance.storage.load_chunk(chunk_hash)
    if not data:
        return jsonify({"error": "chunk_not_found"}), 404
    return Response(data, mimetype="application/octet-stream")

@app.route("/get_manifest/<filename>")
def get_manifest(filename):
    """Provides JSON of a manifest."""
    manifest = peer_instance.storage.load_manifest(filename)
    if not manifest:
        return jsonify({"error": "manifest_not_found"}), 404
    return jsonify(manifest)

@app.route("/update_manifest", methods=["POST"])
def update_manifest():
    """
    """
    Updates an existing manifest by adding a new peer that possesses a chunk.
    Used when a peer downloads a chunk and wants to notify "I have it too".
    """
    """
    data = request.get_json(force=True)
    filename = data.get("filename")
    chunk_hash = data.get("chunk_hash")
    new_peer_id = data.get("peer_id")

    success = peer_instance.storage.update_manifest_with_peer(filename, chunk_hash, new_peer_id)
    
    if success:
        return jsonify({"status": "updated"})
    else:
        return jsonify({"status": "no_change_or_error"}), 404

# --- Naive Search Management (Flooding) ---

@app.route("/search_local", methods=["GET"])
def search_local():
    """
    Specific API for NaivePeer (Flooding).
    A remote peer asks: "Do you have files matching this query?"
    """
    query = request.args.to_dict()
    # Use helper method defined in NaivePeer, but accessible if instance supports it.
    # For safety, use storage directly which is common to all.
    
    matches = []
    local_manifests = peer_instance.storage.list_local_manifests()
    
    for m in local_manifests:
        metadata = m.get("metadata", {})
        # Simple match logic (Logical AND)
        match = True
        for k, v in query.items():
            if k == "filename":
                # Match su filename (case-insensitive)
                if str(m["filename"]).lower() != str(v).lower():
                    match = False
                    break
            elif str(metadata.get(k, "")).lower() != str(v).lower():
                match = False
                break
            
    if match:
            matches.append({
                "filename": m["filename"],
                "metadata": metadata,
                "host": peer_instance.self_id,
                "updated_at": m.get("updated_at", 0),
                "manifest": m # Include full manifest for Read Repair
            })
            
    return jsonify({"results": matches})

# --- Topology Management (Join/Leave/Gossip) ---

@app.route("/join", methods=["POST"])
def join():
    """A peer requests to join the network."""
    data = request.get_json(force=True)
    new_peer = data.get("peer_id")
    
    if not new_peer:
        return jsonify({"error": "missing peer_id"}), 400

    # Add peer to local structures via BasePeer thread-safe method
    peer_instance._merge_peers([new_peer])

    # Propagate the announcement to others (Best effort)
    for p in peer_instance.known_peers:
        if p != peer_instance.self_id and p != new_peer:
            try:
                requests.post(f"http://{p}/announce", json={"peer_id": new_peer}, timeout=1)
            except:
                pass

    return jsonify({"status": "joined", "known_peers": peer_instance.known_peers})

@app.route("/announce", methods=["POST"])
def announce():
    """Notification that a peer has joined."""
    data = request.get_json(force=True)
    new_peer = data.get("peer_id")
    if new_peer:
        peer_instance._merge_peers([new_peer])
    return jsonify({"status": "ok"})

@app.route("/announce_leave", methods=["POST"])
def announce_leave():
    """Notification that a peer is leaving."""
    data = request.get_json(force=True)
    leaving_peer = data.get("peer_id")
    if leaving_peer:
        peer_instance._remove_peer(leaving_peer)
    return jsonify({"status": "ok"})

@app.route("/update_peers", methods=["POST"])
def update_peers():
    """Periodic Gossip reception."""
    data = request.get_json(force=True)
    peers_list = data.get("peers", [])
    if peers_list:
        peer_instance._merge_peers(peers_list)
    return jsonify({"status": "ok"})

@app.route("/known_peers", methods=["GET"])
def get_known_peers():
    """Debug/Utility: shows who we know."""
    return jsonify({"known_peers": peer_instance.known_peers})

# ==========================================
# GSI MANAGEMENT
# ==========================================

@app.route("/index/add", methods=["POST"])
def index_add():
    """
    RPC Endpoint: A peer asks us to save an entry in our local shard.
    """
    try:
        data = request.get_json(force=True)
        key = data.get("key")      # es. "genre:sci-fi:1"
        entry = data.get("entry")  # {filename, metadata, host}
        
        if key and entry:
            peer_instance.storage.save_index_entry(key, entry)
            return jsonify({"status": "ok"})
        return jsonify({"error": "missing_data"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/index/get", methods=["GET"])
def index_get():
    """
    RPC Endpoint: A peer asks us for data for a specific shard.
    """
    key = request.args.get("key") # es. "genre:sci-fi:1"
    if not key:
        return jsonify({"results": []})
    
    results = peer_instance.storage.get_index_entries(key)
    return jsonify({"results": results})

@app.route("/stats", methods=["GET"])
def stats():
    """Returns internal metrics for benchmark suite"""
    storage_stats = peer_instance.storage.get_storage_stats()
    
    return jsonify({
        "peer_id": peer_instance.self_id,
        "mode": type(peer_instance).__name__,
        "storage": storage_stats
    })

@app.route("/debug/ring", methods=["GET"])
def debug_ring():
    """Shows internal Hash Ring state"""
    if not peer_instance or not peer_instance.ring:
        return jsonify({"error": "Ring not initialized"})
    
    # 1. How many physical nodes does the ring know?
    # Extract unique values from ring dictionary
    unique_nodes = list(set(peer_instance.ring.ring.values()))
    
    # 2. How many virtual points are there?
    virtual_points = len(peer_instance.ring.sorted_keys)
    
    return jsonify({
        "self_id": peer_instance.self_id,
        "total_virtual_points": virtual_points,
        "unique_nodes_count": len(unique_nodes),
        "unique_nodes_list": sorted(unique_nodes),
        "replicas_setting": peer_instance.ring.replicas
    })
    
@app.route("/check_existence", methods=["POST"])
def check_existence():
    """
    API for Anti-Entropy.
    A peer asks: "Do you have these chunks/manifests?"
    Input: {"manifests": ["hash1", "hash2"], "chunks": ["ch1", "ch2"]}
    Output: {"missing_manifests": [...], "missing_chunks": [...]}
    """
    data = request.get_json(force=True)
    manifests_to_check = data.get("manifests", [])
    chunks_to_check = data.get("chunks", [])
    
    missing_m = []
    missing_c = []
    
    # Check Manifest existence (based on filename hash, not content)
    # Storage saves manifests as <hash_filename>.manifest.json
    for m_hash in manifests_to_check:
        # Ricostruiamo il path atteso
        path = peer_instance.storage._manifest_filename(m_hash).replace(".manifest.json", "")
        # Usiamo una logica interna a storage per verificare l'esistenza senza caricare
        # Qui assumiamo un check grezzo sul file
        expected_path = os.path.join(peer_instance.storage.data_dir, f"{m_hash}.manifest.json")
        if not os.path.exists(expected_path):
            missing_m.append(m_hash)

    # Check Chunk existence
    for c_hash in chunks_to_check:
        expected_path = os.path.join(peer_instance.storage.data_dir, c_hash)
        if not os.path.exists(expected_path):
            missing_c.append(c_hash)
            
    return jsonify({
        "missing_manifests": missing_m, 
        "missing_chunks": missing_c
    })