#!/usr/bin/env python3
import hashlib
from flask import Flask, request, jsonify, Response
import requests
import os

# ==============================================================================
# CONFIGURAZIONE GLOBALE
# ==============================================================================
app = Flask(__name__)

# Questa variabile verrà popolata da main.py all'avvio.
# Contiene l'istanza concreta (NaivePeer, MetadataPeer, etc.)
peer_instance = None 

# ==============================================================================
# API CLIENT (Interazione Utente)
# ==============================================================================

@app.route("/store_file", methods=["POST"])
def store_file():
    """
    L'utente vuole caricare un file nella rete.
    Payload: {"filename": "/path/to/file", "metadata": {...}}
    """
    try:
        body = request.get_json(force=True)
        filepath = body.get("filename")
        metadata = body.get("metadata", {})

        simulate_content = body.get("simulate_content", False)

        # Polimorfismo: chiama il metodo della classe specifica in uso
        result = peer_instance.upload_file(filepath, metadata, simulate_content=simulate_content)

        status_code = 200 if result.get("status") != "failed" else 400
        return jsonify(result), status_code
    except Exception as e:
        return jsonify({"error": str(e), "status": "failed"}), 500

@app.route("/fetch_file", methods=["POST"])
def fetch_file():
    """
    L'utente vuole scaricare un file.
    Payload: {"filename": "video.mp4"}
    """
    try:
        body = request.get_json(force=True)
        filename = body.get("filename")
        
        # Chiama la logica comune di download (BasePeer)
        result = peer_instance.download_file(filename)
        
        status_code = 200 if result.get("status") in ["fetched", "stored"] else 404
        return jsonify(result), status_code
    except Exception as e:
        return jsonify({"error": str(e), "status": "failed"}), 500

@app.route("/search", methods=["GET"])
def search():
    """
    L'utente cerca file per metadata.
    Query Params: ?actor=Brad Pitt&genre=Action
    """
    query = request.args.to_dict()
    # Polimorfismo: 
    # - NaivePeer farà flooding
    # - MetadataPeer farà lookup su indice distribuito
    search_data = peer_instance.search(query)
    
    # Gestione strutturata (Partial Results) vs Legacy (List)
    if isinstance(search_data, dict) and "results" in search_data:
        return jsonify(search_data)
    else:
        # Fallback se il metodo search ritorna ancora solo una lista
        return jsonify({"results": search_data, "partial_result": False})

@app.route("/leave", methods=["POST"])
def leave():
    """
    Comando manuale per far uscire il peer dalla rete in modo pulito.
    """
    body = request.get_json(force=True)
    req_peer_id = body.get("peer_id")

    # Verifica di sicurezza basilare
    if req_peer_id != peer_instance.self_id:
        return jsonify({"error": "Unauthorized leave request for other peer"}), 403

    result = peer_instance.graceful_shutdown()
    
    # Notifica la rimozione dall'anello
    peer_instance._remove_peer(peer_instance.self_id)
    
    return jsonify(result)


# ==============================================================================
# API P2P (Interazione tra Peer)
# ==============================================================================

@app.route("/ping")
def ping():
    """Health check usato dal Failure Detector."""
    return "pong", 200

# --- Gestione Dati (Chunk e Manifest) ---

@app.route("/store_chunk", methods=["POST"])
def store_chunk():
    """Riceve un chunk fisico da un altro peer e lo salva su disco."""
    f = request.files.get("chunk")
    if not f:
        return jsonify({"error": "no chunk provided"}), 400
    
    data = f.read()
    # Calcoliamo l'hash qui per sicurezza/verifica
    ch_hash = hashlib.sha1(data).hexdigest()
    peer_instance.storage.save_chunk(ch_hash, data)
    
    return jsonify({"status": "chunk_saved", "chunk_hash": ch_hash})

@app.route("/store_manifest", methods=["POST"])
def store_manifest():
    """Riceve un manifest (JSON) da ospitare."""
    manifest = request.get_json(force=True)
    peer_instance.storage.save_manifest(manifest)
    return jsonify({"status": "manifest_saved", "filename": manifest["filename"]})

@app.route("/get_chunk/<chunk_hash>")
def get_chunk(chunk_hash):
    """Fornisce il contenuto binario di un chunk."""
    data = peer_instance.storage.load_chunk(chunk_hash)
    if not data:
        return jsonify({"error": "chunk_not_found"}), 404
    return Response(data, mimetype="application/octet-stream")

@app.route("/get_manifest/<filename>")
def get_manifest(filename):
    """Fornisce il JSON di un manifest."""
    manifest = peer_instance.storage.load_manifest(filename)
    if not manifest:
        return jsonify({"error": "manifest_not_found"}), 404
    return jsonify(manifest)

@app.route("/update_manifest", methods=["POST"])
def update_manifest():
    """
    Aggiorna un manifest esistente aggiungendo un nuovo peer che possiede un chunk.
    Usato quando un peer scarica un chunk e vuole notificare "ce l'ho anche io".
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

# --- Gestione Ricerca Naive (Flooding) ---

@app.route("/search_local", methods=["GET"])
def search_local():
    """
    API specifica per il NaivePeer (Flooding).
    Un peer remoto ci chiede: "Hai file che corrispondono a questa query?"
    """
    query = request.args.to_dict()
    # Usiamo il metodo helper definito in NaivePeer, ma accessibile se l'istanza lo supporta.
    # Per sicurezza, usiamo direttamente lo storage che è comune a tutti.
    
    matches = []
    local_manifests = peer_instance.storage.list_local_manifests()
    
    for m in local_manifests:
        metadata = m.get("metadata", {})
        # Logica di match semplice (AND logico)
        if all(str(metadata.get(k, "")).lower() == str(v).lower() for k, v in query.items()):
            matches.append({
                "filename": m["filename"],
                "metadata": metadata,
                "host": peer_instance.self_id,
                "updated_at": m.get("updated_at", 0),
                "manifest": m # Include full manifest for Read Repair
            })
            
    return jsonify({"results": matches})

# --- Gestione Topologia (Join/Leave/Gossip) ---

@app.route("/join", methods=["POST"])
def join():
    """Un nuovo peer chiede di entrare nella rete."""
    data = request.get_json(force=True)
    new_peer = data.get("peer_id")
    
    if not new_peer:
        return jsonify({"error": "missing peer_id"}), 400

    # Aggiungi il peer alle strutture locali tramite il metodo thread-safe di BasePeer
    peer_instance._merge_peers([new_peer])

    # Propaga l'annuncio agli altri (Best effort)
    # (Nota: qui potremmo delegare a un metodo di peer_instance per pulizia, ma va bene anche qui)
    for p in peer_instance.known_peers:
        if p != peer_instance.self_id and p != new_peer:
            try:
                requests.post(f"http://{p}/announce", json={"peer_id": new_peer}, timeout=1)
            except:
                pass

    return jsonify({"status": "joined", "known_peers": peer_instance.known_peers})

@app.route("/announce", methods=["POST"])
def announce():
    """Qualcuno ci avvisa che c'è un nuovo peer."""
    data = request.get_json(force=True)
    new_peer = data.get("peer_id")
    if new_peer:
        peer_instance._merge_peers([new_peer])
    return jsonify({"status": "ok"})

@app.route("/announce_leave", methods=["POST"])
def announce_leave():
    """Qualcuno ci avvisa che un peer sta uscendo."""
    data = request.get_json(force=True)
    leaving_peer = data.get("peer_id")
    if leaving_peer:
        peer_instance._remove_peer(leaving_peer)
    return jsonify({"status": "ok"})

@app.route("/update_peers", methods=["POST"])
def update_peers():
    """Ricezione Gossip periodico."""
    data = request.get_json(force=True)
    peers_list = data.get("peers", [])
    if peers_list:
        peer_instance._merge_peers(peers_list)
    return jsonify({"status": "ok"})

@app.route("/known_peers", methods=["GET"])
def get_known_peers():
    """Debug/Utility: mostra chi conosciamo."""
    return jsonify({"known_peers": peer_instance.known_peers})

# ==========================================
# GESTIONE GSI (Global Secondary Indexes)
# ==========================================

@app.route("/index/add", methods=["POST"])
def index_add():
    """
    Endpoint RPC: Un peer ci chiede di salvare un'entry nel nostro shard locale.
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
    Endpoint RPC: Un peer ci chiede i dati per uno shard specifico.
    """
    key = request.args.get("key") # es. "genre:sci-fi:1"
    if not key:
        return jsonify({"results": []})
    
    results = peer_instance.storage.get_index_entries(key)
    return jsonify({"results": results})

@app.route("/stats", methods=["GET"])
def stats():
    """Restituisce metriche interne per il benchmark suite"""
    storage_stats = peer_instance.storage.get_storage_stats()
    
    return jsonify({
        "peer_id": peer_instance.self_id,
        "mode": type(peer_instance).__name__,
        "storage": storage_stats
    })

@app.route("/debug/ring", methods=["GET"])
def debug_ring():
    """Mostra lo stato interno dell'Hash Ring"""
    if not peer_instance or not peer_instance.ring:
        return jsonify({"error": "Ring not initialized"})
    
    # 1. Quanti nodi fisici conosce l'anello?
    # Estrarre i valori unici dal dizionario ring
    unique_nodes = list(set(peer_instance.ring.ring.values()))
    
    # 2. Quanti punti virtuali ci sono?
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
    API per Anti-Entropy.
    Un peer ci chiede: "Hai questi chunk/manifest?"
    Input: {"manifests": ["hash1", "hash2"], "chunks": ["ch1", "ch2"]}
    Output: {"missing_manifests": [...], "missing_chunks": [...]}
    """
    data = request.get_json(force=True)
    manifests_to_check = data.get("manifests", [])
    chunks_to_check = data.get("chunks", [])
    
    missing_m = []
    missing_c = []
    
    # Verifica esistenza Manifest (basata su hash del filename, non contenuto)
    # Nota: il tuo storage salva i manifest come <hash_filename>.manifest.json
    for m_hash in manifests_to_check:
        # Ricostruiamo il path atteso
        path = peer_instance.storage._manifest_filename(m_hash).replace(".manifest.json", "")
        # Usiamo una logica interna a storage per verificare l'esistenza senza caricare
        # Qui assumiamo un check grezzo sul file
        expected_path = os.path.join(peer_instance.storage.data_dir, f"{m_hash}.manifest.json")
        if not os.path.exists(expected_path):
            missing_m.append(m_hash)

    # Verifica esistenza Chunk
    for c_hash in chunks_to_check:
        expected_path = os.path.join(peer_instance.storage.data_dir, c_hash)
        if not os.path.exists(expected_path):
            missing_c.append(c_hash)
            
    return jsonify({
        "missing_manifests": missing_m, 
        "missing_chunks": missing_c
    })