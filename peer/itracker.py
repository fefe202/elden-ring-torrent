from datetime import datetime
import random
from flask import Flask, request, jsonify
import threading
import time

app = Flask(__name__)

# peer_map legacy: isp -> region -> [peer_id,...]
peer_map = {}

# peer_location: peer_id -> "isp:region"
peer_location = {}

# cost_map: location_from -> { location_to: cost }
# valori di esempio; puoi renderli dinamici o calcolarli in base a metriche
cost_map = {
    "isp_a:region_a": {"isp_a:region_a": 0.1, "isp_a:region_b": 0.3, "isp_b:region_a": 2.0},
    "isp_a:region_b": {"isp_a:region_a": 0.3, "isp_a:region_b": 0.1, "isp_b:region_a": 1.8},
    "isp_b:region_a": {"isp_a:region_a": 2.0, "isp_a:region_b": 1.8, "isp_b:region_a": 0.1},
}

# timestamp of last update (for potential dynamic updates)
_cost_map_updated_at = time.time()

# Lock to protect shared structures if you plan to update them dynamically
lock = threading.Lock()


@app.route("/register_peer", methods=["POST"])
def register_peer():
    """
    Body: { "peer_id": "...", "isp": "...", "region": "..." }
    """
    data = request.get_json(force=True)
    isp = data.get("isp", "isp_unknown")
    region = data.get("region", "region_unknown")
    peer_id = data.get("peer_id")
    if not peer_id:
        return jsonify({"error": "peer_id missing"}), 400

    with lock:
        peer_map.setdefault(isp, {}).setdefault(region, []).append(peer_id)
        peer_location[peer_id] = f"{isp}:{region}"

    return jsonify({"status": "registered", "peer": peer_id, "location": peer_location[peer_id]})


@app.route("/get_peers", methods=["GET"])
def get_peers():
    """
    Legacy endpoint used by your existing P4P logic.
    Query params: isp, region
    """
    isp = request.args.get("isp")
    region = request.args.get("region")

    result = {"preferred_peers": [], "same_isp": [], "all_known": []}

    with lock:
        if isp in peer_map:
            result["preferred_peers"] = peer_map[isp].get(region, []).copy()
            for reg, peers in peer_map[isp].items():
                if reg != region:
                    result["same_isp"].extend(peers)

        for isp_k, regs in peer_map.items():
            for reg_k, peers in regs.items():
                result["all_known"].extend(peers)

    return jsonify(result)


# -----------------------
# ALTO-like endpoints
# -----------------------
@app.route("/alto/network_map", methods=["GET"])
def alto_network_map():
    """Return mapping peer_id -> location (isp:region)"""
    with lock:
        return jsonify({"network_map": peer_location})


@app.route("/alto/cost_map", methods=["GET"])
def alto_cost_map():
    """Return the cost map (location -> location -> cost)"""
    with lock:
        return jsonify({"cost_map": cost_map, "units": "score", "updated_at": _cost_map_updated_at})


@app.route("/alto/endpoint_cost", methods=["POST"])
def alto_endpoint_cost():
    """
    Body: { "src": "peerX:5000", "dsts": ["peerA:5000","peerB:5000", ...] }
    Returns: { "costs": { "peerA:5000": cost, ... } }
    """
    data = request.get_json(force=True)
    src = data.get("src")
    dsts = data.get("dsts", [])
    if not src:
        return jsonify({"error": "src missing"}), 400

    with lock:
        src_loc = peer_location.get(src)
        if not src_loc:
            return jsonify({"error": "src_unknown"}), 400

        results = {}
        for d in dsts:
            dloc = peer_location.get(d)
            if not dloc:
                results[d] = None
            else:
                # lookup cost_map, fallback to inf
                results[d] = cost_map.get(src_loc, {}).get(dloc, float("inf"))

    return jsonify({"costs": results})


@app.route("/metrics")
def metrics():
    return jsonify({
        # total peers registered across all ISPs and regions
        "peers": sum(len(peers) for regs in peer_map.values() for peers in regs.values()),
        # simple synthetic metric to simulate dynamic network condition
        "cost_map_avg": round(random.uniform(5, 50), 2),
        # number of regions known (sum of region buckets per ISP)
        "regions": sum(len(v) for v in peer_map.values()),
        "timestamp": datetime.utcnow().isoformat()
    })

# optional administrative endpoints to update cost_map dynamically
@app.route("/alto/admin/set_cost", methods=["POST"])
def alto_set_cost():
    """
    Body: { "from": "isp_a:region_a", "to": "isp_b:region_a", "cost": 1.5 }
    """
    data = request.get_json(force=True)
    fr = data.get("from")
    to = data.get("to")
    cost = data.get("cost")
    if fr is None or to is None or cost is None:
        return jsonify({"error": "missing fields"}), 400

    with lock:
        cost_map.setdefault(fr, {})[to] = float(cost)
        global _cost_map_updated_at
        _cost_map_updated_at = time.time()

    return jsonify({"status": "ok", "from": fr, "to": to, "cost": cost})


if __name__ == "__main__":
    # Simple run
    app.run(host="0.0.0.0", port=6000)
