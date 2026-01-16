import os
import time
import random
import hashlib
import requests
import threading
from peer import Peer, app  # assume peer.py exposes Flask "app" and Peer class
from functools import lru_cache

# Configuration tunables
ALTO_TTL = 30            # seconds - cache TTL for ALTO responses
ALTO_TOP_K = 6           # consider only top-K cheapest peers by ALTO cost before RTT probing
ALPHA = 0.7              # weight for ALTO cost in final score
BETA = 0.3               # weight for RTT in final score
RTT_TIMEOUT = 0.4        # seconds for RTT probe
RTT_PENALTY = 9999.0     # large RTT when probe fails

class PeerP4P(Peer):
    def __init__(self, self_id, known_peers, data_dir, isp, region, itracker_url):
        super().__init__(self_id, known_peers, data_dir)
        self.isp = isp
        self.region = region
        self.itracker_url = itracker_url.rstrip("/")
        self._alto_cache = {"cost_map": None, "network_map": None, "endpoint_costs": {}, "ts": 0}
        self._alto_lock = threading.Lock()
        self.register_to_itracker()

    # -------------------------
    # Registration + helpers
    # -------------------------
    def register_to_itracker(self):
        payload = {"peer_id": self.self_id, "isp": self.isp, "region": self.region}
        try:
            r = requests.post(f"{self.itracker_url}/register_peer", json=payload, timeout=3)
            if r.status_code == 200:
                print(f"✅ Peer {self.self_id} registrato all’iTracker ({self.isp}, {self.region})")
            else:
                print(f"⚠️ iTracker ha risposto con {r.status_code}: {r.text}")
        except Exception as e:
            print(f"⚠️ Errore nella registrazione all’iTracker: {e}")

    # -------------------------
    # ALTO caching utilities
    # -------------------------
    def _alto_need_refresh(self):
        return time.time() - self._alto_cache.get("ts", 0) > ALTO_TTL

    def get_cost_map(self):
        with self._alto_lock:
            if not self._alto_cache["cost_map"] or self._alto_need_refresh():
                try:
                    r = requests.get(f"{self.itracker_url}/alto/cost_map", timeout=2)
                    if r.status_code == 200:
                        data = r.json()
                        self._alto_cache["cost_map"] = data.get("cost_map", {})
                        self._alto_cache["ts"] = time.time()
                except Exception:
                    # leave old cache if exists
                    pass
            return self._alto_cache.get("cost_map", {})

    def get_network_map(self):
        with self._alto_lock:
            if not self._alto_cache["network_map"] or self._alto_need_refresh():
                try:
                    r = requests.get(f"{self.itracker_url}/alto/network_map", timeout=2)
                    if r.status_code == 200:
                        self._alto_cache["network_map"] = r.json().get("network_map", {})
                        self._alto_cache["ts"] = time.time()
                except Exception:
                    pass
            return self._alto_cache.get("network_map", {})

    def get_endpoint_costs(self, dsts):
        """
        Ask tracer for endpoint costs for self -> dsts. Cache per-dst for ALTO_TTL.
        Returns dict {dst: cost_or_None}
        """
        key = ",".join(sorted(dsts))
        with self._alto_lock:
            cached = self._alto_cache["endpoint_costs"].get(key)
            if cached and time.time() - cached["ts"] <= ALTO_TTL:
                return cached["costs"]
        # request
        try:
            r = requests.post(f"{self.itracker_url}/alto/endpoint_cost", json={"src": self.self_id, "dsts": dsts}, timeout=2)
            if r.status_code == 200:
                costs = r.json().get("costs", {})
            else:
                costs = {d: None for d in dsts}
        except Exception:
            costs = {d: None for d in dsts}
        with self._alto_lock:
            self._alto_cache["endpoint_costs"][key] = {"ts": time.time(), "costs": costs}
        return costs

    # -------------------------
    # RTT probe (simple TCP connect timing or HTTP ping)
    # -------------------------
    def measure_rtt_ms(self, peer_addr):
        """
        Measure a quick RTT to peer by requesting /ping with short timeout.
        peer_addr = "peer1:5000" or "hostname:port"
        Returns RTT in ms or RTT_PENALTY on failure.
        """
        try:
            url = f"http://{peer_addr}/ping"
            start = time.time()
            r = requests.get(url, timeout=RTT_TIMEOUT)
            if r.status_code == 200:
                return (time.time() - start) * 1000.0
            else:
                return RTT_PENALTY
        except Exception:
            return RTT_PENALTY

    # -------------------------
    # ALTO-based selection
    # -------------------------
    def select_peer_with_alto(self, available_peers, alpha=ALPHA, beta=BETA, top_k=ALTO_TOP_K):
        """
        For a given list of available_peers, choose best peer combining ALTO cost + RTT.
        Steps:
        1. ask iTracker endpoint_cost -> costs
        2. sort peers by cost ascending, keep top_k
        3. probe RTT for the top_k candidates (short timeout)
        4. normalize cost and rtt into [0,1] and compute score = alpha*cost_norm + beta*rtt_norm
        5. return peer with minimal score
        """
        if not available_peers:
            return None
        # 1) endpoint costs
        costs = self.get_endpoint_costs(available_peers)  # {peer: cost or None}
        # fill missing with large number
        filled = []
        for p in available_peers:
            c = costs.get(p)
            if c is None:
                c_val = float("inf")
            else:
                try:
                    c_val = float(c)
                except Exception:
                    c_val = float("inf")
            filled.append((p, c_val))

        # 2) sort by cost and take top_k candidates
        filled_sorted = sorted(filled, key=lambda x: x[1])
        candidates = [p for p, _ in filled_sorted[:top_k]]

        # 3) RTT probing (parallelize could be added; here sequential with short timeout)
        rtts = {}
        for p in candidates:
            rtts[p] = self.measure_rtt_ms(p)

        # 4) normalization
        # cost normalization among candidates (finite only)
        cost_values = [c for p, c in filled_sorted[:top_k] if c != float("inf")]
        if cost_values:
            cmin, cmax = min(cost_values), max(cost_values)
        else:
            cmin, cmax = 0.0, 1.0

        rvals = list(rtts.values())
        rmin, rmax = (min(rvals), max(rvals)) if rvals else (0.0, 1.0)

        scores = {}
        for p in candidates:
            # cost normalized
            raw_cost = dict(filled).get(p, float("inf"))
            if raw_cost == float("inf"):
                cn = 1.0  # worst
            else:
                cn = 0.0 if cmax == cmin else (raw_cost - cmin) / (cmax - cmin)
                cn = max(0.0, min(1.0, cn))
            # rtt normalized
            raw_rtt = rtts.get(p, RTT_PENALTY)
            rn = 0.0 if rmax == rmin else (raw_rtt - rmin) / (rmax - rmin)
            rn = max(0.0, min(1.0, rn))
            score = alpha * cn + beta * rn
            scores[p] = score

        # pick best
        best = min(scores.items(), key=lambda kv: kv[1])[0]
        # debug print
        print(f"[ALTO] candidates={candidates} costs={ {p: costs.get(p) for p in candidates} } rtts={rtts} scores={scores} selected={best}")
        return best

    # -------------------------
    # override select_peer_for_chunk to use ALTO
    # -------------------------
    def select_peer_for_chunk(self, available_peers):
        # first try ALTO-based selection
        try:
            chosen = self.select_peer_with_alto(available_peers)
            if chosen:
                return chosen
        except Exception as e:
            print(f"[ALTO] Error during ALTO selection: {e}")
        # fallback to legacy tiered selection via get_peers
        try:
            preferred, same_isp, all_known = self.get_peer_tiers()
            local = [p for p in available_peers if p in preferred]
            if local:
                return random.choice(local)
            mid = [p for p in available_peers if p in same_isp]
            if mid:
                return random.choice(mid)
            return random.choice(available_peers)
        except Exception:
            return random.choice(available_peers)

    # -------------------------
    # upload_file and download_file remain mostly unchanged,
    # but call select_peer_for_chunk for selection (already done in your code)
    # -------------------------
    # (You can keep your existing upload_file/download_file implementations that
    #  call select_peer_for_chunk. No change needed here.)

# If this module is run directly, create the PeerP4P and start Flask app from peer.py
if __name__ == "__main__":
    # environment
    port = int(os.environ.get("PORT", 5000))
    SELF_ID = os.environ.get("SELF_ID")
    KNOWN_PEERS = os.environ.get("KNOWN_PEERS", "").split(",") if os.environ.get("KNOWN_PEERS") else []
    DATA_DIR = os.environ.get("DATA_DIR", "/app/data")
    ISP = os.environ.get("ISP", "isp_default")
    REGION = os.environ.get("REGION", "region_default")
    ITRACKER_URL = os.environ.get("ITRACKER_URL", "http://itracker:6000")

    peer = PeerP4P(self_id=SELF_ID, known_peers=KNOWN_PEERS, data_dir=DATA_DIR,
                   isp=ISP, region=REGION, itracker_url=ITRACKER_URL)

    print(f"Peer {SELF_ID} P4P in ascolto su {port} con KNOWN_PEERS={KNOWN_PEERS} ISP={ISP}/{REGION}")
    # start Flask app defined in peer.py (endpoints /store_chunk, /get_chunk, /fetch_file, ecc.)
    app.run(host="0.0.0.0", port=port)
