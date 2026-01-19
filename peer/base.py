#!/usr/bin/env python3
import os
import json
import hashlib
import threading
import time
import requests
from storage import Storage
from hashing import ConsistentHashRing

class BasePeer:
    def __init__(self, self_id, known_peers, data_dir, config=None):
        """
        Inizializza le strutture dati comuni a tutti i tipi di Peer.
        """
        self.self_id = self_id
        self.known_peers = known_peers # Lista di indirizzi IP:PORT
        self.storage = Storage(data_dir)
        
        # Inizializza l'anello DHT con i peer conosciuti + se stesso
        self.ring = ConsistentHashRing(known_peers + [self_id])

        # Configurazione parametri (con default)
        config = config or {}
        self.heartbeat_interval = config.get('heartbeat_interval', 5)
        self.failure_timeout = config.get('failure_timeout', 15)
        self.ring_refresh_interval = config.get('ring_refresh_interval', 10)

        # Stato per failure detection e sincronizzazione
        self.lock = threading.Lock()
        self.last_seen = {p: time.time() for p in self.known_peers}
        self.bootstrap_peers = list(known_peers)  # Snapshot iniziale per rejoin

    # ==========================================
    # METODI ASTRATTI (Da implementare nei figli)
    # ==========================================

    def upload_file(self, filepath, metadata=None, simulate_content=False):
        """
        Ogni strategia (Naive, Metadata, P4P) gestisce l'upload in modo diverso.
        - Naive: Solo chunks + manifest.
        - Metadata: Chunks + manifest + aggiornamento indici.
        """
        raise NotImplementedError("Devi implementare upload_file nella sottoclasse")

    def search(self, query):
        """
        Ogni strategia gestisce la ricerca in modo diverso.
        - Naive: Flooding.
        - Metadata: DHT Lookup su indici invertiti.
        """
        raise NotImplementedError("Devi implementare search nella sottoclasse")

    # ==========================================
    # LOGICA COMUNE: Download (File Layer)
    # ==========================================

    def download_file(self, filename):
        """
        Il download segue la logica DHT standard per recuperare il manifest e i chunk.
        Questa logica è condivisa perché il 'Data Plane' (dove stanno i file) 
        non cambia drasticamente tra le versioni.
        """
        print(f"[Peer:{self.self_id}] Avvio download_file per '{filename}'")
        
        # 1. Trova chi ospita il manifest (Consistent Hashing sul filename)
        manifest_hash = hashlib.sha1(filename.encode()).hexdigest()
        manifest_peer = self.ring.get_node(manifest_hash)
        
        # 2. Recupera il manifest
        manifest = self._fetch_manifest(filename, manifest_peer)
        if not manifest:
            return {"error": "manifest_not_found", "status": "failed"}

        # 3. Scarica i chunks elencati nel manifest
        fetched_chunks, failed_chunks = self._fetch_chunks(manifest)

        # 4. Ricostruisci il file
        return self._rebuild_file(filename, manifest, fetched_chunks, failed_chunks)

    def _fetch_manifest(self, filename, manifest_peer):
        """Helper per recuperare il JSON del manifest (locale o remoto)"""
        if manifest_peer == self.self_id:
            return self.storage.load_manifest(filename)
        
        try:
            url = f"http://{manifest_peer}/get_manifest/{filename}"
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            print(f"[Peer:{self.self_id}] Errore recupero manifest da {manifest_peer}: {e}")
        return None

    def _fetch_chunks(self, manifest):
        """Cicla sui chunk del manifest e prova a scaricarli"""
        fetched = []
        failed = []
        
        for chunk_info in manifest["chunks"]:
            ch_hash = chunk_info["hash"]
            peers = chunk_info.get("peers", [])
            
            # Se ce l'ho già, salto
            if self.storage.load_chunk(ch_hash):
                fetched.append(ch_hash)
                continue

            got_it = False
            for p in peers:
                try:
                    r = requests.get(f"http://{p}/get_chunk/{ch_hash}", timeout=5)
                    if r.status_code == 200:
                        self.storage.save_chunk(ch_hash, r.content)
                        # Aggiorno il manifest per dire "ce l'ho anche io ora"
                        self._notify_chunk_possession(manifest["filename"], ch_hash, manifest)
                        fetched.append(ch_hash)
                        got_it = True
                        break
                except Exception:
                    continue
            
            if not got_it:
                failed.append(ch_hash)
        
        return fetched, failed

    def _notify_chunk_possession(self, filename, ch_hash, manifest_data):
        """Opzionale: dice al proprietario del manifest che ora ho anch'io il chunk"""
        # Nota: Qui si potrebbe ottimizzare calcolando l'hash del manifest
        pass 

    def _rebuild_file(self, filename, manifest, fetched, failed):
        """Assembla i pezzi"""
        if failed:
            return {"status": "partial", "missing": failed}
            
        output_path = os.path.join(self.storage.data_dir, f"rebuilt_{filename}")
        try:
            final_path = self.storage.rebuild_file(manifest, output_path)
            return {"status": "fetched", "path": final_path}
        except Exception as e:
            return {"status": "failed_rebuild", "error": str(e)}

    # ==========================================
    # LOGICA COMUNE: Gestione Rete e Background
    # ==========================================

    def start_background_tasks(self):
        """Avvia tutti i thread di manutenzione"""
        tasks = [
            self.attempt_rejoin,
            self.failure_detector,
            self.gossip_known_peers_loop,
            self.ring_refresh
        ]
        for task in tasks:
            t = threading.Thread(target=task, daemon=True)
            t.start()

    def attempt_rejoin(self, retries=6, wait=5):
        """Prova a riconnettersi alla rete all'avvio"""
        print(f"[Peer:{self.self_id}] Tentativo di join alla rete...")
        for attempt in range(1, retries + 1):
            for b in self.bootstrap_peers:
                if b == self.self_id: continue
                try:
                    r = requests.post(f"http://{b}/join", json={"peer_id": self.self_id}, timeout=3)
                    if r.status_code == 200:
                        data = r.json()
                        self._merge_peers(data.get("known_peers", []))
                        print(f"[Peer:{self.self_id}] JOIN riuscito tramite {b}")
                        return
                except Exception:
                    pass
            time.sleep(wait)
        print(f"[Peer:{self.self_id}] Impossibile contattare bootstrap peers. Opero in isolamento o come primo nodo.")

    def failure_detector(self):
        """Ping periodico per rimuovere nodi morti"""
        while True:
            time.sleep(self.heartbeat_interval)
            with self.lock:
                snapshot = list(self.known_peers)
            
            for p in snapshot:
                if p == self.self_id: continue
                if not self.ping_peer(p):
                    # Logica semplificata: se fallisce il ping, controlla timeout
                    last = self.last_seen.get(p, 0)
                    if time.time() - last > self.failure_timeout:
                        self._remove_peer(p)

    def ping_peer(self, peer_addr):
        try:
            requests.get(f"http://{peer_addr}/ping", timeout=2)
            self.last_seen[peer_addr] = time.time()
            return True
        except:
            return False

    def gossip_known_peers_loop(self):
        """Diffonde la conoscenza dei peer"""
        while True:
            time.sleep(self.ring_refresh_interval)
            with self.lock:
                my_list = list(self.known_peers)
            
            # Manda a un sottoinsieme casuale o a tutti (qui tutti per semplicità)
            for p in my_list:
                if p == self.self_id: continue
                try:
                    requests.post(f"http://{p}/update_peers", json={"peers": my_list}, timeout=2)
                except:
                    pass

    def ring_refresh(self):
        """Sincronizza periodicamente l'anello"""
        # Implementazione base: la gossip loop già fa gran parte del lavoro aggiornando known_peers
        # Qui potresti forzare una sincronizzazione più aggressiva se necessario
        pass

    def _merge_peers(self, new_peers):
        """Helper thread-safe per aggiungere nuovi peer"""
        with self.lock:
            changed = False
            for p in new_peers:
                if p not in self.known_peers and p != self.self_id:
                    self.known_peers.append(p)
                    self.ring.add_node(p)
                    self.last_seen[p] = time.time()
                    changed = True
            if changed:
                print(f"[Peer:{self.self_id}] Lista peer aggiornata: {len(self.known_peers)} nodi")

    def _remove_peer(self, peer_id):
        """Helper thread-safe per rimuovere peer"""
        print(f"[Peer:{self.self_id}] Rilevato nodo DOWN: {peer_id}")
        with self.lock:
            if peer_id in self.known_peers:
                self.known_peers.remove(peer_id)
                self.ring.remove_node(peer_id)
                # Avvisa gli altri (opzionale, ma buona pratica)

    # ==========================================
    # LOGICA COMUNE: Graceful Shutdown
    # ==========================================

    def graceful_shutdown(self):
        """
        Trasferisce le responsabilità (manifest) prima di chiudere.
        """
        print(f"🚪 [Peer:{self.self_id}] Inizio graceful shutdown...")
        
        # 1. Rimuovi se stesso dall'anello locale per calcolare i nuovi responsabili
        temp_peers = [p for p in self.known_peers if p != self.self_id]
        if not temp_peers:
            return {"status": "isolated", "msg": "Nessun peer a cui cedere i dati"}
        
        temp_ring = ConsistentHashRing(temp_peers)
        
        # 2. Ridistribuzione Manifest Locali
        local_manifests = self.storage.list_local_manifests()
        moved_count = 0
        
        for m in local_manifests:
            fname = m["filename"]
            h_name = hashlib.sha1(fname.encode()).hexdigest()
            target = temp_ring.get_node(h_name)
            
            try:
                requests.post(f"http://{target}/store_manifest", json=m, timeout=3)
                self.storage.remove_local_manifest(fname)
                moved_count += 1
            except Exception as e:
                print(f"Errore spostamento manifest {fname} a {target}: {e}")

        # 3. Notifica Leave
        for p in temp_peers:
            try:
                requests.post(f"http://{p}/announce_leave", json={"peer_id": self.self_id}, timeout=1)
            except:
                pass

        return {"status": "completed", "manifests_moved": moved_count}