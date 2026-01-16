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
    Implementazione NAIVE del protocollo P2P.
    
    Caratteristiche:
    - Upload: Distribuisce chunk tramite Consistent Hashing (DHT).
    - Search: FLOODING. Per trovare un file, interroga tutti i peer conosciuti.
      Non usa indici distribuiti.
    """

    def upload_file(self, filepath, metadata=None):
        """
        Carica un file nella rete:
        1. Split del file in chunk.
        2. Distribuzione dei chunk ai nodi responsabili (DHT).
        3. Creazione e distribuzione del Manifest (replicato su k nodi).
        """
        if not os.path.exists(filepath):
            return {"error": "file inesistente", "status": "failed"}

        # 1. Preparazione Chunk
        chunks = self.storage.split_file(filepath)
        peers_map = {}
        chunks_info = []

        # 2. Distribuzione Chunk
        for idx, ch_hash, data in chunks:
            responsible_node = self.ring.get_node(ch_hash)
            peers_map[ch_hash] = responsible_node
            
            # Info per il manifest
            chunks_info.append({"hash": ch_hash, "peers": [responsible_node]})

            # Invio fisico del dato
            if responsible_node == self.self_id:
                self.storage.save_chunk(ch_hash, data)
            else:
                self._send_chunk(responsible_node, ch_hash, data)
        
        manifest = {
            "filename": os.path.basename(filepath),
            "chunks": chunks_info,
            "metadata": metadata or {},
            "size": os.path.getsize(filepath),
            "updated_at": time.time()  # <--- LWW: Timestamp (UTC float)
        }



        # 4. Distribuzione Manifest (Replication Factor = 3)
        # Hash del filename per decidere dove mettere il manifest
        manifest_hash = hashlib.sha1(manifest["filename"].encode()).hexdigest()
        responsible_peers = self.ring.get_successors(manifest_hash, count=3)

        for peer_target in responsible_peers:
            if peer_target == self.self_id:
                self.storage.save_manifest(manifest)
                print(f"[Peer:{self.self_id}] Manifest salvato localmente")
            else:
                self._send_manifest(peer_target, manifest)

        return {
            "status": "stored",
            "manifest": manifest,
            "replicas": responsible_peers
        }

    def search(self, query):
        """
        Ricerca NAIVE (Flooding / Scatter-Gather).
        
        1. Cerca nei manifest salvati localmente.
        2. Invia una richiesta HTTP a TUTTI i peer conosciuti.
        3. Aggrega i risultati.
        
        Complexity: O(N) dove N è il numero di peer conosciuti.
        """
        results = []
        seen_keys = set() # Per evitare duplicati se più peer hanno lo stesso file

        # --- STEP 1: Ricerca Locale ---
        local_results = self._search_local_storage(query)
        for res in local_results:
            key = f"{res['filename']}_{self.self_id}"
            if key not in seen_keys:
                results.append(res)
                seen_keys.add(key)

        # --- STEP 2: Ricerca Remota (Flooding) ---
        # Nota: In una rete enorme questo è inefficiente.
        # Qui interroghiamo solo i vicini (1-hop flooding).
        is_partial = False
        
        for peer_addr in self.known_peers:
            if peer_addr == self.self_id:
                continue
            
            try:
                # Chiamiamo l'endpoint specifico per la ricerca locale del vicino
                # (Vedi api.py: /search_local)
                url = f"http://{peer_addr}/search_local"
                r = requests.get(url, params=query, timeout=2) # Timeout basso per non bloccare
                
                if r.status_code == 200:
                    remote_data = r.json().get("results", [])
                    for item in remote_data:
                        # Aggiungiamo l'host se manca, per sapere chi contattare
                        if "host" not in item:
                            item["host"] = peer_addr
                        
                        # Deduplica
                        key = f"{item['filename']}_{item['host']}"
                        if key not in seen_keys:
                            results.append(item)
                            seen_keys.add(key)
            except Exception as e:
                # Se un peer è giù durante la ricerca, lo annotiamo ma continuiamo (Partial Result)
                # print(f"Peer {peer_addr} non risponde alla search: {e}")
                is_partial = True

        # --- STEP 3: Conflict Resolution (LWW) ---
        # Deduplica per filename, tenendo quello con timestamp più recente
        final_results = self._resolve_conflicts(results)
        
        return {
            "results": final_results,
            "partial_result": is_partial
        }

    def _resolve_conflicts(self, raw_results):
        """
        Gestisce conflitti Read Repair implementando Last Write Wins (LWW).
        1. Identifica la versione vincente (timestamp più alto).
        2. Restituisce solo la vincente.
        3. (Read Repair) Aggiorna in background i nodi con versioni obsolete.
        """
        grouped = {}
        
        # Raggruppa per filename
        for item in raw_results:
            fname = item["filename"]
            if fname not in grouped:
                grouped[fname] = []
            grouped[fname].append(item)
            
        final_list = []
        
        for fname, versions in grouped.items():
            # Trova la versione con timestamp maggiore
            winner = max(versions, key=lambda x: x.get("updated_at", 0))
            final_list.append(winner)
            
            # Read Repair: Se ci sono versioni perdenti, aggiornale
            winner_ts = winner.get("updated_at", 0)
            winner_manifest = winner.get("manifest")
            
            if not winner_manifest: continue # Non possiamo riparare senza manifest
            
            for v in versions:
                v_ts = v.get("updated_at", 0)
                if v_ts < winner_ts:
                    loser_host = v.get("host")
                    print(f"[ReadRepair] 🛠️ Found stale version on {loser_host} (ts={v_ts} < {winner_ts}). Repairing...")
                    # Lancia repair in thread separato per non bloccare la search
                    threading.Thread(target=self._send_manifest, args=(loser_host, winner_manifest)).start()

        return final_list

    def start_background_tasks(self):
        """Override per aggiungere l'Anti-Entropy ai task base"""
        super().start_background_tasks()
        
        # Avvia il thread di riparazione
        t = threading.Thread(target=self.anti_entropy_loop, daemon=True)
        t.start()
        print(f"[Peer:{self.self_id}] Anti-Entropy Protocol Started")
    
    def anti_entropy_loop(self):
        """
        Ciclo infinito che verifica la salute delle repliche.
        Gira ogni 20-40 secondi (randomizzato per evitare sincronizzazioni globali).
        """
        while True:
            sleep_time = random.randint(20, 40)
            time.sleep(sleep_time)
            
            try:
                self._repair_manifests()
            except Exception as e:
                print(f"[Anti-Entropy] ⚠️ Error in loop: {e}")

    # --- Helper Methods ---

    def _send_chunk(self, target, ch_hash, data):
        """Helper per inviare un chunk via HTTP"""
        try:
            url = f"http://{target}/store_chunk"
            requests.post(url, files={"chunk": data}, timeout=5)
        except Exception as e:
            print(f"Errore invio chunk {ch_hash} a {target}: {e}")

    def _send_manifest(self, target, manifest):
        """Helper per inviare un manifest via HTTP"""
        try:
            url = f"http://{target}/store_manifest"
            requests.post(url, json=manifest, timeout=3)
            print(f"[Peer:{self.self_id}] Manifest replicato su {target}")
        except Exception as e:
            print(f"Errore invio manifest a {target}: {e}")

    def _search_local_storage(self, query):
        """
        Cerca tra i manifest presenti sul disco di questo nodo.
        Usato sia internamente da search(), sia dall'API /search_local.
        """
        matches = []
        local_manifests = self.storage.list_local_manifests()
        
        for m in local_manifests:
            metadata = m.get("metadata", {})
            # Match: tutti i campi della query devono essere presenti e uguali (case-insensitive)
            is_match = True
            for k, v in query.items():
                meta_val = str(metadata.get(k, ""))
                if meta_val.lower() != str(v).lower():
                    is_match = False
                    break
            
            if is_match:
                matches.append({
                    "filename": m["filename"],
                    "metadata": metadata,
                    "host": self.self_id,
                    "updated_at": m.get("updated_at", 0),  # <--- Critical for LWW
                    "manifest": m  # Include full manifest for Read Repair
                })
        return matches

    def _repair_manifests(self):
        """
        Scansiona i file locali. Se sono io il responsabile (Primary),
        mi assicuro che i miei Neighbor (Successors) abbiano una copia.
        """
        local_manifests = self.storage.list_local_manifests()
        if not local_manifests:
            return

        print(f"[Anti-Entropy] 🔍 Checking replication for {len(local_manifests)} files...")

        for manifest in local_manifests:
            filename = manifest["filename"]
            # 1. HASH DI STORAGE (Come è salvato su disco)
            manifest_hash = hashlib.sha1(filename.encode()).hexdigest()
            
            # 2. HASH DI ROUTING (Dove deve andare)
            placement_key = self._get_placement_key(manifest)
            placement_hash = hashlib.sha1(placement_key.encode()).hexdigest()
            
            # Controllo responsabilità usando l'hash di ROUTING
            primary_node = self.ring.get_node(placement_hash)
            
            if primary_node != self.self_id:
                continue

            # Trovo i vicini usando l'hash di ROUTING
            replicas = self.ring.get_successors(placement_hash, count=2)
            
            for replica in replicas:
                if replica == self.self_id: continue 
                
                # CORREZIONE: Controllo esistenza usando l'hash di STORAGE!
                self._ensure_replica_has_file(replica, manifest, manifest_hash)

    def _get_placement_key(self, manifest):
        """
        Determina la chiave usata per posizionare il file nel Ring.
        - Naive/Metadata: usa il filename.
        - Semantic: usa la chiave semantica (es. genre).
        """
        return manifest["filename"]

    def _ensure_replica_has_file(self, target_peer, manifest, manifest_hash):
        """Chiede al peer se ha il file. Se no, glielo manda."""
        try:
            # Chiediamo solo del manifest per ora (controllo leggero)
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
                    print(f"[Anti-Entropy] 🚑 REPAIR: Sending '{manifest['filename']}' to {target_peer}")
                    # Inviamo il Manifest
                    self._send_manifest(target_peer, manifest)
                    
                    # (Opzionale) Potremmo inviare anche i chunk se mancano,
                    # ma per ora ripariamo i metadati.
                    # Per riparare i chunk, servirebbe scorrere manifest['chunks']
                    # e inviare quelli di cui questo nodo è responsabile.
            
        except Exception as e:
            # Se il peer è giù, il failure detector base lo rimuoverà.
            # L'anti-entropy al prossimo giro sceglierà un nuovo successore.
            pass