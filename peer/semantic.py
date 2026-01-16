#!/usr/bin/env python3
import os
import hashlib
import requests
import json
import concurrent.futures
from naive import NaivePeer

class SemanticPeer(NaivePeer):
    """
    Implementazione SEMANTIC PARTITIONING (Document Partitioning).
    
    Concetto:
    - Data Locality: Chunk, Manifest e Indici dello stesso file risiedono 
      TUTTI sullo stesso nodo (o i suoi successori per replica).
    - Partition Key: Si usa 'genre' per decidere il nodo responsabile.
    """

    def upload_file(self, filepath, metadata=None):
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
        
        print(f"[SemanticPeer] 📦 Placement: '{partition_key}' -> {primary_node}")

        # 2. Split del file
        chunks = self.storage.split_file(filepath)
        chunks_info = []

        # 3. Distribuzione Chunk (TUTTI VERSO LO STESSO NODO)
        # Nota: Qui sacrifichiamo il load balancing dello storage per la velocità di accesso.
        for idx, ch_hash, data in chunks:
            chunks_info.append({"hash": ch_hash, "peers": [primary_node]})
            
            # Invio fisico
            if primary_node == self.self_id:
                self.storage.save_chunk(ch_hash, data)
            else:
                self._send_chunk(primary_node, ch_hash, data)

        # 4. Creazione Manifest
        manifest = {
            "filename": os.path.basename(filepath),
            "chunks": chunks_info,
            "metadata": metadata,
            "placement_key": partition_key
        }

        # 5. Invio Manifest (Allo stesso nodo dei chunk)
        if primary_node == self.self_id:
            self.storage.save_manifest(manifest)
            # Indicizzazione Locale (Document Index)
            self._local_index(manifest)
        else:
            self._send_manifest(primary_node, manifest)
            # Chiediamo al nodo remoto di indicizzarlo localmente
            self._remote_index_request(primary_node, manifest)

        return {"status": "stored", "manifest": manifest, "strategy": "semantic_locality"}

    def search(self, query):
        """
        Search ottimizzata per Partition Key.
        """
        print(f"[SemanticPeer] 🔎 Search Query: {query}")
        
        # CASO 1: La query contiene la Partition Key (es. Genre)
        # Possiamo andare a colpo sicuro (Routing Diretto O(1))
        if "genre" in query:
            partition_key = query["genre"].lower().strip()
            target_hash = hashlib.sha1(partition_key.encode()).hexdigest()
            target_node = self.ring.get_node(target_hash)
            
            print(f"   -> Routing diretto verso nodo {target_node} (Key: {partition_key})")
            return self._query_node(target_node, query)

        # CASO 2: Query senza Partition Key (es. solo Actor)
        # Dobbiamo fare Broadcast / Scatter-Gather su TUTTI i nodi.
        # È costoso in rete, ma necessario in questa architettura.
        print(f"   -> Broadcast Search (Manca Partition Key)")
        results = []
        
        # Parallelizziamo le richieste
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(self._query_node, p, query): p for p in self.known_peers}
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    res = future.result()
                    results.extend(res)
                except Exception:
                    pass
        
        # Aggiungi anche i risultati locali (se stesso)
        local_res = self._search_local_storage(query) # Metodo ereditato da NaivePeer
        results.extend(local_res)
        
        return results

    # ==========================
    # Metodi Helper
    # ==========================

    def _local_index(self, manifest):
        """
        Salva un indice locale. In questo modello, ogni nodo ha il SUO indice
        dei file che ospita. Non c'è indice globale.
        """
        # In realtà, nel semantic partitioning, l'indice è implicito nei manifest locali.
        # Il metodo _search_local_storage di NaivePeer fa già iterazione sui manifest locali.
        # Per ottimizzare, potremmo creare un inverted index LOCALE su file,
        # ma per ora riusare _search_local_storage va benissimo.
        pass

    def _remote_index_request(self, target, manifest):
        """
        In questo modello non serve inviare indici separati, 
        perché il manifest risiede già sul target.
        """
        pass

    def _query_node(self, node, query):
        """Interroga un nodo remoto (usa l'API search_local esistente)"""
        if node == self.self_id:
            return self._search_local_storage(query)
        
        try:
            # Usiamo l'endpoint di ricerca locale che guarda solo nel disco del nodo
            r = requests.get(f"http://{node}/search_local", params=query, timeout=2)
            if r.status_code == 200:
                return r.json().get("results", [])
        except Exception:
            pass
        return []

    def _get_placement_key(self, manifest):
        """
        Override per Semantic Partitioning.
        La chiave di posizionamento è data dal 'placement_key' salvato nel manifest
        (che corrisponde a genre o titolo).
        """
        # Se non c'è placement_key, fallback su filename (comportamento safe)
        return manifest.get("placement_key", manifest["filename"])