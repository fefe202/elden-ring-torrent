#!/usr/bin/env python3
import os
import hashlib
import requests
import json
import random
import concurrent.futures
from naive import NaivePeer

class MetadataPeer(NaivePeer):
    """
    Implementazione GSI (Global Secondary Index) con Salting.
    
    - Upload: Distribuisce i metadati su N shard casuali (Load Balancing Scrittura).
    - Search: Interroga tutti gli N shard in parallelo e unisce i risultati (Scatter-Gather).
    """
    
    # Numero di shard per ogni chiave di indice.
    # Esempio: "actor:brad pitt" viene diviso in ...:0, ...:1, ...:2
    INDEX_SHARDS = 3 

    def upload_file(self, filepath, metadata=None, simulate_content=False):
        # 1. Upload Chunk (Storage Fisico) - Usa logica Naive (Hash del contenuto)
        #    Questo garantisce che i dati pesanti siano perfettamente bilanciati.
        result = super().upload_file(filepath, metadata, simulate_content=simulate_content)

        if result.get("status") != "stored":
            return result

        # 2. Aggiornamento Indici (GSI)
        self._gsi_write(result["manifest"])
        
        return result

    def search(self, query):
        """
        Esegue la ricerca usando Scatter-Gather sugli indici shardati.
        """
        print(f"[MetadataPeer] 🔎 Search Query: {query}")
        if not query:
            return []

        # Lista di set di risultati (uno per ogni attributo della query)
        # Es: Risultati(Actor=Brad) AND Risultati(Genre=Action)
        candidates_per_attribute = []

        is_partial = False
        
        # ThreadPool per parallelizzare le richieste agli shard
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            
            # --- FASE 1: Scatter (Raccolta dati per ogni attributo) ---
            for attr_key, attr_val in query.items():
                base_key = f"{attr_key}:{str(attr_val).lower().strip()}"
                
                # Dobbiamo interrogare TUTTI gli shard (0..N) per questo attributo
                # perché non sappiamo in quale bucket è finito il dato.
                futures = []
                for shard_id in range(self.INDEX_SHARDS):
                    sharded_key = f"{base_key}:{shard_id}"
                    node = self.ring.get_node(hashlib.sha1(sharded_key.encode()).hexdigest())
                    
                    # Sottometti il task al thread pool
                    futures.append(executor.submit(self._fetch_remote_index, node, sharded_key))
                
                # --- FASE 2: Gather (Unione risultati dello stesso attributo) ---
                attribute_matches = []
                for f in concurrent.futures.as_completed(futures):
                    res = f.result() # Ritorna lista di file o None
                    if res is None:
                        is_partial = True
                    else:
                        attribute_matches.extend(res)
                
                print(f"   -> Attributo '{base_key}': trovati {len(attribute_matches)} file totali su {self.INDEX_SHARDS} shard.")
                candidates_per_attribute.append(attribute_matches)

        # --- FASE 3: Intersezione (AND logico tra attributi diversi) ---
        if not candidates_per_attribute:
            return {
                "results": [],
                "partial_result": is_partial
            }

        # Trasformiamo la prima lista in un dizionario {filename: entry} per accesso rapido
        final_map = {item['filename']: item for item in candidates_per_attribute[0]}
        
        # Intersechiamo con le liste successive
        for i in range(1, len(candidates_per_attribute)):
            current_filenames = set(item['filename'] for item in candidates_per_attribute[i])
            # Tieni solo le chiavi che esistono in entrambi
            keys_to_keep = set(final_map.keys()) & current_filenames
            final_map = {k: final_map[k] for k in keys_to_keep}

        results = list(final_map.values())
        print(f"   ✅ Risultati finali dopo intersezione: {len(results)}")
        
        return {
            "results": results,
            "partial_result": is_partial
        }

    # ==========================
    # Metodi Helper Interni
    # ==========================

    def _gsi_write(self, manifest):
        """
        Scrive i metadati negli indici distribuiti usando il Salting.
        """
        metadata = manifest.get("metadata", {})
        summary = {
            "filename": manifest["filename"],
            "metadata": metadata,
            "host": self.self_id
        }

        print("[MetadataPeer] 📝 Scrittura GSI...")
        
        for key, value in metadata.items():
            # Gestisce liste (es. actors=["a", "b"]) o valori singoli
            values = value if isinstance(value, list) else [value]
            
            for v in values:
                base_key = f"{key}:{str(v).lower().strip()}"
                
                # --- SALTING STRATEGY ---
                # Scegliamo UNO shard a caso per distribuire il carico di scrittura.
                # Invece di scrivere su tutti, scriviamo su 1 solo.
                shard_id = random.randint(0, self.INDEX_SHARDS - 1)
                sharded_key = f"{base_key}:{shard_id}"
                
                target_node = self.ring.get_node(hashlib.sha1(sharded_key.encode()).hexdigest())
                
                try:
                    if target_node == self.self_id:
                        self.storage.save_index_entry(sharded_key, summary)
                    else:
                        requests.post(
                            f"http://{target_node}/index/add", 
                            json={"key": sharded_key, "entry": summary},
                            timeout=2
                        )
                except Exception as e:
                    print(f"⚠️ Errore scrittura GSI su {target_node}: {e}")

    def _fetch_remote_index(self, node, key):
        """Helper per scaricare un indice (locale o remoto)"""
        try:
            if node == self.self_id:
                return self.storage.get_index_entries(key)
            else:
                r = requests.get(
                    f"http://{node}/index/get", 
                    params={"key": key}, 
                    timeout=2
                )
                if r.status_code == 200:
                    return r.json().get("results", [])
        except Exception:
            pass
        return None # Signal failure (Partial Result)