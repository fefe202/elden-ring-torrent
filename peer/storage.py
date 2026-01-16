#!/usr/bin/env python3
import os
import hashlib
import json

# Dimensione di ogni chunk: 1 MB (1024 * 1024 bytes)
# I file vengono suddivisi in pezzi di questa dimensione per la distribuzione
CHUNK_SIZE = 1024 * 1024  # 1 MB

class Storage:
    """
    Classe per la gestione dello storage locale di un peer nel sistema BitTorrent distribuito.
    
    Responsabilità:
    - Suddividere file in chunk di dimensione fissa
    - Salvare e caricare chunk dal disco
    - Creare e gestire manifest (metadati dei file)
    - Tracciare quali peer possiedono quali chunk
    
    Ogni peer ha la propria istanza di Storage che gestisce la sua directory dati locale.
    """
    
    def __init__(self, data_dir):
        """
        Inizializza lo storage per un peer.
        
        Args:
            data_dir (str): Path della directory dove salvare i dati del peer
                           (es. "data_peer1", "data_peer2")
        
        Crea la directory se non esiste.
        """
        self.data_dir = data_dir
        # Crea la directory dati se non esiste già (exist_ok=True evita errori)
        os.makedirs(data_dir, exist_ok=True)

    def _chunk_filename(self, chunk_hash):
        """
        Genera il path completo per salvare un chunk.
        
        I chunk sono salvati con il loro hash SHA-1 come nome file.
        Questo permette di identificarli univocamente e verificarne l'integrità.
        
        Args:
            chunk_hash (str): Hash SHA-1 del chunk (40 caratteri esadecimali)
        
        Returns:
            str: Path completo del file chunk
        
        Esempio:
            _chunk_filename("8b2d01bc0230a5558c363701c5e1fb2956ceafae")
            -> "data_peer1/8b2d01bc0230a5558c363701c5e1fb2956ceafae"
        """
        return os.path.join(self.data_dir, chunk_hash)

    def _manifest_filename(self, file_hash):
        """
        Genera il path completo per salvare un manifest.
        
        I manifest sono salvati con estensione .manifest.json per distinguerli dai chunk.
        Il nome base è l'hash del nome del file originale.
        
        Args:
            file_hash (str): Hash SHA-1 del nome del file originale
        
        Returns:
            str: Path completo del file manifest
        
        Esempio:
            _manifest_filename("a1b2c3d4...")
            -> "data_peer1/a1b2c3d4....manifest.json"
        """
        return os.path.join(self.data_dir, f"{file_hash}.manifest.json")

    def split_file(self, filepath):
        """
        Suddivide un file in chunk di dimensione fissa e calcola gli hash.
        
        Questo è il primo passo quando un file viene caricato nel sistema:
        1. Legge il file in blocchi di CHUNK_SIZE bytes
        2. Per ogni blocco calcola l'hash SHA-1
        3. Ritorna una lista di tuple (indice, hash, dati)
        
        Args:
            filepath (str): Path del file da suddividere
        
        Returns:
            list: Lista di tuple (idx, chunk_hash, data) dove:
                  - idx: indice del chunk (0, 1, 2, ...)
                  - chunk_hash: hash SHA-1 del chunk
                  - data: contenuto binario del chunk
        
        Esempio:
            split_file("test.txt")  # File da 2.5 MB
            -> [(0, "hash1", b"dati chunk 0..."), 
                (1, "hash2", b"dati chunk 1..."),
                (2, "hash3", b"dati chunk 2...")]  # 3 chunk (1MB + 1MB + 0.5MB)
        """
        chunks = []
        # Apri il file in modalità binaria (rb = read binary)
        with open(filepath, "rb") as f:
            idx = 0
            while True:
                # Leggi fino a CHUNK_SIZE bytes
                data = f.read(CHUNK_SIZE)
                # Se non ci sono più dati, esci dal loop
                if not data:
                    break
                # Calcola l'hash SHA-1 del chunk
                chunk_hash = hashlib.sha1(data).hexdigest()
                # Aggiungi la tupla alla lista
                chunks.append((idx, chunk_hash, data))
                idx += 1
        return chunks

    def save_chunk(self, chunk_hash, data):
        """
        Salva un chunk su disco.
        
        Il chunk viene salvato come file binario con nome uguale al suo hash.
        Questo permette:
        - Identificazione univoca del chunk
        - Verifica automatica dell'integrità (ricalcolando l'hash)
        - Deduplicazione (stesso contenuto = stesso hash = stesso file)
        
        Args:
            chunk_hash (str): Hash SHA-1 del chunk
            data (bytes): Contenuto binario del chunk da salvare
        
        Esempio:
            save_chunk("8b2d01bc...", b"Hello World! ...")
            # Crea file: data_peer1/8b2d01bc...
        """
        # wb = write binary
        with open(self._chunk_filename(chunk_hash), "wb") as f:
            f.write(data)

    def load_chunk(self, chunk_hash):
        """
        Carica un chunk dal disco.
        
        Verifica prima se il chunk esiste localmente, poi lo legge.
        
        Args:
            chunk_hash (str): Hash SHA-1 del chunk da caricare
        
        Returns:
            bytes: Contenuto del chunk se esiste, None altrimenti
        
        Esempio:
            data = load_chunk("8b2d01bc...")
            if data:
                print("Chunk trovato!")  # Chunk presente localmente
            else:
                print("Chunk non trovato")  # Devo scaricarlo da altri peer
        """
        path = self._chunk_filename(chunk_hash)
        # Controlla se il file esiste
        if os.path.exists(path):
            # rb = read binary
            with open(path, "rb") as f:
                return f.read()
        return None

    def create_manifest(self, filename, chunks, peers_map, metadata=None):
        """
        Crea un manifest (file di metadati) per un file distribuito.
        
        Il manifest contiene:
        - Informazioni sul file originale (nome, dimensione)
        - Parametri di chunking (dimensione chunk)
        - Lista di chunk con hash e peer che li possiedono
        
        Questo permette ai peer di:
        - Sapere quali chunk servono per ricostruire il file
        - Trovare da quali peer scaricare ciascun chunk
        - Verificare l'integrità di ogni chunk
        
        Args:
            filename (str): Path del file originale
            chunks (list): Lista di tuple (idx, chunk_hash, data) da split_file()
            peers_map (dict): Dizionario {chunk_hash: peer_id} che mappa ogni chunk
                             al peer responsabile iniziale (dal consistent hashing)
        
        Returns:
            dict: Manifest con struttura:
                {
                    "filename": "nome_file.txt",
                    "total_size": 2500000,  # bytes totali
                    "chunk_size": 1048576,  # 1 MB
                    "chunks": [
                        {"index": 0, "hash": "...", "peers": ["peer1:5000"]},
                        {"index": 1, "hash": "...", "peers": ["peer2:5000"]},
                        ...
                    ]
                }
        
        Esempio:
            chunks = [(0, "hash1", data1), (1, "hash2", data2)]
            peers_map = {"hash1": "peer1:5000", "hash2": "peer2:5000"}
            manifest = create_manifest("test.txt", chunks, peers_map)
            # Il manifest traccia che hash1 è su peer1 e hash2 è su peer2
        """
        # Ottieni la dimensione totale del file originale
        total_size = os.path.getsize(filename)
        
        manifest = {
            # Nome del file (solo basename, senza path)
            "filename": os.path.basename(filename),
            # Dimensione totale in bytes
            "total_size": total_size,
            # Dimensione di ogni chunk
            "chunk_size": CHUNK_SIZE,
            # Lista di chunk con metadati
            "chunks": [
                {
                    "index": idx,           # Posizione del chunk nel file
                    "hash": ch_hash,        # Hash per identificazione e verifica
                    "peers": [peers_map[ch_hash]]  # Lista dei peer che lo possiedono
                }
                for idx, ch_hash, _ in chunks
            ],
            "metadata": metadata or {}
        }
        return manifest

    def save_manifest(self, manifest):
        """
        Salva un manifest su disco in formato JSON.
        
        Il manifest viene salvato con un nome basato sull'hash del filename originale.
        L'estensione .manifest.json lo distingue dai chunk normali.
        
        Args:
            manifest (dict): Dizionario manifest da salvare
        
        Returns:
            str: Hash SHA-1 del filename (usato come identificatore del manifest)
        
        Esempio:
            manifest = {"filename": "test.txt", ...}
            file_hash = save_manifest(manifest)
            # Crea: data_peer1/<file_hash>.manifest.json
            # Ritorna: file_hash per riferimenti futuri
        """
        # Calcola l'hash del nome file per usarlo come identificatore
        file_hash = hashlib.sha1(manifest["filename"].encode()).hexdigest()
        
        # Salva il manifest in formato JSON leggibile (indent=2 per formattazione)
        with open(self._manifest_filename(file_hash), "w") as f:
            json.dump(manifest, f, indent=2)
        
        return file_hash

    def load_manifest(self, filename):
        """
        Carica un manifest dal disco.
        
        Cerca il manifest basandosi sull'hash del filename.
        
        Args:
            filename (str): Nome del file originale (non il path completo)
        
        Returns:
            dict: Manifest caricato se esiste, None altrimenti
        
        Esempio:
            manifest = load_manifest("test.txt")
            if manifest:
                chunks = manifest["chunks"]  # Accedi ai chunk
        """
        # Calcola l'hash del filename per trovare il manifest
        file_hash = hashlib.sha1(filename.encode()).hexdigest()
        path = self._manifest_filename(file_hash)
        
        # Controlla se il manifest esiste
        if os.path.exists(path):
            with open(path, "r") as f:
                return json.load(f)
        return None

    def update_manifest_with_peer(self, filename, chunk_hash, new_peer):
        """
        Aggiorna il manifest aggiungendo un peer alla lista di chi possiede un chunk.
        
        Questa funzione è chiamata quando:
        - Un peer scarica con successo un chunk
        - Vogliamo tracciare che quel peer ora ha quel chunk
        
        Questo permette di:
        - Aumentare la ridondanza (più copie del chunk)
        - Bilanciare il carico (più peer possono servire lo stesso chunk)
        - Tolleranza ai guasti (se un peer si disconnette, altri hanno il chunk)
        
        Args:
            filename (str): Nome del file originale
            chunk_hash (str): Hash del chunk da aggiornare
            new_peer (str): Identificatore del peer da aggiungere (es. "peer3:5000")
        
        Returns:
            bool: True se il manifest è stato aggiornato, False se manifest non trovato
                  o peer già presente
        
        Esempio:
            # peer3 ha appena scaricato il chunk "abc123..."
            success = update_manifest_with_peer("test.txt", "abc123...", "peer3:5000")
            # Ora il manifest mostra che sia peer1 che peer3 hanno quel chunk
            # {"hash": "abc123...", "peers": ["peer1:5000", "peer3:5000"]}
        """
        # Carica il manifest esistente
        manifest = self.load_manifest(filename)
        if not manifest:
            return False  # manifest non trovato

        updated = False
        # Cerca il chunk specifico nel manifest
        for chunk in manifest["chunks"]:
            if chunk["hash"] == chunk_hash:
                # Aggiungi il peer solo se non è già nella lista
                if new_peer not in chunk["peers"]:
                    chunk["peers"].append(new_peer)
                    updated = True
                break

        # Se ci sono state modifiche, salva il manifest aggiornato
        if updated:
            self.save_manifest(manifest)
        return updated

    def list_local_manifests(self):
        """
        Trova tutti i manifest salvati localmente in questo peer.
        
        Scansiona la directory dati cercando file con estensione .manifest.json
        e li carica tutti.
        
        Returns:
            list: Lista di dizionari manifest trovati localmente
        
        Esempio:
            manifests = storage.list_local_manifests()
            for manifest in manifests:
                print(f"File: {manifest['filename']}")
        """
        manifests = []
        
        # Scansiona tutti i file nella directory dati
        for filename in os.listdir(self.data_dir):
            # Cerca file con estensione .manifest.json
            if filename.endswith('.manifest.json'):
                manifest_path = os.path.join(self.data_dir, filename)
                try:
                    with open(manifest_path, 'r') as f:
                        manifest = json.load(f)
                        manifests.append(manifest)
                except (json.JSONDecodeError, IOError) as e:
                    print(f"Errore nel caricamento del manifest {filename}: {e}")
        
        return manifests

    def remove_local_manifest(self, filename):
        """
        Rimuove un manifest dal disco locale.
        
        Args:
            filename (str): Nome del file originale del manifest da rimuovere
        
        Returns:
            bool: True se il manifest è stato rimosso, False se non esisteva
        """
        file_hash = hashlib.sha1(filename.encode()).hexdigest()
        manifest_path = self._manifest_filename(file_hash)
        
        if os.path.exists(manifest_path):
            try:
                os.remove(manifest_path)
                return True
            except IOError as e:
                print(f"Errore nella rimozione del manifest {filename}: {e}")
                return False
        return False
    
    def rebuild_file(self, manifest, output_path):
        """
        Ricostruisce il file originale a partire dai chunk salvati localmente,
        con stampe di debug dettagliate per capire quale chunk manca o è corrotto.
        """
        print(f"[Storage] Avvio rebuild_file -> output: {output_path}")

        if not manifest or "chunks" not in manifest:
            raise ValueError("Manifest non valido o vuoto")

        # Ordina i chunk per indice per garantire l'ordine corretto
        ordered_chunks = sorted(manifest["chunks"], key=lambda c: c.get("index", 0))
        total_chunks = len(ordered_chunks)
        print(f"[Storage] Manifest con {total_chunks} chunk. CHUNK_SIZE={CHUNK_SIZE} bytes")

        written = 0
        with open(output_path, "wb") as out:
            for i, ch in enumerate(ordered_chunks):
                ch_hash = ch.get("hash")
                if not ch_hash:
                    raise ValueError(f"[Storage] Chunk senza hash nella posizione {i}: {ch}")

                chunk_path = self._chunk_filename(ch_hash)
                exists = os.path.exists(chunk_path)
                print(f"[Storage] [{i+1}/{total_chunks}] hash={ch_hash} path={chunk_path} exists={exists}")

                if not exists:
                    # Fornisce informazioni aggiuntive prima di fallire
                    peers = ch.get("peers", [])
                    raise FileNotFoundError(f"[Storage] Chunk mancante: {ch_hash} (index {i}). Peers noti: {peers}")

                # Carica e scrive il chunk
                with open(chunk_path, "rb") as cf:
                    data = cf.read()
                    if hashlib.sha1(data).hexdigest() != ch_hash:
                        raise IOError(f"[Storage] Errore integrità chunk {ch_hash}: hash mismatch")
                    out.write(data)
                    written += len(data)
                    print(f"[Storage] [{i+1}/{total_chunks}] scritto {len(data)} bytes, totale scritto {written} bytes")

        print(f"[Storage] Rebuild completato: {output_path} ({written} bytes scritti)")
        return output_path

    def save_index_entry(self, sharded_key, manifest_summary):
        """
        Salva una voce nell'indice locale.
        sharded_key: es. "actor:brad pitt:2" (già saltata)
        manifest_summary: dati essenziali del file (filename, metadata, host)
        """
        # Sanitizza il nome file per evitare caratteri illegali
        safe_name = hashlib.md5(sharded_key.encode()).hexdigest()
        path = os.path.join(self.data_dir, f"idx_{safe_name}.json")
        
        entries = []
        # Carica esistente
        if os.path.exists(path):
            try:
                with open(path, 'r') as f:
                    entries = json.load(f)
            except:
                entries = []
        
        # Evita duplicati (Idempotenza)
        for e in entries:
            if e["filename"] == manifest_summary["filename"]:
                return # Già presente

        entries.append(manifest_summary)
        
        # Scrivi su disco (Atomico sarebbe meglio, ma ok per ora)
        with open(path, 'w') as f:
            json.dump(entries, f)

    def get_index_entries(self, sharded_key):
        """Legge l'indice locale per una specifica chiave shardata"""
        safe_name = hashlib.md5(sharded_key.encode()).hexdigest()
        path = os.path.join(self.data_dir, f"idx_{safe_name}.json")
        
        if os.path.exists(path):
            try:
                with open(path, 'r') as f:
                    return json.load(f)
            except:
                pass
        return []
    
    def get_storage_stats(self):
        """
        Ritorna statistiche sull'utilizzo dello storage locale.
        Utile per il benchmark del Load Balancing.
        """
        stats = {
            "chunks_count": 0,
            "chunks_bytes": 0,
            "manifests_count": 0,
            "indexes_count": 0,
            "total_files": 0
        }
        
        try:
            for f in os.listdir(self.data_dir):
                path = os.path.join(self.data_dir, f)
                if os.path.isfile(path):
                    stats["total_files"] += 1
                    size = os.path.getsize(path)
                    
                    if f.endswith(".manifest.json"):
                        stats["manifests_count"] += 1
                    elif f.startswith("idx_"):
                        stats["indexes_count"] += 1
                    elif len(f) == 40: # Sha1 hash (Chunk)
                        stats["chunks_count"] += 1
                        stats["chunks_bytes"] += size
        except Exception:
            pass
            
        return stats
