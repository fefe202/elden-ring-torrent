import hashlib
import bisect

class ConsistentHashRing:
    def __init__(self, nodes=None, replicas=100):
        """
        Anello di Consistent Hashing con Virtual Nodes per un bilanciamento uniforme.
        
        Args:
            nodes (list): Lista iniziale dei nodi (es. ['peer1:5000', ...])
            replicas (int): Numero di nodi virtuali per ogni nodo fisico. 
                            100-200 è un buon numero per bilanciare cluster piccoli.
        """
        self.replicas = replicas
        self.ring = {}
        self.sorted_keys = []

        if nodes:
            for node in nodes:
                self.add_node(node)

    def _hash(self, key):
        """Ritorna l'hash MD5 (intero) della chiave per posizionarla sull'anello."""
        return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)

    def add_node(self, node):
        """Aggiunge un nodo fisico (e i suoi vNodes) all'anello."""
        for i in range(self.replicas):
            # Creiamo N repliche virtuali sparse per l'anello
            virtual_node_key = f"{node}#{i}"
            key_hash = self._hash(virtual_node_key)
            
            self.ring[key_hash] = node
            bisect.insort(self.sorted_keys, key_hash)

    def remove_node(self, node):
        """Rimuove un nodo fisico (e i suoi vNodes) dall'anello."""
        for i in range(self.replicas):
            virtual_node_key = f"{node}#{i}"
            key_hash = self._hash(virtual_node_key)
            
            if key_hash in self.ring:
                del self.ring[key_hash]
                try:
                    self.sorted_keys.remove(key_hash)
                except ValueError:
                    pass

    def get_node(self, item_key):
        """
        Trova il nodo responsabile per una data chiave (es. filename o chunk_hash).
        """
        if not self.ring:
            return None
            
        hash_val = self._hash(item_key)
        
        # Trova il primo nodo virtuale con hash >= hash_val (Binary Search)
        idx = bisect.bisect(self.sorted_keys, hash_val)
        
        # Se siamo alla fine dell'anello, torniamo al primo (comportamento circolare)
        if idx == len(self.sorted_keys):
            idx = 0
            
        return self.ring[self.sorted_keys[idx]]

    def get_successors(self, item_key, count=1):
        """
        Ritorna il nodo responsabile E i suoi successori (per la replica).
        Gestisce il caso in cui nodi virtuali adiacenti appartengano allo stesso nodo fisico.
        """
        if not self.ring:
            return []

        hash_val = self._hash(item_key)
        idx = bisect.bisect(self.sorted_keys, hash_val)
        
        unique_nodes = []
        seen = set()
        
        # Scorriamo l'anello finché non troviamo 'count' nodi FISICI distinti
        # L'anello potrebbe avere 700 punti, noi ne vogliamo 3 distinti (es. peer1, peer4, peer2)
        total_keys = len(self.sorted_keys)
        
        # Evitiamo loop infinito se count > nodi fisici disponibili
        attempts = 0
        max_attempts = total_keys * 2 
        
        while len(unique_nodes) < count and attempts < max_attempts:
            if idx == total_keys:
                idx = 0
            
            key_hash = self.sorted_keys[idx]
            physical_node = self.ring[key_hash]
            
            if physical_node not in seen:
                unique_nodes.append(physical_node)
                seen.add(physical_node)
            
            idx += 1
            attempts += 1
            
        return unique_nodes