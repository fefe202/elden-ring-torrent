# EldenRingTorrent

Sistema distribuito P2P per condivisione file basato su BitTorrent con Consistent Hashing.

## Funzionalità

- **Distribuzione file**: I file vengono divisi in chunk e distribuiti tra i peer usando Consistent Hashing
- **Manifest distribuiti**: Metadati dei file distribuiti per tracciare la posizione dei chunk
- **Download intelligente**: I peer possono scaricare chunk da multiple fonti
- **Graceful Shutdown**: Quando un peer esce dalla rete, ridistribuisce automaticamente i suoi manifest

## Architettura

### Consistent Hashing
Il sistema usa un anello di hash consistente per determinare quale peer è responsabile di ogni chunk e manifest.

### Componenti Principali
- **Peer**: Nodo della rete P2P che può uploadare/downloadare file
- **Storage**: Gestisce il salvataggio locale di chunk e manifest
- **ConsistentHashRing**: Determina la distribuzione di chunk e manifest tra peer

## API Endpoints

### Gestione File
- `POST /store_file` - Carica un file nella rete
- `POST /fetch_file` - Scarica un file dalla rete

### Gestione Chunk e Manifest
- `POST /store_chunk` - Salva un chunk ricevuto da altri peer
- `POST /store_manifest` - Salva un manifest ricevuto da altri peer
- `POST /update_manifest` - Aggiorna un manifest con nuovo peer
- `GET /get_chunk/<hash>` - Scarica un chunk specifico
- `GET /get_manifest/<filename>` - Ottieni manifest di un file

### Graceful Shutdown
- `POST /shutdown` - Esegue uscita controllata dalla rete con ridistribuzione manifest

### Utilità
- `GET /ping` - Test di connettività

## Utilizzo

### Avvio del Sistema
```bash
docker-compose up -d
```

### Upload di un File
```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"filename": "/app/data/test_file_large.txt"}' \
  http://localhost:5001/store_file
```

### Download di un File
```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"filename": "test_file_large.txt"}' \
  http://localhost:5002/fetch_file
```

### Graceful Shutdown di un Peer
```bash
# Metodo 1: API diretta
curl -X POST http://localhost:5001/shutdown

# Metodo 2: Script dedicato
python manual_shutdown.py peer1

# Metodo 3: Test completo
python test_graceful_shutdown.py
```

## Test

### Test Graceful Shutdown
Il file `test_graceful_shutdown.py` esegue un test completo:
1. Upload file tramite peer1
2. Verifica distribuzione manifest
3. Graceful shutdown peer1
4. Verifica ridistribuzione manifest
5. Test download tramite peer2

```bash
python test_graceful_shutdown.py
```

### Test Manuale
Per testare manualmente il graceful shutdown di un peer specifico:

```bash
python manual_shutdown.py peer1
python manual_shutdown.py peer2
python manual_shutdown.py peer3
```

## Configurazione

I peer sono configurati tramite variabili d'ambiente nel `docker-compose.yml`:

- `SELF_ID`: Identificatore del peer (es. "peer1:5000")
- `KNOWN_PEERS`: Lista dei peer conosciuti (es. "peer2:5000,peer3:5000")
- `DATA_DIR`: Directory per salvataggio dati locali
- `PORT`: Porta del servizio HTTP

## Graceful Shutdown

Quando un peer esegue graceful shutdown:

1. **Scansione manifest locali**: Trova tutti i manifest salvati localmente
2. **Calcolo nuove assegnazioni**: Usa Consistent Hashing senza il peer uscente
3. **Ridistribuzione**: Trasferisce ogni manifest al nuovo peer responsabile
4. **Cleanup**: Rimuove i manifest trasferiti e si rimuove dall'anello
5. **Reporting**: Ritorna statistiche sui manifest ridistribuiti/falliti

Questo garantisce che la rete continui a funzionare anche quando peer escono improvvisamente.

## Struttura File

```
peer/
├── peer.py           # Logica principale del peer
├── storage.py        # Gestione storage locale 
├── hashing.py        # Consistent Hash Ring
├── requirements.txt  # Dipendenze Python
└── Dockerfile        # Container configuration

test_graceful_shutdown.py  # Test automatico graceful shutdown
manual_shutdown.py          # Shutdown manuale peer specifico
docker-compose.yml          # Configurazione multi-peer
```