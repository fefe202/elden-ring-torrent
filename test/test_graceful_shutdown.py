#!/usr/bin/env python3
"""
Script di test per verificare il graceful shutdown dei peer.

Questo script testa:
1. Upload di un file tramite peer1
2. Verifica che i manifest siano distribuiti correttamente
3. Graceful shutdown di peer1 
4. Verifica che i manifest siano stati ridistribuiti agli altri peer
5. Download del file tramite peer2 per verificare che tutto funzioni ancora
"""

import requests
import time
import json

# Configurazione peer
PEER1_URL = "http://localhost:5001"
PEER2_URL = "http://localhost:5002" 
PEER3_URL = "http://localhost:5003"

def test_peer_connectivity():
    """Testa se tutti i peer sono online"""
    print("🔗 Testando connettività dei peer...")
    
    peers = {"peer1": PEER1_URL, "peer2": PEER2_URL, "peer3": PEER3_URL}
    online_peers = {}
    
    for peer_name, url in peers.items():
        try:
            response = requests.get(f"{url}/ping", timeout=3)
            if response.status_code == 200:
                print(f"✅ {peer_name} è online")
                online_peers[peer_name] = url
            else:
                print(f"❌ {peer_name} risponde ma con errore: {response.status_code}")
        except Exception as e:
            print(f"❌ {peer_name} non raggiungibile: {e}")
    
    return online_peers

def upload_test_file(peer_url, filename):
    """Carica un file tramite un peer"""
    print(f"📤 Caricamento file '{filename}' tramite {peer_url}...")
    
    try:
        response = requests.post(
            f"{peer_url}/store_file",
            json={"filename": f"/app/data/{filename}"},
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ File caricato con successo!")
            print(f"   📄 Manifest: {result['manifest']['filename']}")
            print(f"   🧩 Chunks: {len(result['manifest']['chunks'])}")
            return result
        else:
            print(f"❌ Errore nell'upload: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Errore nell'upload: {e}")
        return None

def check_manifest_location(filename):
    """Verifica dove si trova il manifest di un file"""
    print(f"🔍 Cercando manifest per '{filename}'...")
    
    peers = {"peer1": PEER1_URL, "peer2": PEER2_URL, "peer3": PEER3_URL}
    manifest_locations = []
    
    for peer_name, url in peers.items():
        try:
            response = requests.get(f"{url}/get_manifest/{filename}", timeout=3)
            if response.status_code == 200:
                manifest = response.json()
                manifest_locations.append({
                    "peer": peer_name,
                    "url": url,
                    "manifest": manifest
                })
                print(f"   📄 Manifest trovato su {peer_name}")
        except Exception as e:
            # Normal - manifest potrebbe non essere su questo peer
            pass
    
    return manifest_locations

def perform_graceful_shutdown(peer_url, peer_name):
    """Esegue graceful shutdown di un peer"""
    print(f"🚪 Eseguendo graceful shutdown di {peer_name}...")
    
    try:
        response = requests.post(f"{peer_url}/shutdown", timeout=15)
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Graceful shutdown completato!")
            print(f"   📄 Manifest ridistribuiti: {result['redistributed_manifests']}")
            print(f"   ❌ Ridistribuzioni fallite: {result['failed_redistributions']}")
            
            if result.get('redistributed_details'):
                for detail in result['redistributed_details']:
                    print(f"      📤 '{detail['filename']}' -> {detail['new_peer']}")
            
            if result.get('failed_details'):
                for detail in result['failed_details']:
                    print(f"      ❌ '{detail['filename']}' -> {detail['target_peer']}: {detail['error']}")
            
            return result
        else:
            print(f"❌ Errore nel graceful shutdown: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Errore nel graceful shutdown: {e}")
        return None

def download_test_file(peer_url, filename):
    """Scarica un file tramite un peer"""
    print(f"📥 Download file '{filename}' tramite {peer_url}...")
    
    try:
        response = requests.post(
            f"{peer_url}/fetch_file",
            json={"filename": filename},
            timeout=10
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ File scaricato con successo!")
            print(f"   🧩 Chunks scaricati: {len(result['chunks'])}")
            return result
        else:
            print(f"❌ Errore nel download: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Errore nel download: {e}")
        return None

def main():
    """Test principale per graceful shutdown"""
    print("🧪 === TEST GRACEFUL SHUTDOWN ===")
    print()
    
    # 1. Testa connettività
    online_peers = test_peer_connectivity()
    if len(online_peers) < 2:
        print("❌ Servono almeno 2 peer online per il test")
        return
    print()
    
    # 2. Upload file tramite peer1
    filename = "test_file_large.txt"
    upload_result = upload_test_file(PEER1_URL, filename)
    if not upload_result:
        print("❌ Test fallito: impossibile caricare il file")
        return
    print()
    
    # 3. Verifica posizione manifest prima del shutdown
    print("📍 Posizione manifest PRIMA del graceful shutdown:")
    manifest_locations_before = check_manifest_location(filename)
    print()
    
    # 4. Graceful shutdown peer1
    shutdown_result = perform_graceful_shutdown(PEER1_URL, "peer1")
    if not shutdown_result:
        print("❌ Test fallito: graceful shutdown non riuscito")
        return
    print()
    
    # 5. Attendi un momento per la propagazione
    print("⏳ Attendo 3 secondi per la propagazione...")
    time.sleep(3)
    print()
    
    # 6. Verifica posizione manifest dopo il shutdown
    print("📍 Posizione manifest DOPO il graceful shutdown:")
    manifest_locations_after = check_manifest_location(filename)
    print()
    
    # 7. Test download tramite peer2 per verificare che tutto funzioni
    download_result = download_test_file(PEER2_URL, filename)
    if download_result:
        print("✅ Il sistema funziona ancora correttamente dopo il graceful shutdown!")
    else:
        print("❌ Il sistema ha problemi dopo il graceful shutdown")
    print()
    
    # 8. Riepilogo
    print("📊 === RIEPILOGO TEST ===")
    print(f"   📤 Upload riuscito: {'✅' if upload_result else '❌'}")
    print(f"   🚪 Graceful shutdown riuscito: {'✅' if shutdown_result else '❌'}")
    print(f"   📥 Download post-shutdown riuscito: {'✅' if download_result else '❌'}")
    print(f"   📄 Manifest prima: {len(manifest_locations_before)} posizioni")
    print(f"   📄 Manifest dopo: {len(manifest_locations_after)} posizioni")

if __name__ == "__main__":
    main()