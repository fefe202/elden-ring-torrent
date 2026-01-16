#!/usr/bin/env python3
"""
Script semplice per eseguire graceful shutdown di un peer specifico.

Utilizzo:
python manual_shutdown.py peer1
python manual_shutdown.py peer2  
python manual_shutdown.py peer3
"""

import sys
import requests

def graceful_shutdown_peer(peer_name):
    """Esegue graceful shutdown di un peer specifico"""
    
    # Mapping nomi peer -> URL
    peer_urls = {
        "peer1": "http://localhost:5001",
        "peer2": "http://localhost:5002", 
        "peer3": "http://localhost:5003"
    }
    
    if peer_name not in peer_urls:
        print(f"❌ Peer '{peer_name}' non riconosciuto. Usa: peer1, peer2, o peer3")
        return False
    
    peer_url = peer_urls[peer_name]
    
    print(f"🚪 Eseguendo graceful shutdown di {peer_name} ({peer_url})...")
    
    try:
        # Test connettività prima
        ping_response = requests.get(f"{peer_url}/ping", timeout=3)
        if ping_response.status_code != 200:
            print(f"❌ {peer_name} non risponde al ping")
            return False
        
        # Esegui graceful shutdown
        response = requests.post(f"{peer_url}/shutdown", timeout=15)
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Graceful shutdown di {peer_name} completato!")
            print(f"   📄 Manifest ridistribuiti: {result['redistributed_manifests']}")
            print(f"   ❌ Ridistribuzioni fallite: {result['failed_redistributions']}")
            
            if result.get('redistributed_details'):
                print("   📤 Dettagli ridistribuzione:")
                for detail in result['redistributed_details']:
                    print(f"      • '{detail['filename']}' trasferito a {detail['new_peer']}")
            
            if result.get('failed_details'):
                print("   ❌ Dettagli fallimenti:")
                for detail in result['failed_details']:
                    print(f"      • '{detail['filename']}' -> {detail['target_peer']}: {detail['error']}")
            
            return True
        else:
            print(f"❌ Errore nel graceful shutdown: {response.status_code}")
            print(f"   Risposta: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Errore di connessione: {e}")
        return False

def main():
    """Funzione principale"""
    if len(sys.argv) != 2:
        print("Utilizzo: python manual_shutdown.py <peer_name>")
        print("Esempio: python manual_shutdown.py peer1")
        sys.exit(1)
    
    peer_name = sys.argv[1]
    success = graceful_shutdown_peer(peer_name)
    
    if success:
        print("🎉 Graceful shutdown eseguito con successo!")
    else:
        print("💥 Graceful shutdown fallito!")
        sys.exit(1)

if __name__ == "__main__":
    main()