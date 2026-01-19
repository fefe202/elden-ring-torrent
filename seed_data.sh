#!/bin/bash
# Crea i file dummy su tutti i worker con permessi di ROOT (sudo)

NUM_FILES=500

echo "🌱 Inizio seeding dei dati sui worker..."

# Loop sui 4 worker
for w in {1..4}; do
  echo "➡️  Configurazione Worker-$w..."
  
  # Comando da eseguire dentro la VM:
  # 1. Usa sudo per creare cartelle e file
  # 2. Imposta permessi 777 (tutti possono leggere/scrivere) per evitare problemi con Docker
  CMD="
    sudo bash -c '
    for p in {1..5}; do
      echo \"  Processing peer-\$p...\";
      mkdir -p /var/data/peer-\$p;
      
      # Creazione rapida di 500 file vuoti
      for i in \$(seq 0 $((NUM_FILES-1))); do
        touch /var/data/peer-\$p/movie_\$i.bin;
      done
      
      # FIX PERMESSI: Rende i file leggibili/scrivibili da chiunque (anche dal container)
      chmod -R 777 /var/data/peer-\$p
    done
    '
  "
  
  # Esegui via SSH
  gcloud compute ssh worker-$w --zone=europe-west1-b --command "$CMD" --quiet
done

echo "✅ Seeding completato! File creati e permessi sbloccati."