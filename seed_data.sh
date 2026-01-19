#!/bin/bash
# Crea i file dummy su tutti i worker per permettere l'indicizzazione

NUM_FILES=500

echo "🌱 Inizio seeding dei dati sui worker..."

# Loop sui 4 worker
for w in {1..4}; do
  echo "➡️  Configurazione Worker-$w..."
  
  # Comando da eseguire dentro la VM del worker
  # Crea i file movie_0.bin ... movie_499.bin nella cartella condivisa /var/data/peer-X
  CMD="
    for p in {1..5}; do
      echo '  Processing peer-\$p...';
      mkdir -p /var/data/peer-\$p;
      for i in \$(seq 0 $((NUM_FILES-1))); do
        touch /var/data/peer-\$p/movie_\$i.bin;
      done
    done
  "
  
  # Esegui via SSH
  gcloud compute ssh worker-$w --zone=europe-west1-b --command "$CMD" --quiet
done

echo "✅ Seeding completato! Ora puoi lanciare il benchmark."