# ==========================================
# CHANGE MODE: METADATA (30 NODES)
# ==========================================

# 1. DESTRUCTION (Cleans old SEMANTIC network)
Write-Host "Deleting workers..." -ForegroundColor Red
1..5 | ForEach-Object { gcloud compute instances delete worker-$_ --zone=europe-west1-b --quiet }

# 2. NEW CREATION (Mode: METADATA)
Write-Host "Creating Workers (Mode: METADATA)..." -ForegroundColor Cyan
1..5 | ForEach-Object {
  Write-Host "   -> Starting worker-$_ ..."
  gcloud compute instances create "worker-$_" `
    --machine-type=e2-medium `
    --image-family=ubuntu-2204-lts `
    --image-project=ubuntu-os-cloud `
    --metadata=PEER_MODE=METADATA `
    --metadata-from-file=startup-script=startup-hybrid.sh `
    --tags=p2p-node `
    --zone=europe-west1-b --quiet
}

# 3. WAIT
Write-Host "Waiting for Docker (3 minutes)..." -ForegroundColor Yellow
Start-Sleep -Seconds 180

# 4. DATA SEEDING (5000 Files x 6 Peers)
Write-Host "Data Injection..." -ForegroundColor Green
1..5 | ForEach-Object {
  $worker = "worker-$_"
  $cmd = "sudo bash -c 'for p in {1..6}; do mkdir -p /var/data/peer-`$p; touch /var/data/peer-`$p/movie_{0..4999}.bin; chmod -R 777 /var/data/peer-`$p; done'"
  gcloud compute ssh $worker --zone=europe-west1-b --command $cmd --quiet
}

Write-Host "READY FOR METADATA! Go to the Orchestrator." -ForegroundColor Green