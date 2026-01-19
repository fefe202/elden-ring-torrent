# Imposta la zona per sicurezza
gcloud config set compute/zone europe-west1-b

# Loop di creazione
for i in {1..20}; do
  gcloud compute instances create "peer-$i" \
    --machine-type=e2-medium \
    --image-family=ubuntu-2204-lts \
    --image-project=ubuntu-os-cloud \
    --metadata=PEER_MODE=NAIVE \
    --metadata-from-file=startup-script=startup-script.sh \
    --tags=p2p-node &
done