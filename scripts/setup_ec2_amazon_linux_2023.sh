#!/usr/bin/env bash
set -euo pipefail

# Setup base per Amazon Linux 2023 in AWS Academy Learner Lab.
# Eseguire dentro l'istanza EC2 come ec2-user.

echo "[1/6] Update sistema e installazione Docker/Git"
sudo dnf update -y
sudo dnf install -y docker git

echo "[2/6] Avvio Docker"
sudo systemctl start docker
sudo systemctl enable docker

echo "[3/6] Aggiunta ec2-user al gruppo docker"
sudo usermod -aG docker ec2-user || true

echo "[4/6] Installazione Docker Compose plugin"
sudo mkdir -p /usr/local/lib/docker/cli-plugins
sudo curl -SL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-$(uname -m)" \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

echo "[5/6] Installazione Docker Buildx aggiornato"
sudo curl -SL "https://github.com/docker/buildx/releases/download/v0.35.0/buildx-v0.35.0.linux-amd64" \
  -o /usr/local/lib/docker/cli-plugins/docker-buildx
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-buildx

echo "[6/6] Verifica versioni"
docker --version || sudo docker --version
docker compose version || sudo docker compose version
docker buildx version || sudo docker buildx version
git --version

echo "Setup completato. Se docker senza sudo non funziona, uscire e rientrare via SSH."
