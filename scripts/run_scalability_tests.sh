#!/usr/bin/env bash
set -euo pipefail

# Esegue i benchmark di scalabilità dal PC locale verso il Proxy su EC2.
# Uso:
#   EC2_IP=34.207.234.177 ./scripts/run_scalability_test.sh
#
# Prerequisiti:
#   - cluster corretto avviato su EC2 prima di ogni run:
#       3 nodi -> docker-compose.yml
#       5 nodi -> docker-compose-5nodes.yml
#       7 nodi -> docker-compose-7nodes.yml
#   - porta 8080 aperta nel Security Group
#   - cmd/perf-client disponibile localmente

if [ -z "${EC2_IP:-}" ]; then
  echo "Errore: imposta EC2_IP, esempio:"
  echo "  EC2_IP=34.207.234.177 ./scripts/run_scalability_test.sh"
  exit 1
fi

TARGET="${EC2_IP}:8080"
PUTS="${PUTS:-300}"
GETS="${GETS:-300}"
WARMUP_PUTS="${WARMUP_PUTS:-30}"
WARMUP_GETS="${WARMUP_GETS:-30}"
CONCURRENCY="${CONCURRENCY:-1}"

mkdir -p reports/raw reports/processed reports/figures

run_case() {
  local cluster_size="$1"

  echo "============================================================"
  echo "Benchmark scalabilità: ${cluster_size} nodi"
  echo "Target: ${TARGET}"
  echo "PUTS=${PUTS} GETS=${GETS} WARMUP_PUTS=${WARMUP_PUTS} WARMUP_GETS=${WARMUP_GETS} CONCURRENCY=${CONCURRENCY}"
  echo "============================================================"

  TARGET="${TARGET}" \
  CLUSTER_SIZE="${cluster_size}" \
  WARMUP_PUTS="${WARMUP_PUTS}" \
  WARMUP_GETS="${WARMUP_GETS}" \
  PUTS="${PUTS}" \
  GETS="${GETS}" \
  CONCURRENCY="${CONCURRENCY}" \
  KEY_PREFIX="ec2-scalability-${cluster_size}" \
  CSV_OUT="reports/raw/scalability_${cluster_size}nodes.csv" \
  SUMMARY_OUT="reports/processed/scalability_${cluster_size}nodes_summary.csv" \
  go run ./cmd/perf-client

  echo "Summary ${cluster_size} nodi:"
  cat "reports/processed/scalability_${cluster_size}nodes_summary.csv"
  echo
}

cat <<'EOF'
IMPORTANTE:
Questo script lancia solo il client di benchmark dal PC locale.
Prima di ogni step devi avviare su EC2 il cluster corrispondente:

  3 nodi: docker compose -f deployments/docker/docker-compose.yml up -d --build
  5 nodi: docker compose -f deployments/docker/docker-compose-5nodes.yml up -d --build
  7 nodi: docker compose -f deployments/docker/docker-compose-7nodes.yml up -d --build

Premi INVIO quando il cluster da 3 nodi è attivo su EC2.
EOF
read -r _
run_case 3

cat <<'EOF'
Ora su EC2 ferma il cluster da 3 nodi e avvia quello da 5 nodi:

  docker compose -f deployments/docker/docker-compose.yml down -v
  docker compose -f deployments/docker/docker-compose-5nodes.yml up -d --build

Premi INVIO quando il cluster da 5 nodi è healthy.
EOF
read -r _
run_case 5

cat <<'EOF'
Ora su EC2 ferma il cluster da 5 nodi e avvia quello da 7 nodi:

  docker compose -f deployments/docker/docker-compose-5nodes.yml down -v
  docker compose -f deployments/docker/docker-compose-7nodes.yml up -d --build

Premi INVIO quando il cluster da 7 nodi è healthy.
EOF
read -r _
run_case 7

if [ -f scripts/plot_results.py ]; then
  echo "Genero grafici..."
  python scripts/plot_results.py
else
  echo "scripts/plot_results.py non trovato: salta generazione grafici."
fi

echo "Benchmark di scalabilità completati."
