#!/usr/bin/env bash
set -euo pipefail

# Misura dimensione dei file persistenti nei container dei Consensus Node.
# Eseguire su EC2, dalla root del repository, mentre il cluster Docker Compose è attivo.
#
# Uso:
#   CLUSTER_SIZE=5 LABEL=before-backup ./scripts/measure_node_storage.sh
#   CLUSTER_SIZE=5 LABEL=after-backup  ./scripts/measure_node_storage.sh

CLUSTER_SIZE="${CLUSTER_SIZE:-5}"
LABEL="${LABEL:-measurement}"
OUT="${OUT:-reports/raw/node_storage_measurements.csv}"

mkdir -p "$(dirname "$OUT")"

if [ ! -f "$OUT" ]; then
  echo "timestamp_utc,label,node_id,file,path,bytes" > "$OUT"
fi

now_utc() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

measure_file() {
  local node="$1"
  local file="$2"
  local path="/data/$file"
  local bytes
  bytes=$(docker exec "sdcc-$node" sh -c "if [ -f '$path' ]; then wc -c < '$path'; else echo 0; fi" | tr -d '[:space:]')
  echo "$(now_utc),$LABEL,$node,$file,$path,$bytes" >> "$OUT"
}

for i in $(seq 1 "$CLUSTER_SIZE"); do
  node="node-$i"
  measure_file "$node" "wal.log"
  measure_file "$node" "snapshot.json"
  measure_file "$node" "state.json"
done

echo "Misure salvate in $OUT"
