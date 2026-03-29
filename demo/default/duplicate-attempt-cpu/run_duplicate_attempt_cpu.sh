#!/bin/bash
# =============================================================================
# run_duplicate_attempt_cpu.sh
# Jalankan job Kubernetes dari YAML di folder duplicate-attempt-cpu
# Ambil metrics, simpan ke duplicate-attempt-cpu.csv
#
# Cara pakai:
#   bash run_duplicate_attempt_cpu.sh
#   bash run_duplicate_attempt_cpu.sh --yaml-dir /path/ke/folder/yaml
# =============================================================================

WAIT_TIMEOUT=1800
POLL_INTERVAL=10
YAML_DIR="./duplicate-attempt-cpu"
CSV_FILE="duplicate-attempt-cpu.csv"

# ─── Parse argumen ────────────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
  case $1 in
    --yaml-dir)
      YAML_DIR="$2"
      shift 2
      ;;
    *)
      echo "Argumen tidak dikenal: $1"
      echo "Cara pakai: bash run_duplicate_attempt_cpu.sh [--yaml-dir path]"
      exit 1
      ;;
  esac
done

# ─── Validasi ─────────────────────────────────────────────────────────────────

if [ ! -d "$YAML_DIR" ]; then
  echo "ERROR: Folder $YAML_DIR tidak ditemukan."
  exit 1
fi

# ─── Buat CSV kalau belum ada ─────────────────────────────────────────────────

CSV_HEADER="id,attempt_id,job_name,pod_name,size,fill_a,fill_b,logs_end,logs_end_time,logs_start,logs_start_time,logs_wall_cs,logs_wall_us,logs_cpu_usage,pod_creation_timestamp,container_creation_timestamp,started_at,finished_at,scheduled_at"

if [ ! -f "$CSV_FILE" ]; then
  echo "$CSV_HEADER" > "$CSV_FILE"
  echo "CSV baru dibuat: $CSV_FILE"
fi

# ─── Fungsi: parse size dan fill dari YAML ───────────────────────────────────

parse_yaml_args() {
  local yaml_file=$1

  # Ambil baris args dari YAML, cari pattern "python3 /app/matrix_mult.py SIZE FILL_A FILL_B"
  local args_line
  args_line=$(grep -A1 "matrix_mult.py" "$yaml_file" 2>/dev/null | grep "matrix_mult.py" | head -1)

  YAML_SIZE=$(echo "$args_line" | grep -oP 'matrix_mult\.py\s+\K\S+')
  YAML_FILL_A=$(echo "$args_line" | grep -oP 'matrix_mult\.py\s+\S+\s+\K\S+')
  YAML_FILL_B=$(echo "$args_line" | grep -oP 'matrix_mult\.py\s+\S+\s+\S+\s+\K\S+')

  # Fallback: cari di args list (format: args dengan item terpisah)
  if [ -z "$YAML_SIZE" ]; then
    # Cari pola: python3 /app/matrix_mult.py di dalam block args
    local full_args
    full_args=$(grep -A5 "matrix_mult.py" "$yaml_file" 2>/dev/null | tr '\n' ' ')
    YAML_SIZE=$(echo "$full_args"   | grep -oP 'matrix_mult\.py\s+\K[0-9]+')
    YAML_FILL_A=$(echo "$full_args" | grep -oP 'matrix_mult\.py\s+[0-9]+\s+\K\S+')
    YAML_FILL_B=$(echo "$full_args" | grep -oP 'matrix_mult\.py\s+[0-9]+\s+\S+\s+\K\S+')
  fi
}

# ─── Fungsi: tunggu pod selesai ───────────────────────────────────────────────

tunggu_pod_selesai() {
  local job_name=$1
  local elapsed=0

  echo "  Menunggu pod selesai..."

  while [ $elapsed -lt $WAIT_TIMEOUT ]; do
    STATUS=$(kubectl get pods \
      --selector=job-name=${job_name} \
      --no-headers 2>/dev/null | awk '{print $3}' | head -1)

    if [ "$STATUS" = "Completed" ]; then
      echo "  Pod selesai (${elapsed}s)"
      return 0
    elif [ "$STATUS" = "Error" ] || [ "$STATUS" = "OOMKilled" ] || [ "$STATUS" = "CrashLoopBackOff" ]; then
      echo "  ERROR: Pod gagal dengan status $STATUS"
      return 1
    fi

    sleep $POLL_INTERVAL
    elapsed=$((elapsed + POLL_INTERVAL))
    echo "  Masih berjalan... (${elapsed}s, status: ${STATUS})"
  done

  echo "  TIMEOUT: Pod tidak selesai dalam ${WAIT_TIMEOUT} detik"
  return 1
}

# ─── Fungsi: ambil pod name ───────────────────────────────────────────────────

ambil_pod_name() {
  local job_name=$1
  kubectl get pods \
    --selector=job-name=${job_name} \
    --no-headers 2>/dev/null | grep "Completed" | awk '{print $1}' | head -1
}

# ─── Fungsi: ambil logs ───────────────────────────────────────────────────────

ambil_logs() {
  local pod_name=$1
  kubectl logs "$pod_name" 2>/dev/null
}

# ─── Fungsi: parse logs ───────────────────────────────────────────────────────

parse_logs() {
  local logs=$1
  LOG_END=$(echo "$logs"       | grep "^END:"        | awk '{print $2}')
  LOG_END_TIME=$(echo "$logs"  | grep "^END_TIME:"   | awk '{print $2}')
  LOG_START=$(echo "$logs"     | grep "^START:"      | awk '{print $2}')
  LOG_START_TIME=$(echo "$logs"| grep "^START_TIME:" | awk '{print $2}')
  LOG_WALL_CS=$(echo "$logs"   | grep "^WALL_CS:"    | awk '{print $2}')
  LOG_WALL_US=$(echo "$logs"   | grep "^WALL_US:"    | awk '{print $2}')
  LOG_CPU=$(echo "$logs"       | grep "^CPU usage:"  | awk '{print $3}')
}

# ─── Fungsi: ambil timestamps dari kubectl ───────────────────────────────────

ambil_timestamps() {
  local pod_name=$1
  local describe
  describe=$(kubectl get pod "$pod_name" -o json 2>/dev/null)

  POD_CREATION_TIMESTAMP=$(echo "$describe" | \
    python3 -c "import sys,json; d=json.load(sys.stdin); \
    print(d['metadata']['creationTimestamp'])" 2>/dev/null)

  CONTAINER_CREATION_TIMESTAMP=$(echo "$describe" | \
    python3 -c "import sys,json; d=json.load(sys.stdin); \
    print(d['status']['startTime'])" 2>/dev/null)

  STARTED_AT=$(echo "$describe" | \
    python3 -c "import sys,json; d=json.load(sys.stdin); \
    print(d['status']['containerStatuses'][0]['state']['terminated']['startedAt'])" 2>/dev/null)

  FINISHED_AT=$(echo "$describe" | \
    python3 -c "import sys,json; d=json.load(sys.stdin); \
    print(d['status']['containerStatuses'][0]['state']['terminated']['finishedAt'])" 2>/dev/null)

  SCHEDULED_AT=$(echo "$describe" | \
    python3 -c "
import sys, json
d = json.load(sys.stdin)
conditions = d.get('status', {}).get('conditions', [])
for c in conditions:
    if c.get('type') == 'PodScheduled':
        print(c.get('lastTransitionTime', ''))
        break
" 2>/dev/null)
}

# ─── Fungsi: append baris ke CSV ─────────────────────────────────────────────

append_csv() {
  local id=$1
  local attempt_id=$2
  local job_name=$3
  local pod_name=$4
  local size=$5
  local fill_a=$6
  local fill_b=$7

  python3 - <<PYEOF
import csv, os

csv_file = "${CSV_FILE}"
fieldnames = "${CSV_HEADER}".split(',')

row = {
    'id':                           '${id}',
    'attempt_id':                   '${attempt_id}',
    'job_name':                     '${job_name}',
    'pod_name':                     '${pod_name}',
    'size':                         '${size}',
    'fill_a':                       '${fill_a}',
    'fill_b':                       '${fill_b}',
    'logs_end':                     '${LOG_END}',
    'logs_end_time':                '${LOG_END_TIME}',
    'logs_start':                   '${LOG_START}',
    'logs_start_time':              '${LOG_START_TIME}',
    'logs_wall_cs':                 '${LOG_WALL_CS}',
    'logs_wall_us':                 '${LOG_WALL_US}',
    'logs_cpu_usage':               '${LOG_CPU}',
    'pod_creation_timestamp':       '${POD_CREATION_TIMESTAMP}',
    'container_creation_timestamp': '${CONTAINER_CREATION_TIMESTAMP}',
    'started_at':                   '${STARTED_AT}',
    'finished_at':                  '${FINISHED_AT}',
    'scheduled_at':                 '${SCHEDULED_AT}',
}

file_exists = os.path.isfile(csv_file)
with open(csv_file, 'a', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writerow(row)

print(f"  CSV diupdate: id=${id}, attempt_id=${attempt_id}")
PYEOF
}

# ─── Fungsi: hapus job ────────────────────────────────────────────────────────

hapus_job() {
  local job_name=$1
  echo "  Menghapus job ${job_name}..."
  kubectl delete job "$job_name" --ignore-not-found=true > /dev/null 2>&1
  echo "  Job dihapus."
}

# ─── Fungsi utama: jalankan satu job ─────────────────────────────────────────

jalankan_job() {
  local yaml_file=$1
  local attempt_id=$2

  # Ambil nama file tanpa path dan ekstensi
  local filename
  filename=$(basename "$yaml_file" .yaml)

  # Ambil ID numerik dari nama file (e.g. default-matrix-jobs-82 → 82)
  local id
  id=$(echo "$filename" | grep -oP '\d+$')

  # Job name = nama file (tanpa .yaml)
  local job_name="$filename"

  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  [ID=${id} | Attempt=${attempt_id}] ${job_name}"
  echo "  YAML: ${yaml_file}"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # Parse size dan fill dari YAML
  parse_yaml_args "$yaml_file"
  echo "  Size: ${YAML_SIZE}, fill_a: ${YAML_FILL_A}, fill_b: ${YAML_FILL_B}"

  # 1. Apply YAML
  echo "  Menjalankan kubectl apply..."
  kubectl apply -f "$yaml_file"
  if [ $? -ne 0 ]; then
    echo "  ERROR: kubectl apply gagal."
    return 1
  fi

  # 2. Tunggu pod selesai
  tunggu_pod_selesai "$job_name"
  if [ $? -ne 0 ]; then
    hapus_job "$job_name"
    return 1
  fi

  # 3. Ambil pod name
  POD_NAME=$(ambil_pod_name "$job_name")
  if [ -z "$POD_NAME" ]; then
    echo "  ERROR: Tidak bisa ambil pod name."
    hapus_job "$job_name"
    return 1
  fi
  echo "  Pod name: $POD_NAME"

  # 4. Ambil logs
  LOGS=$(ambil_logs "$POD_NAME")
  echo "  Logs:"
  echo "$LOGS" | sed 's/^/    /'
  parse_logs "$LOGS"

  # 5. Ambil timestamps
  ambil_timestamps "$POD_NAME"
  echo "  pod_creation_timestamp       : $POD_CREATION_TIMESTAMP"
  echo "  container_creation_timestamp : $CONTAINER_CREATION_TIMESTAMP"
  echo "  started_at                   : $STARTED_AT"
  echo "  finished_at                  : $FINISHED_AT"
  echo "  scheduled_at                 : $SCHEDULED_AT"

  # 6. Append ke CSV
  append_csv "$id" "$attempt_id" "$job_name" "$POD_NAME" \
    "$YAML_SIZE" "$YAML_FILL_A" "$YAML_FILL_B"

  # 7. Hapus job
  hapus_job "$job_name"

  return 0
}

# ─── Daftar YAML dengan attempt count ────────────────────────────────────────
# Format: "nama_file.yaml:jumlah_attempt"
# ID 82 dijalanin 2 kali, sisanya 1 kali

declare -A ATTEMPT_COUNT
ATTEMPT_COUNT["default-matrix-jobs-82.yaml"]=2
# Semua file lain default = 1

# ─── Main ─────────────────────────────────────────────────────────────────────

echo "============================================================"
echo "  Duplicate Attempt CPU Job Runner"
echo "  YAML Dir : $YAML_DIR"
echo "  CSV      : $CSV_FILE"
echo "============================================================"

# Loop semua YAML di folder, diurutkan
for yaml_file in $(ls -v "$YAML_DIR"/*.yaml 2>/dev/null); do
  filename=$(basename "$yaml_file")

  # Tentukan berapa kali dijalanin
  max_attempt=${ATTEMPT_COUNT["$filename"]:-1}

  for (( attempt=1; attempt<=max_attempt; attempt++ )); do
    jalankan_job "$yaml_file" "$attempt"
  done
done

echo ""
echo "============================================================"
echo "  Selesai! Hasil tersimpan di $CSV_FILE"
echo "============================================================"