#!/bin/bash
# =============================================================================
# run_duplicate_attempt_cpu.sh
# Jalankan job Kubernetes dari YAML di folder CURRENT
# Ambil metrics, simpan ke duplicate-attempt-cpu.csv
#
# Cara pakai:
#   bash run_duplicate_attempt_cpu.sh
# =============================================================================

WAIT_TIMEOUT=1800
POLL_INTERVAL=10
YAML_DIR="."
CSV_FILE="duplicate-attempt-cpu.csv"

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

# ─── Fungsi: ambil job name dari YAML ─────────────────────────────────────────

ambil_job_name_dari_yaml() {
  local yaml_file=$1
  # Ambil metadata.name dari YAML
  local job_name
  job_name=$(grep -E "^  name:" "$yaml_file" | head -1 | awk '{print $2}')
  echo "$job_name"
}

# ─── Fungsi: parse size dan fill dari YAML ───────────────────────────────────

parse_yaml_args() {
  local yaml_file=$1
  
  # Baca command dari YAML (format: command: ["/bin/sh", "-c"])
  local cmd_line
  cmd_line=$(grep -A1 "command:" "$yaml_file" | grep -E 'matrix_mult\.py' | head -1)
  
  if [ -z "$cmd_line" ]; then
    # Coba format args
    cmd_line=$(grep -A5 "matrix_mult.py" "$yaml_file" | grep -E 'matrix_mult\.py' | head -1)
  fi
  
  # Parse size, fill_a, fill_b
  YAML_SIZE=$(echo "$cmd_line" | grep -oP 'matrix_mult\.py\s+\K\d+')
  YAML_FILL_A=$(echo "$cmd_line" | grep -oP 'matrix_mult\.py\s+\d+\s+\K\S+')
  YAML_FILL_B=$(echo "$cmd_line" | grep -oP 'matrix_mult\.py\s+\d+\s+\S+\s+\K\S+')
  
  # Fallback: cari di args section
  if [ -z "$YAML_SIZE" ]; then
    local args_section
    args_section=$(awk '/args:/{flag=1} flag && /- /{print; if(/\]/ || /\]\]/) exit}' "$yaml_file" | tr -d ' -')
    YAML_SIZE=$(echo "$args_section" | grep -oP 'matrix_mult\.py\s+\K\d+')
    YAML_FILL_A=$(echo "$args_section" | grep -oP 'matrix_mult\.py\s+\d+\s+\K\S+')
    YAML_FILL_B=$(echo "$args_section" | grep -oP 'matrix_mult\.py\s+\d+\s+\S+\s+\K\S+')
  fi
  
  echo "  Parsed: size=$YAML_SIZE, fill_a=$YAML_FILL_A, fill_b=$YAML_FILL_B"
}

# ─── Fungsi: tunggu pod selesai ───────────────────────────────────────────────

tunggu_pod_selesai() {
  local job_name=$1
  local elapsed=0

  echo "  Menunggu pod selesai..."

  while [ $elapsed -lt $WAIT_TIMEOUT ]; do
    # Ambil semua pods untuk job ini
    PODS=$(kubectl get pods --selector=job-name=${job_name} --no-headers 2>/dev/null)
    
    if [ -z "$PODS" ]; then
      echo "  Belum ada pod yang tercipta... (${elapsed}s)"
      sleep $POLL_INTERVAL
      elapsed=$((elapsed + POLL_INTERVAL))
      continue
    fi
    
    # Cek status semua pods
    COMPLETED=0
    FAILED=0
    
    while IFS= read -r line; do
      STATUS=$(echo "$line" | awk '{print $3}')
      if [ "$STATUS" = "Completed" ]; then
        COMPLETED=1
      elif [ "$STATUS" = "Error" ] || [ "$STATUS" = "OOMKilled" ] || [ "$STATUS" = "CrashLoopBackOff" ] || [ "$STATUS" = "Failed" ]; then
        FAILED=1
        echo "  ERROR: Pod gagal dengan status $STATUS"
      fi
    done <<< "$PODS"
    
    if [ $COMPLETED -eq 1 ]; then
      echo "  Pod selesai (${elapsed}s)"
      return 0
    elif [ $FAILED -eq 1 ]; then
      return 1
    fi

    sleep $POLL_INTERVAL
    elapsed=$((elapsed + POLL_INTERVAL))
    FIRST_POD_STATUS=$(echo "$PODS" | head -1 | awk '{print $3}')
    echo "  Masih berjalan... (${elapsed}s, status: ${FIRST_POD_STATUS})"
  done

  echo "  TIMEOUT: Pod tidak selesai dalam ${WAIT_TIMEOUT} detik"
  return 1
}

# ─── Fungsi: ambil pod name (yang completed) ─────────────────────────────────

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

# ─── Fungsi: append ke CSV ─────────────────────────────────────────────────

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

with open(csv_file, 'a', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writerow(row)

print(f"  CSV diupdate: id={id}, attempt_id={attempt_id}")
PYEOF
}

# ─── Fungsi: hapus job ────────────────────────────────────────────────────────

hapus_job() {
  local job_name=$1
  echo "  Menghapus job ${job_name}..."
  kubectl delete job "$job_name" --ignore-not-found=true > /dev/null 2>&1
  
  # Tunggu sampai benar-benar terhapus
  local elapsed=0
  while [ $elapsed -lt 60 ]; do
    if ! kubectl get job "$job_name" --no-headers 2>/dev/null | grep -q .; then
      echo "  Job berhasil dihapus."
      return 0
    fi
    sleep 2
    elapsed=$((elapsed + 2))
  done
  echo "  Warning: Job mungkin masih dalam proses penghapusan."
}

# ─── Fungsi utama: jalankan satu job ─────────────────────────────────────────

jalankan_job() {
  local yaml_file=$1
  local attempt_id=$2

  # Ambil ID numerik dari nama file (e.g. default-matrix-jobs-82 → 82)
  local id
  id=$(basename "$yaml_file" .yaml | grep -oP '\d+$')
  
  # Ambil job name dari YAML
  local job_name
  job_name=$(ambil_job_name_dari_yaml "$yaml_file")
  
  if [ -z "$job_name" ]; then
    echo "  ERROR: Tidak bisa ambil job name dari YAML"
    return 1
  fi

  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  [ID=${id} | Attempt=${attempt_id}] ${job_name}"
  echo "  YAML: ${yaml_file}"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # Parse size dan fill dari YAML
  parse_yaml_args "$yaml_file"
  
  # Validasi hasil parsing
  if [ -z "$YAML_SIZE" ] || [ -z "$YAML_FILL_A" ] || [ -z "$YAML_FILL_B" ]; then
    echo "  ERROR: Gagal parse size/fill dari YAML"
    echo "  Coba baca file YAML untuk debug:"
    cat "$yaml_file" | grep -A5 -B5 "matrix_mult.py"
    return 1
  fi
  
  echo "  Size: ${YAML_SIZE}, fill_a: ${YAML_FILL_A}, fill_b: ${YAML_FILL_B}"

  # 1. Hapus job lama jika ada (cleanup)
  kubectl delete job "$job_name" --ignore-not-found=true > /dev/null 2>&1
  sleep 2
  
  # 2. Apply YAML
  echo "  Menjalankan kubectl apply..."
  kubectl apply -f "$yaml_file"
  if [ $? -ne 0 ]; then
    echo "  ERROR: kubectl apply gagal."
    return 1
  fi
  
  # Beri waktu sebentar untuk job tercreate
  sleep 2

  # 3. Tunggu pod selesai
  tunggu_pod_selesai "$job_name"
  if [ $? -ne 0 ]; then
    hapus_job "$job_name"
    return 1
  fi

  # 4. Ambil pod name
  POD_NAME=$(ambil_pod_name "$job_name")
  if [ -z "$POD_NAME" ]; then
    echo "  ERROR: Tidak bisa ambil pod name."
    hapus_job "$job_name"
    return 1
  fi
  echo "  Pod name: $POD_NAME"

  # 5. Ambil logs
  LOGS=$(ambil_logs "$POD_NAME")
  echo "  Logs:"
  echo "$LOGS" | sed 's/^/    /'
  parse_logs "$LOGS"

  # 6. Ambil timestamps
  ambil_timestamps "$POD_NAME"
  echo "  pod_creation_timestamp       : $POD_CREATION_TIMESTAMP"
  echo "  container_creation_timestamp : $CONTAINER_CREATION_TIMESTAMP"
  echo "  started_at                   : $STARTED_AT"
  echo "  finished_at                  : $FINISHED_AT"
  echo "  scheduled_at                 : $SCHEDULED_AT"

  # 7. Append ke CSV
  append_csv "$id" "$attempt_id" "$job_name" "$POD_NAME" \
    "$YAML_SIZE" "$YAML_FILL_A" "$YAML_FILL_B"

  # 8. Hapus job
  hapus_job "$job_name"

  return 0
}

# ─── Main ─────────────────────────────────────────────────────────────────────

echo "============================================================"
echo "  Duplicate Attempt CPU Job Runner"
echo "  YAML Dir : $YAML_DIR"
echo "  CSV      : $CSV_FILE"
echo "============================================================"

# Loop semua YAML di folder, diurutkan
YAML_FILES=($(ls -v "$YAML_DIR"/*.yaml 2>/dev/null))
if [ ${#YAML_FILES[@]} -eq 0 ]; then
  echo "ERROR: Tidak ada file YAML ditemukan di $YAML_DIR"
  exit 1
fi

for yaml_file in "${YAML_FILES[@]}"; do
  filename=$(basename "$yaml_file")
  
  # Tentukan berapa kali dijalankan berdasarkan ID
  id=$(echo "$filename" | grep -oP '\d+$')
  
  if [ "$id" = "82" ]; then
    max_attempt=2
  else
    max_attempt=1
  fi
  
  echo ""
  echo ">>> Memproses $filename (ID=$id, akan dijalankan $max_attempt kali)"
  
  for (( attempt=1; attempt<=max_attempt; attempt++ )); do
    jalankan_job "$yaml_file" "$attempt"
    
    # Tunggu sebentar antar attempt biar resources bersih
    if [ $attempt -lt $max_attempt ]; then
      echo "  Menunggu 5 detik sebelum attempt ke-$((attempt+1))..."
      sleep 5
    fi
  done
done

echo ""
echo "============================================================"
echo "  Selesai! Hasil tersimpan di $CSV_FILE"
echo "============================================================"