#!/bin/bash
# =============================================================================
# jobs.sh
# Jalankan job Kubernetes dari CSV, 10 kali per job ID
# Simpan hasil ke CSV yang sama
#
# Cara pakai:
#   bash jobs.sh --type wcet                    → semua ID dari wcet
#   bash jobs.sh --type cpu                     → semua ID dari cpu
#   bash jobs.sh --type wcet --id 1             → ID 1 saja
#   bash jobs.sh --type cpu --id 1,2,5          → ID 1, 2, 5 saja
#   bash jobs.sh --type wcet --skip-id 3,4      → semua kecuali ID 3 dan 4
# =============================================================================

WAIT_TIMEOUT=1800   # maksimal tunggu pod selesai (detik) — 30 menit
POLL_INTERVAL=10    # cek status pod tiap berapa detik

# ─── Parse argumen ────────────────────────────────────────────────────────────

SELECTED_IDS=""
SKIP_IDS=""
CSV_TYPE=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --id)
      SELECTED_IDS="$2"
      shift 2
      ;;
    --skip-id)
      SKIP_IDS="$2"
      shift 2
      ;;
    --type)
      CSV_TYPE="$2"
      shift 2
      ;;
    *)
      echo "Argumen tidak dikenal: $1"
      echo "Cara pakai: bash run_jobs.sh --type wcet|cpu [--id 1,2] [--skip-id 3,4]"
      exit 1
      ;;
  esac
done

# --id dan --skip-id tidak bisa dipakai bersamaan
if [ -n "$SELECTED_IDS" ] && [ -n "$SKIP_IDS" ]; then
  echo "ERROR: --id dan --skip-id tidak bisa dipakai bersamaan."
  exit 1
fi

# Validasi --type
if [ -z "$CSV_TYPE" ]; then
  echo "ERROR: --type wajib diisi. Pilih: wcet atau cpu"
  echo "Cara pakai: bash run_jobs.sh --type wcet|cpu [--id 1]"
  exit 1
fi

if [ "$CSV_TYPE" = "wcet" ]; then
  CSV_FILE="job-variation-wcet.csv"
elif [ "$CSV_TYPE" = "cpu" ]; then
  CSV_FILE="job-variation-cpu.csv"
else
  echo "ERROR: --type tidak valid. Pilih: wcet atau cpu"
  exit 1
fi

# ─── Validasi ─────────────────────────────────────────────────────────────────

if [ ! -f "$CSV_FILE" ]; then
  echo "ERROR: File $CSV_FILE tidak ditemukan."
  exit 1
fi

# ─── Fungsi: buat YAML ────────────────────────────────────────────────────────

buat_yaml() {
  local job_name=$1
  local size=$2
  local fill_a=$3
  local fill_b=$4

  cat <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job_name}
spec:
  template:
    metadata:
      labels:
        app: matrix-mult
    spec:
      schedulerName: default-scheduler
      restartPolicy: Never
      containers:
      - name: matrix-mult
        image: evelynphs/deadline-aware-workload:matrix
        command: ["/bin/sh", "-c"]
        args:
        - |
          START=\$(cat /sys/fs/cgroup/cpu.stat | grep usage_usec | awk '{print \$2}')
          START_TIME=\$(awk '{print \$1}' /proc/uptime | tr -d '.')

          python3 /app/matrix_mult.py ${size} ${fill_a} ${fill_b}

          END=\$(cat /sys/fs/cgroup/cpu.stat | grep usage_usec | awk '{print \$2}')
          END_TIME=\$(awk '{print \$1}' /proc/uptime | tr -d '.')

          CPU_US=\$((END - START))
          WALL_CS=\$((END_TIME - START_TIME))
          WALL_US=\$((WALL_CS * 10000))
          MILLICORES=\$(( (CPU_US * 1000) / WALL_US ))
          echo "END: \${END}"
          echo "END_TIME: \${END_TIME}"
          echo "START: \${START}"
          echo "START_TIME: \${START_TIME}"
          echo "WALL_CS: \${WALL_CS}"
          echo "WALL_US: \${WALL_US}"
          echo "CPU usage: \${MILLICORES}m"
EOF
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

# ─── Fungsi: ambil timestamps dari kubectl describe ───────────────────────────

ambil_timestamps() {
  local pod_name=$1
  local describe
  describe=$(kubectl get pod "$pod_name" -o json 2>/dev/null)

  # pod_creation_timestamp = metadata.creationTimestamp
  POD_CREATION_TIMESTAMP=$(echo "$describe" | \
    python3 -c "import sys,json; d=json.load(sys.stdin); \
    print(d['metadata']['creationTimestamp'])" 2>/dev/null)

  # container_creation_timestamp = status.startTime (paling bawah di JSON)
  CONTAINER_CREATION_TIMESTAMP=$(echo "$describe" | \
    python3 -c "import sys,json; d=json.load(sys.stdin); \
    print(d['status']['startTime'])" 2>/dev/null)

  # started_at = waktu container mulai jalan (terminated.startedAt)
  STARTED_AT=$(echo "$describe" | \
    python3 -c "import sys,json; d=json.load(sys.stdin); \
    print(d['status']['containerStatuses'][0]['state']['terminated']['startedAt'])" 2>/dev/null)

  # finished_at = waktu container selesai (terminated.finishedAt)
  FINISHED_AT=$(echo "$describe" | \
    python3 -c "import sys,json; d=json.load(sys.stdin); \
    print(d['status']['containerStatuses'][0]['state']['terminated']['finishedAt'])" 2>/dev/null)

  # scheduled_at = waktu pod dijadwalkan ke node (kondisi PodScheduled)
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

# ─── Fungsi: update baris CSV ─────────────────────────────────────────────────

update_csv() {
  local id=$1
  local trial_id=$2
  local job_name=$3
  local pod_name=$4

  # Pakai python3 untuk update CSV supaya aman (tidak rusak format)
  python3 - <<PYEOF
import csv, sys, os

csv_file = "${CSV_FILE}"
rows = []

with open(csv_file, newline='') as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames
    for row in reader:
        if row['id'].strip() == '${id}' and row['trial_id'].strip() == '${trial_id}':
            row['job_name']                    = '${job_name}'
            row['pod_name']                    = '${pod_name}'
            row['logs_end']                    = '${LOG_END}'
            row['logs_end_time']               = '${LOG_END_TIME}'
            row['logs_start']                  = '${LOG_START}'
            row['logs_start_time']             = '${LOG_START_TIME}'
            row['logs_wall_cs']                = '${LOG_WALL_CS}'
            row['logs_wall_us']                = '${LOG_WALL_US}'
            row['logs_cpu_usage']              = '${LOG_CPU}'
            row['pod_creation_timestamp']      = '${POD_CREATION_TIMESTAMP}'
            row['container_creation_timestamp']= '${CONTAINER_CREATION_TIMESTAMP}'
            row['started_at']                  = '${STARTED_AT}'
            row['finished_at']                 = '${FINISHED_AT}'
            row['scheduled_at']                = '${SCHEDULED_AT}'
        rows.append(row)

tmp_file = csv_file + '.tmp'
with open(tmp_file, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

os.replace(tmp_file, csv_file)
print("  CSV diupdate: id=${id}, trial_id=${trial_id}")
PYEOF
}

# ─── Fungsi: hapus job ────────────────────────────────────────────────────────

hapus_job() {
  local job_name=$1
  echo "  Menghapus job ${job_name}..."
  kubectl delete job "$job_name" --ignore-not-found=true > /dev/null 2>&1
  echo "  Job dihapus."
}

# ─── Fungsi utama: jalankan satu trial ───────────────────────────────────────

jalankan_trial() {
  local id=$1
  local trial_id=$2
  local size=$3
  local fill_a=$4
  local fill_b=$5

  local job_name="job-matrix-mult-default-${id}"
  local yaml_file="/tmp/${job_name}.yaml"

  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  [ID=${id} | Trial=${trial_id}] ${job_name}"
  echo "  Matrix: ${size}x${size}, fill_a=${fill_a}, fill_b=${fill_b}"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # 1. Buat YAML
  buat_yaml "$job_name" "$size" "$fill_a" "$fill_b" > "$yaml_file"

  # 2. Apply YAML
  echo "  Menjalankan kubectl apply..."
  kubectl apply -f "$yaml_file"
  if [ $? -ne 0 ]; then
    echo "  ERROR: kubectl apply gagal."
    rm -f "$yaml_file"
    return 1
  fi

  # 3. Tunggu pod selesai
  tunggu_pod_selesai "$job_name"
  if [ $? -ne 0 ]; then
    hapus_job "$job_name"
    rm -f "$yaml_file"
    return 1
  fi

  # 4. Ambil pod name
  POD_NAME=$(ambil_pod_name "$job_name")
  if [ -z "$POD_NAME" ]; then
    echo "  ERROR: Tidak bisa ambil pod name."
    hapus_job "$job_name"
    rm -f "$yaml_file"
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

  # 7. Update CSV
  update_csv "$id" "$trial_id" "$job_name" "$POD_NAME"

  # 8. Hapus job
  hapus_job "$job_name"

  # 9. Cleanup YAML
  rm -f "$yaml_file"

  return 0
}

# ─── Main: baca CSV dan jalankan ─────────────────────────────────────────────

echo "============================================================"
echo "  Kubernetes Job Runner"
echo "  Type: $CSV_TYPE"
echo "  CSV : $CSV_FILE"
if [ -n "$SELECTED_IDS" ]; then
  echo "  Mode: ID terpilih = $SELECTED_IDS"
elif [ -n "$SKIP_IDS" ]; then
  echo "  Mode: semua ID kecuali = $SKIP_IDS"
else
  echo "  Mode: semua ID"
fi
echo "============================================================"

# Baca CSV, skip header
# Format: id,trial_id,size,fill_a,fill_b,...
{
  read  # skip header
  while IFS=',' read -r id trial_id size fill_a fill_b rest; do

    # Bersihkan whitespace/carriage return
    id=$(echo "$id" | tr -d '[:space:]')
    trial_id=$(echo "$trial_id" | tr -d '[:space:]')
    size=$(echo "$size" | tr -d '[:space:]')
    fill_a=$(echo "$fill_a" | tr -d '[:space:]')
    fill_b=$(echo "$fill_b" | tr -d '[:space:]')

    # Skip baris kosong
    [ -z "$id" ] && continue

    # Filter ID kalau --id diisi
    if [ -n "$SELECTED_IDS" ]; then
      MATCH=0
      IFS=',' read -ra ID_LIST <<< "$SELECTED_IDS"
      for sel_id in "${ID_LIST[@]}"; do
        sel_id=$(echo "$sel_id" | tr -d '[:space:]')
        if [ "$id" = "$sel_id" ]; then
          MATCH=1
          break
        fi
      done
      [ $MATCH -eq 0 ] && continue
    fi

    # Skip ID kalau --skip-id diisi
    if [ -n "$SKIP_IDS" ]; then
      SKIP=0
      IFS=',' read -ra SKIP_LIST <<< "$SKIP_IDS"
      for skip_id in "${SKIP_LIST[@]}"; do
        skip_id=$(echo "$skip_id" | tr -d '[:space:]')
        if [ "$id" = "$skip_id" ]; then
          SKIP=1
          break
        fi
      done
      [ $SKIP -eq 1 ] && continue
    fi

    jalankan_trial "$id" "$trial_id" "$size" "$fill_a" "$fill_b"

  done
} < "$CSV_FILE"

echo ""
echo "============================================================"
echo "  Selesai! Hasil tersimpan di $CSV_FILE"
echo "============================================================"