#!/bin/bash
# =============================================================================
# run_default.sh  —  Default scheduler, replay timing arrival dari EDF CSV
#
# Cara replay timing:
#   - Baca arrival_timestamp tiap job dari edf_*.csv
#   - Hitung offset[i] = arrival[i] - arrival[0]
#   - Catat start_epoch saat eksperimen default mulai
#   - Sebelum apply job ke-i: sleep sampai (start_epoch + offset[i]) tercapai
#
# Usage: bash run_default.sh [low|medium|high|all]   (default: all)
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# KONFIGURASI
# ---------------------------------------------------------------------------
JOBS_CSV="experiment_five.csv"
YAML_TEMPLATE="job.yaml"
NAMESPACE="default"
JOB_TIMEOUT=2000
POLL_INTERVAL=5
EDF_CSV_DIR="."
OUTPUT_DIR="."

CSV_HEADER="order,rho,ori_id,size,fill_a,fill_b,job_name,pod_name,arrival_timestamp,pod_creation_timestamp,container_creation_timestamp,started_at,finished_at,scheduled_at,queue_wait_seconds"

# ---------------------------------------------------------------------------
# Build lookup: ori_id -> "size,fill_a,fill_b,cpu_usage,max_runtime"
# ---------------------------------------------------------------------------
declare -A JOB_DATA

load_job_data() {
    local first=1
    while IFS=',' read -r ori_id job_name size fill_a fill_b cpu_usage max_runtime; do
        # Skip header baris pertama
        if [[ $first -eq 1 ]]; then
            first=0
            continue
        fi
        [[ -z "$ori_id" ]] && continue
        JOB_DATA["$ori_id"]="${size},${fill_a},${fill_b},${cpu_usage},${max_runtime}"
    done < "$JOBS_CSV"
    echo "  [INFO] Loaded ${#JOB_DATA[@]} job records dari $JOBS_CSV"
}

# ---------------------------------------------------------------------------
# Buat CSV dengan header (timpa kalau sudah ada)
# ---------------------------------------------------------------------------
ensure_csv() {
    local csv_path=$1
    echo "$CSV_HEADER" > "$csv_path"
    echo "  [CSV] Siap: $csv_path"
}

# ---------------------------------------------------------------------------
# Tunggu pod dari suatu job sampai phase=Succeeded atau timeout
# ---------------------------------------------------------------------------
wait_for_pod_completed() {
    local job_name=$1
    local elapsed=0

    while [[ $elapsed -lt $JOB_TIMEOUT ]]; do
        local phase
        phase=$(kubectl get pods -n "$NAMESPACE" \
            --selector="job-name=${job_name}" \
            --output=jsonpath='{.items[0].status.phase}' 2>/dev/null || echo "Unknown")

        if [[ "$phase" == "Succeeded" ]]; then
            return 0
        elif [[ "$phase" == "Failed" ]]; then
            echo "  [WARN] Pod untuk $job_name FAILED" >&2
            return 1
        fi

        sleep "$POLL_INTERVAL"
        elapsed=$((elapsed + POLL_INTERVAL))
    done

    echo "  [WARN] Timeout ${JOB_TIMEOUT}s menunggu $job_name" >&2
    return 1
}

# ---------------------------------------------------------------------------
# Ambil metrics dari kubectl get pod -o json
# Output (pipe-separated):
#   pod_creation_timestamp | container_creation_timestamp | started_at | finished_at | scheduled_at
# ---------------------------------------------------------------------------
get_pod_metrics() {
    local pod_name=$1

    local pod_json
    pod_json=$(kubectl get pod "$pod_name" -n "$NAMESPACE" -o json 2>/dev/null)

    if [[ -z "$pod_json" ]]; then
        echo "N/A|N/A|N/A|N/A|N/A"
        return
    fi

    echo "$pod_json" | python3 - <<'PYEOF'
import sys, json

d = json.load(sys.stdin)

# metadata.creationTimestamp -> pod_creation_timestamp
pod_creation = d["metadata"].get("creationTimestamp", "N/A")

# status.startTime -> container_creation_timestamp (waktu kubelet terima pod)
container_creation_timestamp = d.get("status", {}).get("startTime", "N/A")

# containerStatuses[0].state.terminated -> started_at, finished_at
try:
    term = d["status"]["containerStatuses"][0]["state"]["terminated"]
    started_at  = term.get("startedAt",  "N/A")
    finished_at = term.get("finishedAt", "N/A")
except (KeyError, IndexError):
    started_at  = "N/A"
    finished_at = "N/A"

# conditions[type=PodScheduled].lastTransitionTime -> scheduled_at
scheduled_at = "N/A"
for cond in d.get("status", {}).get("conditions", []):
    if cond.get("type") == "PodScheduled":
        scheduled_at = cond.get("lastTransitionTime", "N/A")
        break

print(f"{pod_creation}|{container_creation_timestamp}|{started_at}|{finished_at}|{scheduled_at}")
PYEOF
}

# ---------------------------------------------------------------------------
# Hitung queue_wait_seconds = pod_creation_epoch - arrival_epoch
# ---------------------------------------------------------------------------
calc_queue_wait() {
    local pod_creation_iso=$1
    local arrival_epoch=$2
    python3 - <<PYEOF
from datetime import datetime, timezone
try:
    ts   = datetime.fromisoformat("${pod_creation_iso}".replace("Z", "+00:00"))
    wait = ts.timestamp() - float("${arrival_epoch}")
    print(f"{wait:.3f}")
except Exception:
    print("N/A")
PYEOF
}

# ---------------------------------------------------------------------------
# Fungsi utama: satu skenario
# ---------------------------------------------------------------------------
run_scenario() {
    local rho_label=$1
    local edf_csv="${EDF_CSV_DIR}/edf_${rho_label}.csv"
    local out_csv="${OUTPUT_DIR}/default_${rho_label}.csv"

    if [[ ! -f "$edf_csv" ]]; then
        echo "[ERROR] EDF CSV tidak ditemukan: $edf_csv" >&2
        echo "         Jalankan run_edf.sh terlebih dahulu." >&2
        return 1
    fi

    echo "=============================================="
    echo " Skenario DEFAULT : rho=${rho_label}"
    echo " Replay timing dari: ${edf_csv}"
    echo " Output CSV         : ${out_csv}"
    echo "=============================================="

    ensure_csv "$out_csv"

    # ------------------------------------------------------------------
    # Baca EDF CSV -> simpan arrays untuk replay
    # Kolom EDF CSV:
    #   order,rho,ori_id,size,fill_a,fill_b,job_name,pod_name,
    #   arrival_timestamp,pod_creation_timestamp,container_creation_timestamp,
    #   started_at,finished_at,scheduled_at,queue_wait_seconds
    # ------------------------------------------------------------------
    declare -a E_ORDER E_ORI_ID E_SIZE E_FILL_A E_FILL_B E_JOB_NAME E_ARRIVAL E_OFFSET

    local idx=0
    local first_arrival=""

    while IFS=',' read -r \
        e_order e_rho e_ori_id e_size e_fill_a e_fill_b \
        e_job_name e_pod_name e_arrival _rest; do

        # Skip header
        [[ "$e_order" == "order" ]] && continue
        [[ -z "$e_order" ]]         && continue

        [[ -z "$first_arrival" ]] && first_arrival="$e_arrival"

        local offset
        offset=$(python3 -c "print(f'{float(\"$e_arrival\") - float(\"$first_arrival\"):.6f}')")

        E_ORDER[$idx]=$e_order
        E_ORI_ID[$idx]=$e_ori_id
        E_SIZE[$idx]=$e_size
        E_FILL_A[$idx]=$e_fill_a
        E_FILL_B[$idx]=$e_fill_b
        E_JOB_NAME[$idx]=$e_job_name
        E_ARRIVAL[$idx]=$e_arrival
        E_OFFSET[$idx]=$offset
        idx=$((idx+1))
    done < "$edf_csv"

    local total=${#E_ORDER[@]}
    echo "  Total job dari EDF CSV: ${total}"
    echo "  First arrival (EDF)   : ${first_arrival}"
    echo ""

    declare -a T_ORDER T_ORI_ID T_SIZE T_FILL_A T_FILL_B
    declare -a T_DEF_JOB_NAME T_POD_NAME T_ARRIVAL_DEF

    local start_epoch
    start_epoch=$(python3 -c "import time; print(f'{time.time():.6f}')")
    echo "  start_epoch default: ${start_epoch}"
    echo ""

    # ==================================================================
    # TAHAP 1: Apply job satu-satu dengan timing replay dari EDF
    # ==================================================================
    echo "--- TAHAP 1: Apply jobs (replay timing EDF) ---"
    for i in "${!E_ORDER[@]}"; do
        local ori_id="${E_ORI_ID[$i]}"
        local offset="${E_OFFSET[$i]}"

        # Hitung sisa sleep yang dibutuhkan
        local sleep_needed
        sleep_needed=$(python3 - <<PYEOF
import time
elapsed = time.time() - float("$start_epoch")
needed  = float("$offset") - elapsed
print(f"{max(0.0, needed):.3f}")
PYEOF
)

        # Sleep kalau masih perlu nunggu (> 50ms)
        if python3 -c "exit(0 if float('$sleep_needed') > 0.05 else 1)" 2>/dev/null; then
            echo "[${E_ORDER[$i]}/${total}] Menunggu ${sleep_needed}s (offset=${offset}s dari start)..."
            sleep "$sleep_needed"
        fi

        local arrival_epoch
        arrival_epoch=$(python3 -c "import time; print(f'{time.time():.6f}')")

        # Lookup data job dari JOBS_CSV berdasarkan ori_id
        local job_info="${JOB_DATA[$ori_id]:-}"
        if [[ -z "$job_info" ]]; then
            echo "  [WARN] ori_id=$ori_id tidak ada di $JOBS_CSV" >&2
            T_ORDER[$i]="${E_ORDER[$i]}"
            T_ORI_ID[$i]=$ori_id
            T_SIZE[$i]="${E_SIZE[$i]}"
            T_FILL_A[$i]="${E_FILL_A[$i]}"
            T_FILL_B[$i]="${E_FILL_B[$i]}"
            T_DEF_JOB_NAME[$i]="UNKNOWN"
            T_POD_NAME[$i]="NOT_FOUND"
            T_ARRIVAL_DEF[$i]=$arrival_epoch
            continue
        fi

        IFS=',' read -r size fill_a fill_b cpu_usage max_runtime <<< "$job_info"

        # Nama job default = nama EDF + "-def" supaya tidak collision
        local orig_job_name="${E_JOB_NAME[$i]}"
        local def_job_name="${orig_job_name}-def"

        local tmp_yaml
        tmp_yaml=$(mktemp /tmp/job_def_XXXXXX.yaml)
        sed \
            -e "s|<job_name>|${def_job_name}|g" \
            -e "s|<max_runtime>|${max_runtime}|g" \
            -e "s|<cpu_usage>|${cpu_usage}|g" \
            -e "s|<size>|${size}|g" \
            -e "s|<fill_a>|${fill_a}|g" \
            -e "s|<fill_b>|${fill_b}|g" \
            -e "s|schedulerName: deadline-aware-scheduler|schedulerName: default-scheduler|g" \
            "$YAML_TEMPLATE" > "$tmp_yaml"

        echo "[${E_ORDER[$i]}/${total}] APPLY ${def_job_name} | arrival=${arrival_epoch}"
        kubectl apply -f "$tmp_yaml" -n "$NAMESPACE"
        rm -f "$tmp_yaml"

        # Tunggu pod muncul (max 60s)
        local pod_name=""
        local wp=0
        while [[ -z "$pod_name" && $wp -lt 60 ]]; do
            pod_name=$(kubectl get pods -n "$NAMESPACE" \
                --selector="job-name=${def_job_name}" \
                --output=jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
            [[ -z "$pod_name" ]] && { sleep 2; wp=$((wp+2)); }
        done
        [[ -z "$pod_name" ]] && pod_name="NOT_FOUND"

        echo "  -> pod: ${pod_name}"

        T_ORDER[$i]="${E_ORDER[$i]}"
        T_ORI_ID[$i]=$ori_id
        T_SIZE[$i]=$size
        T_FILL_A[$i]=$fill_a
        T_FILL_B[$i]=$fill_b
        T_DEF_JOB_NAME[$i]=$def_job_name
        T_POD_NAME[$i]=$pod_name
        T_ARRIVAL_DEF[$i]=$arrival_epoch
    done

    # ==================================================================
    # TAHAP 2: Tunggu SEMUA pod Completed
    # ==================================================================
    echo ""
    echo "--- TAHAP 2: Menunggu semua pod Completed ---"
    for i in "${!T_DEF_JOB_NAME[@]}"; do
        local jname="${T_DEF_JOB_NAME[$i]}"
        local pname="${T_POD_NAME[$i]}"
        [[ "$jname" == "UNKNOWN" || "$pname" == "NOT_FOUND" ]] && continue
        echo "  [WAIT] $jname (pod: $pname) ..."
        wait_for_pod_completed "$jname" || true
        echo "  [DONE] $jname"
    done

    # ==================================================================
    # TAHAP 3: Ambil metrics + tulis CSV
    # ==================================================================
    echo ""
    echo "--- TAHAP 3: Ambil metrics dan tulis CSV ---"
    for i in "${!T_ORDER[@]}"; do
        local order="${T_ORDER[$i]}"
        local ori_id="${T_ORI_ID[$i]}"
        local size="${T_SIZE[$i]}"
        local fill_a="${T_FILL_A[$i]}"
        local fill_b="${T_FILL_B[$i]}"
        local def_job_name="${T_DEF_JOB_NAME[$i]}"
        local pod_name="${T_POD_NAME[$i]}"
        local arrival="${T_ARRIVAL_DEF[$i]}"

        echo "  [METRICS] $def_job_name / $pod_name"

        if [[ "$pod_name" == "NOT_FOUND" || "$def_job_name" == "UNKNOWN" ]]; then
            echo "${order},${rho_label},${ori_id},${size},${fill_a},${fill_b},${def_job_name},N/A,${arrival},N/A,N/A,N/A,N/A,N/A,N/A" >> "$out_csv"
            continue
        fi

        local metrics
        metrics=$(get_pod_metrics "$pod_name")
        IFS='|' read -r pod_created container_creation container_started finished_at scheduled_at <<< "$metrics"

        local queue_wait
        queue_wait=$(calc_queue_wait "$pod_created" "$arrival")

        echo "${order},${rho_label},${ori_id},${size},${fill_a},${fill_b},${def_job_name},${pod_name},${arrival},${pod_created},${container_creation},${container_started},${finished_at},${scheduled_at},${queue_wait}" >> "$out_csv"
        echo "  -> ditulis ke ${out_csv}"
    done

    unset T_ORDER T_ORI_ID T_SIZE T_FILL_A T_FILL_B T_DEF_JOB_NAME T_POD_NAME T_ARRIVAL_DEF
    unset E_ORDER E_ORI_ID E_SIZE E_FILL_A E_FILL_B E_JOB_NAME E_ARRIVAL E_OFFSET

    echo ""
    echo "Selesai default ${rho_label} -> ${out_csv}"
    echo ""
}

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
load_job_data

MODE="${1:-all}"
case "$MODE" in
    low)     run_scenario low ;;
    medium)  run_scenario medium ;;
    high)    run_scenario high ;;
    all)
        run_scenario low
        run_scenario medium
        run_scenario high
        ;;
    *)
        echo "Usage: $0 [low|medium|high|all]"
        exit 1
        ;;
esac

echo "=== run_default.sh selesai ==="
