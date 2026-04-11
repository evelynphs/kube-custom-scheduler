#!/bin/bash
# =============================================================================
# run_edf.sh
# Jalanin EDF scheduler — apply semua job langsung (tanpa jeda Poisson).
# Tujuan utama: catat URUTAN dan ARRIVAL TIMESTAMP tiap job,
# supaya run_default.sh bisa replay timing yang sama persis.
#
# Alur:
#   1. Buat output CSV kalau belum ada
#   2. Apply semua job satu per satu, catat arrival_timestamp real-time
#   3. Tunggu SEMUA pod berstatus Completed (phase=Succeeded)
#   4. Ambil metrics dari kubectl get pod -o json untuk semua pod
#   5. Tulis hasil ke CSV
#
# Field yang diambil dari kubectl get pod -o json:
#   - metadata.creationTimestamp                              -> pod_creation_timestamp
#   - status.startTime                                        -> container_creation_timestamp
#   - status.containerStatuses[0].state.terminated.startedAt  -> started_at
#   - status.containerStatuses[0].state.terminated.finishedAt -> finished_at
#   - status.conditions[type=PodScheduled].lastTransitionTime -> scheduled_at
#
# Dependensi : kubectl, python3
# Usage      : bash run_edf.sh [low|medium|high|all]   (default: all)
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# KONFIGURASI
# ---------------------------------------------------------------------------
# JOBS_CSV="experiment.csv"
JOBS_CSV="experiment_five.csv"
YAML_TEMPLATE="job.yaml"
NAMESPACE="default"
JOB_TIMEOUT=2000
POLL_INTERVAL=5
OUTPUT_DIR="."

declare -A RHO_MAP
RHO_MAP[low]=0.5
RHO_MAP[medium]=0.75
RHO_MAP[high]=0.95

# Kolom:
#   arrival_timestamp   = Unix epoch float saat kubectl apply dipanggil
#   pod_creation_timestamp = metadata.creationTimestamp
#   container_creation_timestamp      = status.startTime (waktu pod diterima kubelet)
#   started_at          = containerStatuses[0].terminated.startedAt
#   finished_at         = containerStatuses[0].terminated.finishedAt
#   scheduled_at        = conditions[PodScheduled].lastTransitionTime
#   queue_wait_seconds  = pod_creation_timestamp - arrival_timestamp
CSV_HEADER="order,rho,ori_id,size,fill_a,fill_b,job_name,pod_name,arrival_timestamp,pod_creation_timestamp,container_creation_timestamp,started_at,finished_at,scheduled_at,queue_wait_seconds"

# ---------------------------------------------------------------------------
# Buat CSV dengan header kalau belum ada (atau timpa kalau sudah ada)
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
#   pod_creation | pod_start_time | container_started_at | finished_at | scheduled_at
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

pod_creation = d["metadata"].get("creationTimestamp", "N/A")

container_creation_timestamp = d.get("status", {}).get("startTime", "N/A")

try:
    term = d["status"]["containerStatuses"][0]["state"]["terminated"]
    started_at = term.get("startedAt", "N/A")
    finished_at          = term.get("finishedAt", "N/A")
except (KeyError, IndexError):
    container_started_at = "N/A"
    finished_at          = "N/A"

scheduled_at = "N/A"
for cond in d.get("status", {}).get("conditions", []):
    if cond.get("type") == "PodScheduled":
        scheduled_at = cond.get("lastTransitionTime", "N/A")
        break

print(f"{pod_creation}|{pod_start_time}|{container_started_at}|{finished_at}|{scheduled_at}")
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
    ts   = datetime.fromisoformat("$pod_creation_iso".replace("Z", "+00:00"))
    wait = ts.timestamp() - float("$arrival_epoch")
    print(f"{wait:.3f}")
except Exception:
    print("N/A")
PYEOF
}

# ---------------------------------------------------------------------------
# Fungsi utama: satu skenario rho
# ---------------------------------------------------------------------------
run_scenario() {
    local rho_label=$1
    local rho_val=${RHO_MAP[$rho_label]}
    local out_csv="${OUTPUT_DIR}/edf_${rho_label}.csv"

    echo "=============================================="
    echo " Skenario EDF  : rho=${rho_label} (rho=${rho_val})"
    echo " Output CSV    : ${out_csv}"
    echo "=============================================="

    ensure_csv "$out_csv"

    mapfile -t JOB_LINES < <(tail -n +2 "$JOBS_CSV")
    local total=${#JOB_LINES[@]}

    declare -a T_ORDER T_ORI_ID T_SIZE T_FILL_A T_FILL_B
    declare -a T_JOB_NAME T_POD_NAME T_ARRIVAL

    # ==================================================================
    # TAHAP 1: Apply semua job langsung, catat arrival_timestamp
    # ==================================================================
    echo ""
    echo "--- TAHAP 1: Apply jobs ---"
    for i in "${!JOB_LINES[@]}"; do
        local line="${JOB_LINES[$i]}"
        IFS=',' read -r ori_id job_name size fill_a fill_b cpu_usage max_runtime <<< "$line"
        local order=$((i + 1))

        local arrival_epoch
        arrival_epoch=$(python3 -c "import time; print(f'{time.time():.6f}')")

        local tmp_yaml
        tmp_yaml=$(mktemp /tmp/job_edf_XXXXXX.yaml)
        sed \
            -e "s|<job_name>|${job_name}|g" \
            -e "s|<max_runtime>|${max_runtime}|g" \
            -e "s|<cpu_usage>|${cpu_usage}|g" \
            -e "s|<size>|${size}|g" \
            -e "s|<fill_a>|${fill_a}|g" \
            -e "s|<fill_b>|${fill_b}|g" \
            "$YAML_TEMPLATE" > "$tmp_yaml"

        echo "[${order}/${total}] APPLY ${job_name} | arrival=${arrival_epoch}"
        kubectl apply -f "$tmp_yaml" -n "$NAMESPACE"
        rm -f "$tmp_yaml"

        local pod_name=""
        local wp=0
        while [[ -z "$pod_name" && $wp -lt 60 ]]; do
            pod_name=$(kubectl get pods -n "$NAMESPACE" \
                --selector="job-name=${job_name}" \
                --output=jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
            [[ -z "$pod_name" ]] && { sleep 2; wp=$((wp+2)); }
        done
        [[ -z "$pod_name" ]] && pod_name="NOT_FOUND"

        echo "  -> pod: ${pod_name}"

        T_ORDER[$i]=$order
        T_ORI_ID[$i]=$ori_id
        T_SIZE[$i]=$size
        T_FILL_A[$i]=$fill_a
        T_FILL_B[$i]=$fill_b
        T_JOB_NAME[$i]=$job_name
        T_POD_NAME[$i]=$pod_name
        T_ARRIVAL[$i]=$arrival_epoch
    done

    # ==================================================================
    # TAHAP 2: Tunggu SEMUA pod Completed
    # ==================================================================
    echo ""
    echo "--- TAHAP 2: Menunggu semua pod Completed ---"
    for i in "${!T_JOB_NAME[@]}"; do
        local jname="${T_JOB_NAME[$i]}"
        local pname="${T_POD_NAME[$i]}"
        [[ "$pname" == "NOT_FOUND" ]] && { echo "  [SKIP] $jname — pod tidak ditemukan"; continue; }
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
        local job_name="${T_JOB_NAME[$i]}"
        local pod_name="${T_POD_NAME[$i]}"
        local arrival="${T_ARRIVAL[$i]}"

        echo "  [METRICS] $job_name / $pod_name"

        if [[ "$pod_name" == "NOT_FOUND" ]]; then
            echo "${order},${rho_label},${ori_id},${size},${fill_a},${fill_b},${job_name},N/A,${arrival},N/A,N/A,N/A,N/A,N/A,N/A" >> "$out_csv"
            continue
        fi

        local metrics
        metrics=$(get_pod_metrics "$pod_name")
        IFS='|' read -r pod_created pod_start container_started finished_at scheduled_at <<< "$metrics"

        local queue_wait
        queue_wait=$(calc_queue_wait "$pod_created" "$arrival")

        echo "${order},${rho_label},${ori_id},${size},${fill_a},${fill_b},${job_name},${pod_name},${arrival},${pod_created},${pod_start},${container_started},${finished_at},${scheduled_at},${queue_wait}" >> "$out_csv"
        echo "  -> ditulis ke ${out_csv}"
    done

    unset T_ORDER T_ORI_ID T_SIZE T_FILL_A T_FILL_B T_JOB_NAME T_POD_NAME T_ARRIVAL

    echo ""
    echo "Selesai EDF ${rho_label} -> ${out_csv}"
    echo ""
}

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
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

echo "=== run_edf.sh selesai ==="