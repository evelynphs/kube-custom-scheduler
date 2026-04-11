#!/bin/bash
# =============================================================================
# run_default.sh
# Default scheduler dengan timing arrival replay dari edf_*.csv.
# Tiap job punya background watcher — begitu pod Succeeded, langsung
# ambil metrics dan tulis ke file sementara /tmp/metrics_def_<i>.txt.
# CSV ditulis sesuai urutan apply (= urutan di EDF CSV).
#
# Usage: bash run_default.sh [low|medium|high|all]   (default: all)
# =============================================================================

set -euo pipefail

JOBS_CSV="experiment_five.csv"
YAML_TEMPLATE="job.yaml"
NAMESPACE="default"
JOB_TIMEOUT=2000
POLL_INTERVAL=5
EDF_CSV_DIR="."
OUTPUT_DIR="."

CSV_HEADER="order,rho,ori_id,size,fill_a,fill_b,job_name,pod_name,arrival_timestamp,pod_creation_timestamp,pod_start_time,container_started_at,finished_at,scheduled_at,queue_wait_seconds"

# ---------------------------------------------------------------------------
# Build lookup: ori_id -> size,fill_a,fill_b,cpu_usage,max_runtime
# ---------------------------------------------------------------------------
declare -A JOB_DATA

load_job_data() {
    while IFS=',' read -r ori_id job_name size fill_a fill_b cpu_usage max_runtime; do
        [[ "$ori_id" == "ID" ]] && continue
        JOB_DATA["$ori_id"]="${size},${fill_a},${fill_b},${cpu_usage},${max_runtime}"
    done < "$JOBS_CSV"
    echo "  [INFO] Loaded ${#JOB_DATA[@]} records dari $JOBS_CSV"
}

ensure_csv() {
    echo "$CSV_HEADER" > "$1"
    echo "  [CSV] Siap: $1"
}

# ---------------------------------------------------------------------------
# Ambil satu field jsonpath, retry sampai non-kosong
# ---------------------------------------------------------------------------
get_field() {
    local pod_name=$1
    local jpath=$2
    local max_retries=${3:-1}
    local val=""
    local attempt=0

    while [[ $attempt -lt $max_retries ]]; do
        val=$(kubectl get pod "$pod_name" -n "$NAMESPACE" \
            -o jsonpath="$jpath" 2>/dev/null || echo "")
        [[ -n "$val" ]] && echo "$val" && return 0
        sleep 3
        attempt=$((attempt + 1))
    done

    echo "N/A"
}

# ---------------------------------------------------------------------------
# Hitung queue_wait_seconds (pure bash, date -d)
# ---------------------------------------------------------------------------
calc_queue_wait() {
    local pod_creation_iso=$1
    local arrival_ts=$2

    [[ "$pod_creation_iso" == "N/A" ]] && echo "N/A" && return
    [[ "$arrival_ts" == "N/A" ]] && echo "N/A" && return

    local pod_epoch arrival_epoch wait_s sign=""
    pod_epoch=$(date -d "$pod_creation_iso" +%s 2>/dev/null || echo "")
    arrival_epoch=$(date -d "$arrival_ts" +%s 2>/dev/null || echo "")

    [[ -z "$pod_epoch" || -z "$arrival_epoch" ]] && echo "N/A" && return

    wait_s=$(( pod_epoch - arrival_epoch ))
    [[ $wait_s -lt 0 ]] && sign="-" && wait_s=$(( -wait_s ))
    printf "%s%d\n" "$sign" "$wait_s"
}

# ---------------------------------------------------------------------------
# Background watcher: poll Succeeded, ambil metrics, tulis tmp file
# ---------------------------------------------------------------------------
watch_job() {
    local idx=$1
    local job_name=$2
    local pod_name=$3
    local arrival_epoch=$4
    local tmp_file="/tmp/metrics_def_${idx}.txt"

    local elapsed=0
    local phase="Unknown"
    while [[ $elapsed -lt $JOB_TIMEOUT ]]; do
        phase=$(kubectl get pods -n "$NAMESPACE" \
            --selector="job-name=${job_name}" \
            --output=jsonpath='{.items[0].status.phase}' 2>/dev/null || echo "Unknown")
        [[ "$phase" == "Succeeded" ]] && break
        [[ "$phase" == "Failed" ]]   && break
        sleep "$POLL_INTERVAL"
        elapsed=$((elapsed + POLL_INTERVAL))
    done

    if [[ "$phase" != "Succeeded" ]]; then
        echo "METRICS_STATUS=FAILED" > "$tmp_file"
        echo "  [WARN] $job_name tidak Succeeded (phase=$phase)" >&2
        return
    fi

    # Refresh pod_name kalau belum dapat
    if [[ "$pod_name" == "NOT_FOUND" ]]; then
        pod_name=$(kubectl get pods -n "$NAMESPACE" \
            --selector="job-name=${job_name}" \
            --output=jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "NOT_FOUND")
    fi

    if [[ "$pod_name" == "NOT_FOUND" ]]; then
        echo "METRICS_STATUS=NO_POD" > "$tmp_file"
        return
    fi

    local pod_creation pod_start_time container_started_at finished_at scheduled_at queue_wait

    pod_creation=$(get_field "$pod_name" '{.metadata.creationTimestamp}' 3)
    pod_start_time=$(get_field "$pod_name" '{.status.startTime}' 3)
    container_started_at=$(get_field "$pod_name" \
        '{.status.containerStatuses[0].state.terminated.startedAt}' 5)
    finished_at=$(get_field "$pod_name" \
        '{.status.containerStatuses[0].state.terminated.finishedAt}' 5)
    scheduled_at=$(kubectl get pod "$pod_name" -n "$NAMESPACE" \
        -o go-template='{{range .status.conditions}}{{if eq .type "PodScheduled"}}{{.lastTransitionTime}}{{end}}{{end}}' \
        2>/dev/null || echo "N/A")
    [[ -z "$scheduled_at" ]] && scheduled_at="N/A"

    queue_wait=$(calc_queue_wait "$pod_creation" "$arrival_epoch")

    {
        echo "METRICS_STATUS=OK"
        echo "POD_NAME=${pod_name}"
        echo "POD_CREATION=${pod_creation}"
        echo "POD_START_TIME=${pod_start_time}"
        echo "CONTAINER_STARTED_AT=${container_started_at}"
        echo "FINISHED_AT=${finished_at}"
        echo "SCHEDULED_AT=${scheduled_at}"
        echo "QUEUE_WAIT=${queue_wait}"
    } > "$tmp_file"

    echo "  [DONE] $job_name -> $tmp_file"
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
        return 1
    fi

    echo "=============================================="
    echo " Skenario DEFAULT : rho=${rho_label}"
    echo " Replay timing dari: ${edf_csv}"
    echo " Output CSV         : ${out_csv}"
    echo "=============================================="

    ensure_csv "$out_csv"

    # Baca EDF CSV
    # Kolom: order,rho,ori_id,size,fill_a,fill_b,job_name,pod_name,
    #        arrival_timestamp,...
    declare -a E_ORDER E_ORI_ID E_SIZE E_FILL_A E_FILL_B E_JOB_NAME E_ARRIVAL E_OFFSET

    local idx=0
    local first_arrival=""

    while IFS=',' read -r \
        e_order e_rho e_ori_id e_size e_fill_a e_fill_b \
        e_job_name e_pod_name e_arrival _rest; do

        [[ "$e_order" == "order" ]] && continue
        [[ -z "$e_order" ]]        && continue

        [[ -z "$first_arrival" ]] && first_arrival="$e_arrival"

        # Offset dalam detik (integer) dari job pertama
        local arr_int fa_int
        arr_int=$(echo "$e_arrival"    | cut -d'.' -f1)
        fa_int=$(echo  "$first_arrival" | cut -d'.' -f1)
        local offset
        offset=$(( $(date -d "$e_arrival" +%s) - $(date -d "$first_arrival" +%s) ))

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
    echo "  Total job: ${total} | first_arrival: ${first_arrival}"

    declare -a T_ORDER T_ORI_ID T_SIZE T_FILL_A T_FILL_B T_DEF_JOB_NAME T_ARRIVAL_DEF
    declare -a BG_PIDS

    rm -f /tmp/metrics_def_*.txt

    # start_epoch sebagai integer (detik)
    local start_epoch
    start_epoch=$(date +%s)
    echo "  start_epoch: ${start_epoch}"
    echo ""

    # ==================================================================
    # TAHAP 1: Apply job satu-satu dengan timing replay + spawn watcher
    # ==================================================================
    echo "--- TAHAP 1: Apply jobs (replay timing EDF) ---"
    for i in "${!E_ORDER[@]}"; do
        local ori_id="${E_ORI_ID[$i]}"
        local offset="${E_OFFSET[$i]}"

        # Hitung berapa detik lagi harus nunggu
        local now elapsed sleep_needed
        now=$(date +%s)
        elapsed=$(( now - start_epoch ))
        sleep_needed=$(( offset - elapsed ))

        if [[ $sleep_needed -gt 0 ]]; then
            echo "[${E_ORDER[$i]}/${total}] Menunggu ${sleep_needed}s (offset=${offset}s)..."
            sleep "$sleep_needed"
        fi

        # Catat arrival aktual
        local arrival_epoch
        arrival_epoch=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

        # Lookup data job
        local job_info="${JOB_DATA[$ori_id]:-}"
        if [[ -z "$job_info" ]]; then
            echo "  [WARN] ori_id=$ori_id tidak ada di $JOBS_CSV" >&2
            T_ORDER[$i]="${E_ORDER[$i]}"
            T_ORI_ID[$i]=$ori_id
            T_SIZE[$i]="${E_SIZE[$i]}"
            T_FILL_A[$i]="${E_FILL_A[$i]}"
            T_FILL_B[$i]="${E_FILL_B[$i]}"
            T_DEF_JOB_NAME[$i]="UNKNOWN"
            T_ARRIVAL_DEF[$i]=$arrival_epoch
            # Dummy background job supaya index BG_PIDS tidak bolong
            ( echo "METRICS_STATUS=NO_DATA" > "/tmp/metrics_def_${i}.txt" ) &
            BG_PIDS[$i]=$!
            continue
        fi

        IFS=',' read -r size fill_a fill_b cpu_usage max_runtime <<< "$job_info"

        local def_job_name="${E_JOB_NAME[$i]}-def"

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

        # Ambil pod name
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
        T_ARRIVAL_DEF[$i]=$arrival_epoch

        watch_job "$i" "$def_job_name" "$pod_name" "$arrival_epoch" &
        BG_PIDS[$i]=$!
        echo "  -> watcher PID: ${BG_PIDS[$i]}"
    done

    # ==================================================================
    # TAHAP 2: Tunggu semua background watcher selesai
    # ==================================================================
    echo ""
    echo "--- TAHAP 2: Menunggu semua watcher selesai ---"
    for i in "${!BG_PIDS[@]}"; do
        wait "${BG_PIDS[$i]}" || true
        echo "  [WATCHER DONE] index=${i} (${T_DEF_JOB_NAME[$i]})"
    done

    # ==================================================================
    # TAHAP 3: Tulis CSV sesuai urutan apply
    # ==================================================================
    echo ""
    echo "--- TAHAP 3: Tulis CSV (urutan apply) ---"
    for i in "${!T_ORDER[@]}"; do
        local order="${T_ORDER[$i]}"
        local ori_id="${T_ORI_ID[$i]}"
        local size="${T_SIZE[$i]}"
        local fill_a="${T_FILL_A[$i]}"
        local fill_b="${T_FILL_B[$i]}"
        local def_job_name="${T_DEF_JOB_NAME[$i]}"
        local arrival="${T_ARRIVAL_DEF[$i]}"
        local tmp_file="/tmp/metrics_def_${i}.txt"

        local status pod_name pod_creation pod_start container_started finished_at scheduled_at queue_wait
        status="MISSING"; pod_name="N/A"; pod_creation="N/A"; pod_start="N/A"
        container_started="N/A"; finished_at="N/A"; scheduled_at="N/A"; queue_wait="N/A"

        if [[ -f "$tmp_file" ]]; then
            while IFS='=' read -r key val; do
                case "$key" in
                    METRICS_STATUS)       status="$val" ;;
                    POD_NAME)             pod_name="$val" ;;
                    POD_CREATION)         pod_creation="$val" ;;
                    POD_START_TIME)       pod_start="$val" ;;
                    CONTAINER_STARTED_AT) container_started="$val" ;;
                    FINISHED_AT)          finished_at="$val" ;;
                    SCHEDULED_AT)         scheduled_at="$val" ;;
                    QUEUE_WAIT)           queue_wait="$val" ;;
                esac
            done < "$tmp_file"
        fi

        echo "${order},${rho_label},${ori_id},${size},${fill_a},${fill_b},${def_job_name},${pod_name},${arrival},${pod_creation},${pod_start},${container_started},${finished_at},${scheduled_at},${queue_wait}" >> "$out_csv"
        echo "  [WRITE] order=${order} ${def_job_name} | status=${status}"
    done

    rm -f /tmp/metrics_def_*.txt
    unset T_ORDER T_ORI_ID T_SIZE T_FILL_A T_FILL_B T_DEF_JOB_NAME T_ARRIVAL_DEF BG_PIDS
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