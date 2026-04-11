#!/bin/bash
# =============================================================================
# run_edf.sh
# Apply job dengan Poisson inter-arrival timing, catat arrival_timestamp.
# Tiap job punya background watcher — begitu pod Succeeded, langsung
# ambil metrics dan tulis ke /tmp/metrics_<i>.txt.
# CSV ditulis sesuai urutan apply setelah semua watcher selesai.
#
# Parameter M/M/c:
#   c   = 10  (12 core / 1.1 core per job, round down)
#   mu  = 0.0015  (1 / 678.18s avg runtime)
#   lambda = rho * c * mu
#   rho: low=0.50, medium=0.75, high=0.95
#
# Usage: bash run_edf.sh [low|medium|high|all]   (default: all)
# =============================================================================

set -euo pipefail

JOBS_CSV="experiment_five.csv"
YAML_TEMPLATE="job.yaml"
NAMESPACE="default"
JOB_TIMEOUT=2000
POLL_INTERVAL=5
OUTPUT_DIR="."

# M/M/c parameters
C_SERVERS=10
# MU=0.0015    # 1 / 678.18
MU=0.00862 

declare -A RHO_MAP
RHO_MAP[low]=0.50
RHO_MAP[medium]=0.75
RHO_MAP[high]=0.95

CSV_HEADER="order,rho,ori_id,size,fill_a,fill_b,job_name,pod_name,arrival_timestamp,pod_creation_timestamp,container_creation_timestamp,container_started_at,finished_at,scheduled_at,queue_wait_seconds"

ensure_csv() {
    echo "$CSV_HEADER" > "$1"
    echo "  [CSV] Siap: $1"
}

# ---------------------------------------------------------------------------
# Generate N inter-arrival times (detik) dari distribusi Exponential(lambda)
# Output: satu float per baris
# ---------------------------------------------------------------------------
generate_interarrivals() {
    local n=$1
    local rho=$2
    # Pakai awk supaya no python dependency
    awk -v n="$n" -v rho="$rho" -v c="$C_SERVERS" -v mu="$MU" -v seed="$RANDOM" '
    BEGIN {
        srand(seed)
        lam = rho * c * mu
        for (i = 0; i < n; i++) {
            u = rand()
            while (u == 0) u = rand()
            printf "%.6f\n", -log(u) / lam
        }
    }'
}

# ---------------------------------------------------------------------------
# Ambil satu field jsonpath dari pod, retry sampai non-kosong
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
# Hitung queue_wait_seconds = pod_creation - arrival (pure bash + date)
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
    local tmp_file="/tmp/metrics_${idx}.txt"

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
        echo "  [WARN] $job_name tidak Succeeded (phase=$phase, elapsed=${elapsed}s)" >&2
        return
    fi

    # Refresh pod_name kalau masih NOT_FOUND
    if [[ "$pod_name" == "NOT_FOUND" ]]; then
        pod_name=$(kubectl get pods -n "$NAMESPACE" \
            --selector="job-name=${job_name}" \
            --output=jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "NOT_FOUND")
    fi

    if [[ "$pod_name" == "NOT_FOUND" ]]; then
        echo "METRICS_STATUS=NO_POD" > "$tmp_file"
        return
    fi

    local pod_creation container_creation_timestamp container_started_at finished_at scheduled_at queue_wait

    pod_creation=$(get_field "$pod_name" '{.metadata.creationTimestamp}' 3)
    container_creation_timestamp=$(get_field "$pod_name" '{.status.startTime}' 3)
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
        echo "CONTAINER_CREATION_TIMESTAMP=${container_creation_timestamp}"
        echo "CONTAINER_STARTED_AT=${container_started_at}"
        echo "FINISHED_AT=${finished_at}"
        echo "SCHEDULED_AT=${scheduled_at}"
        echo "QUEUE_WAIT=${queue_wait}"
    } > "$tmp_file"

    echo "  [DONE] $job_name -> $tmp_file"
}

# ---------------------------------------------------------------------------
# Fungsi utama: satu skenario rho
# ---------------------------------------------------------------------------
run_scenario() {
    local rho_label=$1
    local rho_val=${RHO_MAP[$rho_label]}
    local out_csv="${OUTPUT_DIR}/edf_${rho_label}.csv"
    local lambda
    lambda=$(awk -v rho="$rho_val" -v c="$C_SERVERS" -v mu="$MU" \
        'BEGIN { printf "%.6f", rho * c * mu }')

    echo "=============================================="
    echo " Skenario EDF  : rho=${rho_label} (rho=${rho_val})"
    echo " lambda        : ${lambda} job/s"
    echo " mean interval : $(awk -v l="$lambda" 'BEGIN { printf "%.2f", 1/l }')s"
    echo " Output CSV    : ${out_csv}"
    echo "=============================================="

    ensure_csv "$out_csv"

    mapfile -t JOB_LINES < <(tail -n +2 "$JOBS_CSV" | shuf)
    local total=${#JOB_LINES[@]}

    # Generate inter-arrival times
    mapfile -t INTERARRIVALS < <(generate_interarrivals "$total" "$rho_val")

    echo "  [INFO] Total job: ${total}"
    echo "  [INFO] Sample inter-arrivals (3 pertama): ${INTERARRIVALS[0]}s, ${INTERARRIVALS[1]}s, ${INTERARRIVALS[2]}s"
    echo ""

    declare -a T_ORDER T_ORI_ID T_SIZE T_FILL_A T_FILL_B T_JOB_NAME T_ARRIVAL
    declare -a BG_PIDS

    rm -f /tmp/metrics_*.txt

    # ==================================================================
    # TAHAP 1: Apply job satu-satu dengan Poisson inter-arrival + spawn watcher
    # ==================================================================
    echo "--- TAHAP 1: Apply jobs (Poisson arrival) ---"
    for i in "${!JOB_LINES[@]}"; do
        local line="${JOB_LINES[$i]}"
        IFS=',' read -r ori_id job_name size fill_a fill_b cpu_usage max_runtime <<< "$line"
        local order=$((i + 1))

        # Tunggu inter-arrival sebelum apply (kecuali job pertama)
        if [[ $i -gt 0 ]]; then
            local wait_s="${INTERARRIVALS[$i]}"
            # Konversi float ke integer detik (round down) untuk sleep
            local wait_int
            wait_int=$(echo "$wait_s" | cut -d'.' -f1)
            if [[ $wait_int -gt 0 ]]; then
                echo "[${order}/${total}] Menunggu inter-arrival ${wait_s}s ..."
                sleep "$wait_s"
            fi
        fi

        # Catat arrival tepat sebelum apply
        local arrival_epoch
        arrival_epoch=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

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

        # Ambil pod name (poll max 60s)
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
        T_ARRIVAL[$i]=$arrival_epoch

        watch_job "$i" "$job_name" "$pod_name" "$arrival_epoch" &
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
        echo "  [WATCHER DONE] index=${i} (${T_JOB_NAME[$i]})"
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
        local job_name="${T_JOB_NAME[$i]}"
        local arrival="${T_ARRIVAL[$i]}"
        local tmp_file="/tmp/metrics_${i}.txt"

        local status pod_name pod_creation container_creation_timestamp container_started finished_at scheduled_at queue_wait
        status="MISSING"; pod_name="N/A"; pod_creation="N/A"; container_creation_timestamp="N/A"
        container_started="N/A"; finished_at="N/A"; scheduled_at="N/A"; queue_wait="N/A"

        if [[ -f "$tmp_file" ]]; then
            while IFS='=' read -r key val; do
                case "$key" in
                    METRICS_STATUS)       status="$val" ;;
                    POD_NAME)             pod_name="$val" ;;
                    POD_CREATION)         pod_creation="$val" ;;
                    CONTAINER_CREATION_TIMESTAMP) container_creation_timestamp="$val" ;;
                    CONTAINER_STARTED_AT) container_started="$val" ;;
                    FINISHED_AT)          finished_at="$val" ;;
                    SCHEDULED_AT)         scheduled_at="$val" ;;
                    QUEUE_WAIT)           queue_wait="$val" ;;
                esac
            done < "$tmp_file"
        fi

        echo "${order},${rho_label},${ori_id},${size},${fill_a},${fill_b},${job_name},${pod_name},${arrival},${pod_creation},${container_creation_timestamp},${container_started},${finished_at},${scheduled_at},${queue_wait}" >> "$out_csv"
        echo "  [WRITE] order=${order} ${job_name} | status=${status}"
    done

    rm -f /tmp/metrics_*.txt
    unset T_ORDER T_ORI_ID T_SIZE T_FILL_A T_FILL_B T_JOB_NAME T_ARRIVAL BG_PIDS

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