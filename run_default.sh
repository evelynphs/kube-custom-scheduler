#!/bin/bash
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
# (FIX: handle last line + trim whitespace)
# ---------------------------------------------------------------------------
declare -A JOB_DATA

load_job_data() {
    while IFS=',' read -r ori_id job_name size fill_a fill_b cpu_usage max_runtime || [[ -n "$ori_id" ]]; do
        [[ "$ori_id" == "ID" ]] && continue
        [[ -z "$ori_id" ]] && continue

        ori_id=$(echo "$ori_id" | xargs)

        JOB_DATA["$ori_id"]="${size},${fill_a},${fill_b},${cpu_usage},${max_runtime}"
    done < "$JOBS_CSV"

    echo "  [INFO] Loaded ${#JOB_DATA[@]} records dari $JOBS_CSV"
}

ensure_csv() {
    echo "$CSV_HEADER" > "$1"
    echo "  [CSV] Siap: $1"
}

# ---------------------------------------------------------------------------
# Helper ambil field
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
        sleep 2
        attempt=$((attempt + 1))
    done

    echo "N/A"
}

# ---------------------------------------------------------------------------
# Queue wait
# ---------------------------------------------------------------------------
calc_queue_wait() {
    local pod_creation_iso=$1
    local arrival_ts=$2

    [[ "$pod_creation_iso" == "N/A" ]] && echo "N/A" && return

    local pod_epoch arrival_epoch
    pod_epoch=$(date -d "$pod_creation_iso" +%s 2>/dev/null || echo "")
    arrival_epoch=$(date -d "$arrival_ts" +%s 2>/dev/null || echo "")

    [[ -z "$pod_epoch" || -z "$arrival_epoch" ]] && echo "N/A" && return

    echo $(( pod_epoch - arrival_epoch ))
}

# ---------------------------------------------------------------------------
# Watcher
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
        [[ "$phase" == "Failed" ]] && break

        sleep "$POLL_INTERVAL"
        elapsed=$((elapsed + POLL_INTERVAL))
    done

    if [[ "$phase" != "Succeeded" ]]; then
        echo "METRICS_STATUS=FAILED" > "$tmp_file"
        return
    fi

    pod_name=$(kubectl get pods -n "$NAMESPACE" \
        --selector="job-name=${job_name}" \
        --output=jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "NOT_FOUND")

    [[ "$pod_name" == "NOT_FOUND" ]] && {
        echo "METRICS_STATUS=NO_POD" > "$tmp_file"
        return
    }

    local pod_creation pod_start container_started finished_at scheduled_at queue_wait

    pod_creation=$(get_field "$pod_name" '{.metadata.creationTimestamp}' 3)
    pod_start=$(get_field "$pod_name" '{.status.startTime}' 3)
    container_started=$(get_field "$pod_name" '{.status.containerStatuses[0].state.terminated.startedAt}' 5)
    finished_at=$(get_field "$pod_name" '{.status.containerStatuses[0].state.terminated.finishedAt}' 5)

    scheduled_at=$(kubectl get pod "$pod_name" -n "$NAMESPACE" \
        -o go-template='{{range .status.conditions}}{{if eq .type "PodScheduled"}}{{.lastTransitionTime}}{{end}}{{end}}' \
        2>/dev/null || echo "N/A")
    [[ -z "$scheduled_at" ]] && scheduled_at="N/A"

    queue_wait=$(calc_queue_wait "$pod_creation" "$arrival_epoch")

    {
        echo "METRICS_STATUS=OK"
        echo "POD_NAME=${pod_name}"
        echo "POD_CREATION=${pod_creation}"
        echo "POD_START_TIME=${pod_start}"
        echo "CONTAINER_STARTED_AT=${container_started}"
        echo "FINISHED_AT=${finished_at}"
        echo "SCHEDULED_AT=${scheduled_at}"
        echo "QUEUE_WAIT=${queue_wait}"
    } > "$tmp_file"
}

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
run_scenario() {
    local rho_label=$1
    local edf_csv="${EDF_CSV_DIR}/edf_${rho_label}.csv"
    local out_csv="${OUTPUT_DIR}/default_${rho_label}.csv"

    ensure_csv "$out_csv"

    declare -a E_ORDER E_ORI_ID E_JOB_NAME E_ARRIVAL E_OFFSET

    local idx=0
    local first_arrival=""

    while IFS=',' read -r e_order e_rho e_ori_id _ _ _ e_job_name _ e_arrival _ || [[ -n "$e_order" ]]; do
        [[ "$e_order" == "order" ]] && continue
        [[ -z "$e_order" ]] && continue

        e_ori_id=$(echo "$e_ori_id" | xargs)

        [[ -z "$first_arrival" ]] && first_arrival="$e_arrival"

        local offset
        offset=$(( $(date -d "$e_arrival" +%s) - $(date -d "$first_arrival" +%s) ))

        E_ORDER[$idx]=$e_order
        E_ORI_ID[$idx]=$e_ori_id
        E_JOB_NAME[$idx]=$e_job_name
        E_ARRIVAL[$idx]=$e_arrival
        E_OFFSET[$idx]=$offset

        idx=$((idx+1))
    done < "$edf_csv"

    local total=${#E_ORDER[@]}
    echo "  Total job: ${total}"

    declare -a BG_PIDS
    local start_epoch
    start_epoch=$(date +%s)

    for i in "${!E_ORDER[@]}"; do
        local ori_id="${E_ORI_ID[$i]}"
        local offset="${E_OFFSET[$i]}"

        local now elapsed sleep_needed
        now=$(date +%s)
        elapsed=$(( now - start_epoch ))
        sleep_needed=$(( offset - elapsed ))

        [[ $sleep_needed -gt 0 ]] && sleep "$sleep_needed"

        local arrival_epoch
        arrival_epoch=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

        local job_info="${JOB_DATA[$ori_id]:-}"

        if [[ -z "$job_info" ]]; then
            echo "[WARN] ori_id=$ori_id tidak ada"
            continue
        fi

        IFS=',' read -r size fill_a fill_b cpu_usage max_runtime <<< "$job_info"

        local def_job_name="${E_JOB_NAME[$i]}-def"

        sed \
            -e "s|<job_name>|${def_job_name}|g" \
            -e "s|<max_runtime>|${max_runtime}|g" \
            -e "s|schedulerName: deadline-aware-scheduler|schedulerName: default-scheduler|g" \
            "$YAML_TEMPLATE" | kubectl apply -f -

        local pod_name
        pod_name=$(kubectl get pods -n "$NAMESPACE" \
            --selector="job-name=${def_job_name}" \
            --output=jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "NOT_FOUND")

        watch_job "$i" "$def_job_name" "$pod_name" "$arrival_epoch" &
        BG_PIDS[$i]=$!
    done

    for pid in "${BG_PIDS[@]}"; do
        wait "$pid" || true
    done

    for i in "${!E_ORDER[@]}"; do
        local tmp_file="/tmp/metrics_def_${i}.txt"
        [[ -f "$tmp_file" ]] && cat "$tmp_file"
    done

    echo "DONE"
}

# ENTRY
load_job_data
run_scenario "${1:-low}"