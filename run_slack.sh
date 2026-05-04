#!/bin/bash
# =============================================================================
# run_slack.sh
# Runner EDF + Default scheduler dengan timing replay dari
# edf_<rho>_slack.csv atau default_<rho>_slack.csv.
#
# CSV input sudah punya: order, ori_id, arrival_timestamp (+ kolom lain kosong)
# Script ini yang ngisi sisanya setelah job selesai.
#
# Deadline annotation = arrival_aktual + max_runtime (dari experiment_slack.csv)
# max_runtime di experiment_slack.csv sudah include slack, langsung pakai.
#
# Usage:
#   bash run_slack.sh [edf|default|both] [low|medium|high|very_high|all]
#   bash run_slack.sh both all    <- default
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Konfigurasi
# ---------------------------------------------------------------------------
JOBS_CSV="experiment_slack.csv"
YAML_TEMPLATE="job.yaml"
NAMESPACE="default"
JOB_TIMEOUT=2000
POLL_INTERVAL=5
CSV_DIR="."
OUTPUT_DIR="."

CSV_HEADER="order,rho,ori_id,size,fill_a,fill_b,job_name,pod_name,arrival_timestamp,pod_creation_timestamp,container_creation_timestamp,container_started_at,finished_at,scheduled_at,queue_wait_seconds,deadline_timestamp"

# Inisialisasi global
declare -a E_ORDER=() E_ORI_ID=() E_ARRIVAL=() E_OFFSET=()
declare -A JOB_DATA

# ---------------------------------------------------------------------------
# Cek versi bash
# ---------------------------------------------------------------------------
if [[ ${BASH_VERSINFO[0]} -lt 4 ]]; then
    echo "[ERROR] Bash 4+ required. Current: ${BASH_VERSION}" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Lookup: ori_id -> size,fill_a,fill_b,cpu_usage,max_runtime
# ---------------------------------------------------------------------------
load_job_data() {
    while IFS=',' read -r ori_id job_name size fill_a fill_b cpu_usage max_runtime; do
        [[ "$ori_id" == "ID" || -z "$ori_id" || -z "$job_name" ]] && continue

        ori_id=$(echo "$ori_id"           | xargs)
        size=$(echo "$size"               | xargs)
        fill_a=$(echo "$fill_a"           | xargs)
        fill_b=$(echo "$fill_b"           | xargs)
        cpu_usage=$(echo "$cpu_usage"     | xargs)
        max_runtime=$(echo "$max_runtime" | xargs)

        JOB_DATA[$ori_id]="${size},${fill_a},${fill_b},${cpu_usage},${max_runtime}"
        echo "  [DEBUG] Loaded ID=$ori_id max_runtime=$max_runtime" >&2
    done < "$JOBS_CSV"

    echo "  [INFO] Loaded ${#JOB_DATA[@]} records dari $JOBS_CSV"
    echo "  [DEBUG] Available IDs: ${!JOB_DATA[*]}" >&2
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
ensure_csv() {
    [[ -f "$1" ]] && rm -f "$1"
    echo "$CSV_HEADER" > "$1"
    echo "  [CSV] Siap: $1"
}

to_epoch() {
    local iso=$1
    if [[ "$OSTYPE" == "darwin"* ]]; then
        date -j -f "%Y-%m-%dT%H:%M:%SZ" "$iso" +%s 2>/dev/null || echo ""
    else
        date -d "$iso" +%s 2>/dev/null || echo ""
    fi
}

from_epoch() {
    local epoch=$1
    if [[ "$OSTYPE" == "darwin"* ]]; then
        date -j -f "%s" "$epoch" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || echo "N/A"
    else
        date -d "@${epoch}" -u +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null || echo "N/A"
    fi
}

calc_queue_wait() {
    local pod_creation_iso=$1 arrival_iso=$2
    [[ "$pod_creation_iso" == "N/A" || -z "$pod_creation_iso" ]] && echo "N/A" && return
    [[ "$arrival_iso"      == "N/A" || -z "$arrival_iso"      ]] && echo "N/A" && return

    local pod_epoch arr_epoch wait_s sign=""
    pod_epoch=$(to_epoch "$pod_creation_iso")
    arr_epoch=$(to_epoch "$arrival_iso")
    [[ -z "$pod_epoch" || -z "$arr_epoch" ]] && echo "N/A" && return

    wait_s=$(( pod_epoch - arr_epoch ))
    [[ $wait_s -lt 0 ]] && sign="-" && wait_s=$(( -wait_s ))
    printf "%s%d\n" "$sign" "$wait_s"
}

get_field() {
    local pod_name=$1 jpath=$2 max_retries=${3:-1}
    local val="" attempt=0
    while [[ $attempt -lt $max_retries ]]; do
        val=$(kubectl get pod "$pod_name" -n "$NAMESPACE" \
            -o jsonpath="$jpath" 2>/dev/null || echo "")
        if [[ -n "$val" && "$val" != "N/A" ]]; then echo "$val"; return 0; fi
        sleep 3
        attempt=$(( attempt + 1 ))
    done
    echo "N/A"
}

# ---------------------------------------------------------------------------
# Background watcher
# ---------------------------------------------------------------------------
watch_job() {
    local idx=$1 job_name=$2 pod_name=$3 arrival_iso=$4
    local tmp_file="/tmp/metrics_slack_${idx}.txt"

    local elapsed=0 phase="Unknown"
    while [[ $elapsed -lt $JOB_TIMEOUT ]]; do
        phase=$(kubectl get pods -n "$NAMESPACE" \
            --selector="job-name=${job_name}" \
            --output=jsonpath='{.items[0].status.phase}' 2>/dev/null || echo "Unknown")
        if   [[ "$phase" == "Succeeded" ]]; then break
        elif [[ "$phase" == "Failed"    ]]; then
            echo "  [WARN] $job_name Failed" >&2
            echo "METRICS_STATUS=FAILED" > "$tmp_file"; return
        fi
        sleep "$POLL_INTERVAL"
        elapsed=$(( elapsed + POLL_INTERVAL ))
    done

    if [[ "$phase" != "Succeeded" ]]; then
        echo "METRICS_STATUS=TIMEOUT" > "$tmp_file"
        echo "  [WARN] $job_name timeout setelah ${JOB_TIMEOUT}s (phase=$phase)" >&2
        return
    fi

    if [[ "$pod_name" == "NOT_FOUND" || -z "$pod_name" ]]; then
        pod_name=$(kubectl get pods -n "$NAMESPACE" \
            --selector="job-name=${job_name}" \
            --output=jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "NOT_FOUND")
    fi

    if [[ "$pod_name" == "NOT_FOUND" || -z "$pod_name" ]]; then
        echo "METRICS_STATUS=NO_POD" > "$tmp_file"
        echo "  [WARN] $job_name: pod tidak ditemukan" >&2
        return
    fi

    local pod_creation container_creation_ts container_started finished scheduled queue_wait deadline_annotation

    pod_creation=$(get_field "$pod_name" '{.metadata.creationTimestamp}' 3)
    container_creation_ts=$(get_field "$pod_name" '{.status.startTime}' 3)
    container_started=$(get_field "$pod_name" \
        '{.status.containerStatuses[0].state.terminated.startedAt}' 5)
    finished=$(get_field "$pod_name" \
        '{.status.containerStatuses[0].state.terminated.finishedAt}' 5)

    scheduled=$(kubectl get pod "$pod_name" -n "$NAMESPACE" \
        -o jsonpath='{.status.conditions[?(@.type=="PodScheduled")].lastTransitionTime}' \
        2>/dev/null | cut -d' ' -f1 | tr -d '"')
    [[ -z "$scheduled" ]] && scheduled="N/A"

    deadline_annotation=$(kubectl get pod "$pod_name" -n "$NAMESPACE" \
        -o jsonpath='{.metadata.annotations.scheduling/deadline-timestamp}' \
        2>/dev/null || echo "N/A")

    queue_wait=$(calc_queue_wait "$pod_creation" "$arrival_iso")

    {
        echo "METRICS_STATUS=OK"
        echo "POD_NAME=${pod_name}"
        echo "POD_CREATION=${pod_creation}"
        echo "CONTAINER_CREATION_TS=${container_creation_ts}"
        echo "CONTAINER_STARTED=${container_started}"
        echo "FINISHED=${finished}"
        echo "SCHEDULED=${scheduled}"
        echo "QUEUE_WAIT=${queue_wait}"
        echo "DEADLINE=${deadline_annotation}"
    } > "$tmp_file"

    echo "  [DONE] $job_name -> $tmp_file"
}

# ---------------------------------------------------------------------------
# Load input CSV
# ---------------------------------------------------------------------------
load_input_csv() {
    local input_csv=$1
    E_ORDER=(); E_ORI_ID=(); E_ARRIVAL=(); E_OFFSET=()
    local idx=0 first_arrival=""

    echo "  [DEBUG] Mencoba membaca $input_csv..." >&2

    while IFS=',' read -r e_order e_rho e_ori_id e_size e_fill_a e_fill_b e_job_name e_pod_name e_arrival rest; do
        e_order=$(echo "$e_order" | xargs | tr -d '"')
        [[ "$e_order" == "order" || -z "$e_order" ]] && continue

        e_ori_id=$(echo "$e_ori_id" | xargs | tr -d '"')
        e_arrival=$(echo "$e_arrival" | xargs | tr -d '"')

        [[ -z "$e_ori_id" || -z "$e_arrival" ]] && continue
        [[ -z "$first_arrival" ]] && first_arrival="$e_arrival"

        local arr_ep=$(to_epoch "$e_arrival")
        local fa_ep=$(to_epoch "$first_arrival")
        local offset=$(( arr_ep - fa_ep ))

        E_ORDER[$idx]=$e_order
        E_ORI_ID[$idx]=$e_ori_id
        E_ARRIVAL[$idx]=$e_arrival
        E_OFFSET[$idx]=$offset

        echo "  [DEBUG] Record[$idx]: order=$e_order id=$e_ori_id arrival=$e_arrival offset=$offset" >&2
        idx=$(( idx + 1 ))
    done < <(tr -d '\r' < "$input_csv")

    echo "$idx"
}

# ---------------------------------------------------------------------------
# Core runner
# ---------------------------------------------------------------------------
run_scenario_inner() {
    local scheduler_name=$1
    local job_suffix=$2
    local out_csv=$3
    local rho_label=$4

    local total=${#E_ORDER[@]}
    echo "  Total job: ${total} | scheduler: ${scheduler_name}"

    if [[ $total -eq 0 ]]; then
        echo "  [ERROR] Total job 0! Cek load_input_csv." >&2
        return 1
    fi

    declare -a T_ORDER T_ORI_ID T_SIZE T_FILL_A T_FILL_B T_JOB_NAME T_POD_NAME T_ARRIVAL BG_PIDS

    rm -f /tmp/metrics_slack_*.txt
    local start_epoch=$(date +%s)
    echo "  start_epoch: ${start_epoch}"
    echo ""

    echo "--- TAHAP 1: Apply jobs ---"
    for i in $(seq 0 $(( total - 1 ))); do
        local offset="${E_OFFSET[$i]}"
        local now=$(date +%s)
        local elapsed=$(( now - start_epoch ))
        local sleep_needed=$(( offset - elapsed ))
        
        if [[ $sleep_needed -gt 0 ]]; then
            echo "[${E_ORDER[$i]}/${total}] Menunggu ${sleep_needed}s (offset=${offset}s)..."
            sleep "$sleep_needed"
        fi

        local arrival_iso=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
        local ori_id="${E_ORI_ID[$i]}"
        local job_info="${JOB_DATA[$ori_id]:-}"

        if [[ -z "$job_info" ]]; then
            echo "  [WARN] ori_id=$ori_id tidak ada di $JOBS_CSV" >&2
            T_ORDER[$i]="${E_ORDER[$i]}"; T_ORI_ID[$i]=$ori_id; T_JOB_NAME[$i]="UNKNOWN"; T_ARRIVAL[$i]=$arrival_iso
            ( echo "METRICS_STATUS=NO_DATA" > "/tmp/metrics_slack_${i}.txt" ) &
            BG_PIDS[$i]=$!
            continue
        fi

        IFS=',' read -r size fill_a fill_b cpu_usage max_runtime <<< "$job_info"
        size=$(echo "$size" | xargs); fill_a=$(echo "$fill_a" | xargs); fill_b=$(echo "$fill_b" | xargs); 
        cpu_usage=$(echo "$cpu_usage" | xargs); max_runtime=$(echo "$max_runtime" | xargs)

        local arrival_epoch=$(to_epoch "$arrival_iso")
        local deadline_iso=$( [[ -n "$arrival_epoch" ]] && from_epoch $(( arrival_epoch + max_runtime )) || echo "N/A" )
        
        # FIX: Penamaan job sesuai request
        local new_job_name="job-$(echo "${ori_id}" | tr '[:upper:]' '[:lower:]')${job_suffix}"

        local tmp_yaml=$(mktemp /tmp/job_slack_XXXXXX.yaml)
        sed -e "s|<job_name>|${new_job_name}|g" -e "s|<max_runtime>|${max_runtime}|g" \
            -e "s|<cpu_usage>|${cpu_usage}|g" -e "s|<size>|${size}|g" -e "s|<fill_a>|${fill_a}|g" \
            -e "s|<fill_b>|${fill_b}|g" -e "s|<scheduler_name>|${scheduler_name}|g" \
            -e "s|<deadline_timestamp>|${deadline_iso}|g" "$YAML_TEMPLATE" > "$tmp_yaml"

        echo "[${E_ORDER[$i]}/${total}] APPLY ${new_job_name} | arrival=${arrival_iso} | deadline=${deadline_iso}"
        kubectl apply -f "$tmp_yaml" -n "$NAMESPACE" >/dev/null && rm -f "$tmp_yaml"

        local pod_name="" wp=0
        while [[ -z "$pod_name" && $wp -lt 60 ]]; do
            pod_name=$(kubectl get pods -n "$NAMESPACE" --selector="job-name=${new_job_name}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
            [[ -z "$pod_name" ]] && sleep 2 && wp=$(( wp + 2 ))
        done
        [[ -z "$pod_name" ]] && pod_name="NOT_FOUND" && echo "  [WARN] Pod tidak ditemukan" >&2 || echo "  -> pod: ${pod_name}"

        T_ORDER[$i]="${E_ORDER[$i]}"; T_ORI_ID[$i]=$ori_id; T_SIZE[$i]=$size; T_FILL_A[$i]=$fill_a; T_FILL_B[$i]=$fill_b; T_JOB_NAME[$i]=$new_job_name; T_POD_NAME[$i]=$pod_name; T_ARRIVAL[$i]=$arrival_iso

        watch_job "$i" "$new_job_name" "$pod_name" "$arrival_iso" &
        BG_PIDS[$i]=$!
        echo "  -> watcher PID: ${BG_PIDS[$i]}"
    done

    echo ""
    echo "--- TAHAP 2: Menunggu semua watcher selesai ---"
    for i in "${!BG_PIDS[@]}"; do
        if [[ -n "${BG_PIDS[$i]:-}" ]]; then
            wait "${BG_PIDS[$i]}" 2>/dev/null || true
            echo "  [WATCHER DONE] index=${i} (${T_JOB_NAME[$i]:-UNKNOWN})"
        fi
    done

    echo ""
    echo "--- TAHAP 3: Tulis CSV ---"
    for i in $(seq 0 $(( total - 1 ))); do
        local tmp_file="/tmp/metrics_slack_${i}.txt"; local status="MISSING"; local pod_creation="N/A"
        local c_create="N/A"; local c_start="N/A"; local finished="N/A"; local scheduled="N/A"; local q_wait="N/A"; local dline="N/A"
        local pod_name_metrics="${T_POD_NAME[$i]}"
        if [[ -f "$tmp_file" ]]; then
            while IFS='=' read -r key val; do
                case "$key" in METRICS_STATUS) status="$val" ;; POD_NAME) pod_name_metrics="$val" ;; POD_CREATION) pod_creation="$val" ;; CONTAINER_CREATION_TS) c_create="$val" ;; CONTAINER_STARTED) c_start="$val" ;; FINISHED) finished="$val" ;; SCHEDULED) scheduled="$val" ;; QUEUE_WAIT) q_wait="$val" ;; DEADLINE) dline="$val" ;; esac
            done < "$tmp_file"
        fi
        echo "${T_ORDER[$i]},${rho_label},${T_ORI_ID[$i]},${T_SIZE[$i]:-N/A},${T_FILL_A[$i]:-N/A},${T_FILL_B[$i]:-N/A},${T_JOB_NAME[$i]},${pod_name_metrics},${T_ARRIVAL[$i]},${pod_creation},${c_create},${c_start},${finished},${scheduled},${q_wait},${dline}" >> "$out_csv"
        echo "  [WRITE] order=${T_ORDER[$i]} ${T_JOB_NAME[$i]} status=${status}"
    done
    rm -f /tmp/metrics_slack_*.txt
}

run_edf_scenario() {
    local rho=$1; local input_csv="${CSV_DIR}/edf_${rho}_slack.csv"; local out_csv="${OUTPUT_DIR}/edf_${rho}_slack_result.csv"
    [[ ! -f "$input_csv" ]] && echo "[ERROR] Input EDF CSV tidak ada: $input_csv" >&2 && return 1
    echo "=============================================="; echo " EDF SLACK  rho=${rho}"; echo " Input : ${input_csv}"; echo "=============================================="
    ensure_csv "$out_csv"
    local n=$(load_input_csv "$input_csv")
    echo "  Loaded ${n} entries"
    run_scenario_inner "edf-scheduler" "" "$out_csv" "$rho"
}

run_default_scenario() {
    local rho=$1; local input_csv="${CSV_DIR}/default_${rho}_slack.csv"; local out_csv="${OUTPUT_DIR}/default_${rho}_slack_result.csv"
    [[ ! -f "$input_csv" ]] && echo "[ERROR] Input Default CSV tidak ada: $input_csv" >&2 && return 1
    echo "=============================================="; echo " DEFAULT SLACK  rho=${rho}"; echo " Input : ${input_csv}"; echo "=============================================="
    ensure_csv "$out_csv"
    local n=$(load_input_csv "$input_csv")
    echo "  Loaded ${n} entries"
    run_scenario_inner "deadline-default-scheduler" "-def" "$out_csv" "$rho"
}

# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------
MODE="${1:-both}"; RHO="${2:-all}"
RHO_LIST=()
case "$RHO" in low|medium|high|very_high) RHO_LIST=("$RHO") ;; all) RHO_LIST=(low medium high very_high) ;; *) exit 1 ;; esac

load_job_data
for rho in "${RHO_LIST[@]}"; do
    case "$MODE" in
        edf) run_edf_scenario "$rho" ;;
        default) run_default_scenario "$rho" ;;
        both) run_edf_scenario "$rho"; run_default_scenario "$rho" ;;
    esac
done
echo "=== run_slack.sh selesai ==="