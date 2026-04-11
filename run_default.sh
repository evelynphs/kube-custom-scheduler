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
    local line_num=0
    while IFS=',' read -r ori_id job_name size fill_a fill_b cpu_usage max_runtime; do
        line_num=$((line_num + 1))
        # Skip header
        [[ "$ori_id" == "ID" ]] && continue
        # Skip empty lines
        [[ -z "$ori_id" || -z "$job_name" ]] && continue
        
        # Trim whitespace
        ori_id=$(echo "$ori_id" | xargs)
        job_name=$(echo "$job_name" | xargs)
        size=$(echo "$size" | xargs)
        fill_a=$(echo "$fill_a" | xargs)
        fill_b=$(echo "$fill_b" | xargs)
        cpu_usage=$(echo "$cpu_usage" | xargs)
        max_runtime=$(echo "$max_runtime" | xargs)
        
        # Store in associative array - menggunakan subscript tanpa kutip
        JOB_DATA[$ori_id]="${size},${fill_a},${fill_b},${cpu_usage},${max_runtime}"
        
        # Debug: tampilkan loading
        echo "  [DEBUG] Loaded ID $ori_id: $job_name (max_runtime: $max_runtime)" >&2
    done < "$JOBS_CSV"
    
    echo "  [INFO] Loaded ${#JOB_DATA[@]} records dari $JOBS_CSV"
    
    # Debug: tampilkan semua keys
    echo "  [DEBUG] Available IDs: ${!JOB_DATA[*]}" >&2
}

ensure_csv() {
    # Hapus file lama jika ada
    [[ -f "$1" ]] && rm -f "$1"
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
        if [[ -n "$val" && "$val" != "N/A" ]]; then
            echo "$val"
            return 0
        fi
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

    [[ "$pod_creation_iso" == "N/A" || -z "$pod_creation_iso" ]] && echo "N/A" && return
    [[ "$arrival_ts" == "N/A" || -z "$arrival_ts" ]] && echo "N/A" && return

    local pod_epoch arrival_epoch wait_s sign=""
    
    # Konversi ke epoch
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        pod_epoch=$(date -j -f "%Y-%m-%dT%H:%M:%SZ" "$pod_creation_iso" +%s 2>/dev/null || echo "")
        arrival_epoch=$(date -j -f "%Y-%m-%dT%H:%M:%SZ" "$arrival_ts" +%s 2>/dev/null || echo "")
    else
        # Linux
        pod_epoch=$(date -d "$pod_creation_iso" +%s 2>/dev/null || echo "")
        arrival_epoch=$(date -d "$arrival_ts" +%s 2>/dev/null || echo "")
    fi

    [[ -z "$pod_epoch" || -z "$arrival_epoch" ]] && echo "N/A" && return

    wait_s=$(( pod_epoch - arrival_epoch ))
    if [[ $wait_s -lt 0 ]]; then
        sign="-"
        wait_s=$(( -wait_s ))
    fi
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
        
        if [[ "$phase" == "Succeeded" ]]; then
            break
        elif [[ "$phase" == "Failed" ]]; then
            echo "  [WARN] $job_name Failed" >&2
            echo "METRICS_STATUS=FAILED" > "$tmp_file"
            return
        fi
        sleep "$POLL_INTERVAL"
        elapsed=$((elapsed + POLL_INTERVAL))
    done

    if [[ "$phase" != "Succeeded" ]]; then
        echo "METRICS_STATUS=TIMEOUT" > "$tmp_file"
        echo "  [WARN] $job_name timeout setelah ${JOB_TIMEOUT}s (phase=$phase)" >&2
        return
    fi

    # Refresh pod_name kalau belum dapat
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

    local pod_creation pod_start_time container_started_at finished_at scheduled_at queue_wait

    pod_creation=$(get_field "$pod_name" '{.metadata.creationTimestamp}' 3)
    pod_start_time=$(get_field "$pod_name" '{.status.startTime}' 3)
    container_started_at=$(get_field "$pod_name" \
        '{.status.containerStatuses[0].state.terminated.startedAt}' 5)
    finished_at=$(get_field "$pod_name" \
        '{.status.containerStatuses[0].state.terminated.finishedAt}' 5)
    
    # Get scheduled_at dengan cara yang lebih robust
    scheduled_at=$(kubectl get pod "$pod_name" -n "$NAMESPACE" \
        -o jsonpath='{.status.conditions[?(@.type=="PodScheduled")].lastTransitionTime}' \
        2>/dev/null | cut -d' ' -f1 | tr -d '"')
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
    declare -a E_ORDER E_ORI_ID E_SIZE E_FILL_A E_FILL_B E_JOB_NAME E_ARRIVAL E_OFFSET

    local idx=0
    local first_arrival=""
    local line_num=0

    while IFS=',' read -r \
        e_order e_rho e_ori_id e_size e_fill_a e_fill_b \
        e_job_name e_pod_name e_arrival _rest; do
        
        line_num=$((line_num + 1))
        
        # Skip header dan baris kosong
        [[ "$e_order" == "order" || -z "$e_order" ]] && continue
        
        # Trim whitespace
        e_order=$(echo "$e_order" | xargs)
        e_ori_id=$(echo "$e_ori_id" | xargs)
        e_size=$(echo "$e_size" | xargs)
        e_fill_a=$(echo "$e_fill_a" | xargs)
        e_fill_b=$(echo "$e_fill_b" | xargs)
        e_job_name=$(echo "$e_job_name" | xargs)
        e_arrival=$(echo "$e_arrival" | xargs)

        # Set first arrival
        if [[ -z "$first_arrival" ]]; then
            first_arrival="$e_arrival"
        fi

        # Hitung offset dalam detik
        local offset=0
        if [[ "$OSTYPE" == "darwin"* ]]; then
            # macOS
            local arr_epoch=$(date -j -f "%Y-%m-%dT%H:%M:%SZ" "$e_arrival" +%s 2>/dev/null || echo "0")
            local fa_epoch=$(date -j -f "%Y-%m-%dT%H:%M:%SZ" "$first_arrival" +%s 2>/dev/null || echo "0")
            offset=$((arr_epoch - fa_epoch))
        else
            # Linux
            offset=$(( $(date -d "$e_arrival" +%s) - $(date -d "$first_arrival" +%s) ))
        fi

        E_ORDER[$idx]=$e_order
        E_ORI_ID[$idx]=$e_ori_id
        E_SIZE[$idx]=$e_size
        E_FILL_A[$idx]=$e_fill_a
        E_FILL_B[$idx]=$e_fill_b
        E_JOB_NAME[$idx]=$e_job_name
        E_ARRIVAL[$idx]=$e_arrival
        E_OFFSET[$idx]=$offset
        
        idx=$((idx + 1))
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
    
    for i in $(seq 0 $((total - 1))); do
        local ori_id="${E_ORI_ID[$i]}"
        local offset="${E_OFFSET[$i]}"

        # Hitung berapa detik lagi harus nunggu
        local now elapsed sleep_needed
        now=$(date +%s)
        elapsed=$((now - start_epoch))
        sleep_needed=$((offset - elapsed))

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
            # Dummy background job
            ( echo "METRICS_STATUS=NO_DATA" > "/tmp/metrics_def_${i}.txt" ) &
            BG_PIDS[$i]=$!
            continue
        fi

        # Parse job_info
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
        
        if ! kubectl apply -f "$tmp_yaml" -n "$NAMESPACE"; then
            echo "  [ERROR] Gagal apply ${def_job_name}" >&2
            rm -f "$tmp_yaml"
            continue
        fi
        
        rm -f "$tmp_yaml"

        # Ambil pod name
        local pod_name=""
        local wp=0
        while [[ -z "$pod_name" && $wp -lt 60 ]]; do
            pod_name=$(kubectl get pods -n "$NAMESPACE" \
                --selector="job-name=${def_job_name}" \
                --output=jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
            if [[ -z "$pod_name" ]]; then
                sleep 2
                wp=$((wp + 2))
            else
                break
            fi
        done
        
        if [[ -z "$pod_name" ]]; then
            pod_name="NOT_FOUND"
            echo "  [WARN] Pod tidak ditemukan untuk ${def_job_name}" >&2
        else
            echo "  -> pod: ${pod_name}"
        fi

        T_ORDER[$i]="${E_ORDER[$i]}"
        T_ORI_ID[$i]=$ori_id
        T_SIZE[$i]=$size
        T_FILL_A[$i]=$fill_a
        T_FILL_B[$i]=$fill_b
        T_DEF_JOB_NAME[$i]=$def_job_name
        T_ARRIVAL_DEF[$i]=$arrival_epoch

        # Jalankan watcher di background
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
        if [[ -n "${BG_PIDS[$i]}" ]]; then
            wait "${BG_PIDS[$i]}" 2>/dev/null || true
            echo "  [WATCHER DONE] index=${i} (${T_DEF_JOB_NAME[$i]:-UNKNOWN})"
        fi
    done

    # ==================================================================
    # TAHAP 3: Tulis CSV sesuai urutan apply
    # ==================================================================
    echo ""
    echo "--- TAHAP 3: Tulis CSV (urutan apply) ---"
    
    for i in $(seq 0 $((total - 1))); do
        local order="${T_ORDER[$i]:-}"
        local ori_id="${T_ORI_ID[$i]:-}"
        local size="${T_SIZE[$i]:-N/A}"
        local fill_a="${T_FILL_A[$i]:-N/A}"
        local fill_b="${T_FILL_B[$i]:-N/A}"
        local def_job_name="${T_DEF_JOB_NAME[$i]:-UNKNOWN}"
        local arrival="${T_ARRIVAL_DEF[$i]:-N/A}"
        local tmp_file="/tmp/metrics_def_${i}.txt"

        local status="MISSING"
        local pod_name="N/A"
        local pod_creation="N/A"
        local pod_start="N/A"
        local container_started="N/A"
        local finished_at="N/A"
        local scheduled_at="N/A"
        local queue_wait="N/A"

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

        # Write to CSV
        echo "${order},${rho_label},${ori_id},${size},${fill_a},${fill_b},${def_job_name},${pod_name},${arrival},${pod_creation},${pod_start},${container_started},${finished_at},${scheduled_at},${queue_wait}" >> "$out_csv"
        echo "  [WRITE] order=${order} ${def_job_name} | status=${status}"
    done

    # Cleanup
    rm -f /tmp/metrics_def_*.txt
    
    # Unset arrays
    unset E_ORDER E_ORI_ID E_SIZE E_FILL_A E_FILL_B E_JOB_NAME E_ARRIVAL E_OFFSET
    unset T_ORDER T_ORI_ID T_SIZE T_FILL_A T_FILL_B T_DEF_JOB_NAME T_ARRIVAL_DEF BG_PIDS

    echo ""
    echo "Selesai default ${rho_label} -> ${out_csv}"
    echo ""
}

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

# Cek versi bash
if [[ ${BASH_VERSINFO[0]} -lt 4 ]]; then
    echo "[ERROR] Bash version 4 or higher required" >&2
    echo "Current version: ${BASH_VERSION}" >&2
    exit 1
fi

# Load job data
load_job_data

# Validasi file requirements
if [[ ! -f "$YAML_TEMPLATE" ]]; then
    echo "[ERROR] YAML template tidak ditemukan: $YAML_TEMPLATE" >&2
    exit 1
fi

if [[ ! -f "$JOBS_CSV" ]]; then
    echo "[ERROR] Jobs CSV tidak ditemukan: $JOBS_CSV" >&2
    exit 1
fi

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