#!/bin/bash
# =============================================================================
# run_default.sh
# Default scheduler dengan timing arrival replay dari edf_*.csv.
# Tiap job punya background watcher — begitu pod Succeeded, langsung
# ambil metrics dan tulis ke file sementara /tmp/metrics_def_<i>.txt.
# CSV ditulis sesuai urutan apply setelah semua watcher selesai.
#
# Fitur tambahan:
#   - Apply stuffer jobs sebelum main jobs
#   - Retry missing CSV rows setelah semua job selesai
#   - Cleanup semua jobs setelah skenario selesai
#
# Usage: bash run_default.sh [low|medium|high|very_high|all]   (default: all)
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Cek versi bash (butuh 4+ untuk associative array)
# ---------------------------------------------------------------------------
if [[ ${BASH_VERSINFO[0]} -lt 4 ]]; then
    echo "[ERROR] Bash 4+ required. Current: ${BASH_VERSION}" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# Konfigurasi
# ---------------------------------------------------------------------------
JOBS_CSV="experiment_slack.csv"
YAML_TEMPLATE="job.yaml"
NAMESPACE="default"
JOB_TIMEOUT=2000
POLL_INTERVAL=5
EDF_CSV_DIR="."
OUTPUT_DIR="."

# Stuffer config
STUFFER_DEFAULT_DIR="temp-stuffer-default"
STUFFER_DEFAULT_PREFIX="temp-stuffer-default"
STUFFER_DEFAULT_COUNT=3

# Retry config
RETRY_MAX=3
RETRY_WAIT=10

CSV_HEADER="order,rho,ori_id,size,fill_a,fill_b,job_name,pod_name,arrival_timestamp,pod_creation_timestamp,container_creation_timestamp,container_started_at,finished_at,scheduled_at,queue_wait_seconds,deadline_timestamp"

# ---------------------------------------------------------------------------
# Build lookup: ori_id -> size,fill_a,fill_b,cpu_usage,max_runtime
# ---------------------------------------------------------------------------
declare -A JOB_DATA

load_job_data() {
    while IFS=',' read -r ori_id job_name size fill_a fill_b cpu_usage max_runtime; do
        [[ "$ori_id" == "ID" || -z "$ori_id" || -z "$job_name" ]] && continue

        ori_id=$(echo "$ori_id"           | xargs)
        job_name=$(echo "$job_name"       | xargs)
        size=$(echo "$size"               | xargs)
        fill_a=$(echo "$fill_a"           | xargs)
        fill_b=$(echo "$fill_b"           | xargs)
        cpu_usage=$(echo "$cpu_usage"     | xargs)
        max_runtime=$(echo "$max_runtime" | xargs)

        JOB_DATA[$ori_id]="${size},${fill_a},${fill_b},${cpu_usage},${max_runtime}"

        echo "  [DEBUG] Loaded ID=$ori_id: $job_name (max_runtime: $max_runtime)" >&2
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

calc_queue_wait() {
    local pod_creation_iso=$1
    local arrival_iso=$2

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
        if [[ -n "$val" && "$val" != "N/A" ]]; then
            echo "$val"; return 0
        fi
        sleep 3
        attempt=$(( attempt + 1 ))
    done
    echo "N/A"
}

# ---------------------------------------------------------------------------
# FITUR: Apply stuffer jobs sebelum job utama
# ---------------------------------------------------------------------------
apply_stuffers() {
    echo "--- STUFFER: Apply stuffer jobs (Default) ---"

    if [[ ! -d "$STUFFER_DEFAULT_DIR" ]]; then
        echo "  [WARN] Stuffer dir tidak ditemukan: $STUFFER_DEFAULT_DIR, skip stuffer." >&2
        return 0
    fi

    local applied=0
    for n in $(seq 1 "$STUFFER_DEFAULT_COUNT"); do
        local yaml_path="${STUFFER_DEFAULT_DIR}/${STUFFER_DEFAULT_PREFIX}-${n}.yaml"
        if [[ ! -f "$yaml_path" ]]; then
            echo "  [WARN] Stuffer YAML tidak ada: $yaml_path, skip." >&2
            continue
        fi
        echo "  [STUFFER] Applying: $yaml_path"
        if kubectl apply -f "$yaml_path" -n "$NAMESPACE"; then
            echo "  [STUFFER] OK: $yaml_path"
            applied=$(( applied + 1 ))
        else
            echo "  [ERROR] Gagal apply stuffer: $yaml_path" >&2
        fi
    done

    echo "  [STUFFER] Total applied: ${applied}/${STUFFER_DEFAULT_COUNT}"
    echo ""
}

# ---------------------------------------------------------------------------
# FITUR: Cleanup semua jobs di namespace
# ---------------------------------------------------------------------------
cleanup_all_jobs() {
    echo "--- CLEANUP: Menghapus semua jobs di namespace=${NAMESPACE} ---"

    local jobs
    jobs=$(kubectl get jobs -n "$NAMESPACE" \
        --output=jsonpath='{.items[*].metadata.name}' 2>/dev/null || echo "")

    if [[ -z "$jobs" ]]; then
        echo "  [CLEANUP] Tidak ada jobs yang perlu dihapus."
        return 0
    fi

    echo "  [CLEANUP] Jobs yang akan dihapus: $jobs"

    if kubectl delete jobs -n "$NAMESPACE" --all --wait=false 2>/dev/null; then
        echo "  [CLEANUP] Semua jobs berhasil dihapus."
    else
        echo "  [WARN] Ada masalah saat delete jobs, coba satu per satu..." >&2
        for job in $jobs; do
            kubectl delete job "$job" -n "$NAMESPACE" --wait=false 2>/dev/null || \
                echo "  [WARN] Gagal hapus job: $job" >&2
        done
    fi

    echo "  [CLEANUP] Menunggu pods terminate..."
    sleep 5
    echo "  [CLEANUP] Selesai."
    echo ""
}

# ---------------------------------------------------------------------------
# FITUR: Cek apakah baris CSV ada field yang missing
# ---------------------------------------------------------------------------
row_has_missing() {
    local row=$1
    local f10 f11 f12 f13 f14
    f10=$(echo "$row" | cut -d',' -f10)
    f11=$(echo "$row" | cut -d',' -f11)
    f12=$(echo "$row" | cut -d',' -f12)
    f13=$(echo "$row" | cut -d',' -f13)
    f14=$(echo "$row" | cut -d',' -f14)

    for f in "$f10" "$f11" "$f12" "$f13" "$f14"; do
        if [[ -z "$f" || "$f" == "N/A" ]]; then
            return 0  # ada yang missing
        fi
    done
    return 1  # semua lengkap
}

# ---------------------------------------------------------------------------
# FITUR: Fetch metrics dari pod yang sudah ada (untuk retry)
# ---------------------------------------------------------------------------
fetch_pod_metrics() {
    local pod_name=$1
    local arrival_iso=$2

    local pod_creation container_creation started finished scheduled deadline queue_wait

    pod_creation=$(kubectl get pod "$pod_name" -n "$NAMESPACE" \
        -o jsonpath='{.metadata.creationTimestamp}' 2>/dev/null || echo "")
    container_creation=$(kubectl get pod "$pod_name" -n "$NAMESPACE" \
        -o jsonpath='{.status.startTime}' 2>/dev/null || echo "")
    started=$(kubectl get pod "$pod_name" -n "$NAMESPACE" \
        -o jsonpath='{.status.containerStatuses[0].state.terminated.startedAt}' 2>/dev/null || echo "")
    finished=$(kubectl get pod "$pod_name" -n "$NAMESPACE" \
        -o jsonpath='{.status.containerStatuses[0].state.terminated.finishedAt}' 2>/dev/null || echo "")
    scheduled=$(kubectl get pod "$pod_name" -n "$NAMESPACE" \
        -o go-template='{{range .status.conditions}}{{if eq .type "PodScheduled"}}{{.lastTransitionTime}}{{end}}{{end}}' \
        2>/dev/null || echo "")
    deadline=$(kubectl get pod "$pod_name" -n "$NAMESPACE" \
        -o jsonpath='{.metadata.annotations.scheduling/deadline-timestamp}' 2>/dev/null || echo "")

    [[ -z "$pod_creation"      ]] && pod_creation="N/A"
    [[ -z "$container_creation" ]] && container_creation="N/A"
    [[ -z "$started"           ]] && started="N/A"
    [[ -z "$finished"          ]] && finished="N/A"
    [[ -z "$scheduled"         ]] && scheduled="N/A"
    [[ -z "$deadline"          ]] && deadline="N/A"

    queue_wait=$(calc_queue_wait "$pod_creation" "$arrival_iso")

    echo "${pod_creation},${container_creation},${started},${finished},${scheduled},${queue_wait},${deadline}"
}

# ---------------------------------------------------------------------------
# FITUR: Retry mechanism untuk mengisi data yang missing di CSV
# ---------------------------------------------------------------------------
retry_missing_rows() {
    local csv_file=$1

    echo "--- RETRY: Cek dan isi data missing di ${csv_file} ---"

    if [[ ! -f "$csv_file" ]]; then
        echo "  [ERROR] CSV tidak ditemukan: $csv_file" >&2
        return 1
    fi

    local attempt=0
    local has_missing=1

    while [[ $attempt -lt $RETRY_MAX && $has_missing -eq 1 ]]; do
        attempt=$(( attempt + 1 ))
        has_missing=0

        echo "  [RETRY] Attempt ${attempt}/${RETRY_MAX}..."

        local tmp_csv="${csv_file}.tmp_retry"
        echo "$CSV_HEADER" > "$tmp_csv"

        local line_num=0
        while IFS= read -r row; do
            line_num=$(( line_num + 1 ))
            [[ $line_num -eq 1 ]] && continue  # skip header

            if row_has_missing "$row"; then
                has_missing=1

                local order rho ori_id size fill_a fill_b job_name pod_name arrival
                order=$(echo "$row"    | cut -d',' -f1)
                rho=$(echo "$row"      | cut -d',' -f2)
                ori_id=$(echo "$row"   | cut -d',' -f3)
                size=$(echo "$row"     | cut -d',' -f4)
                fill_a=$(echo "$row"   | cut -d',' -f5)
                fill_b=$(echo "$row"   | cut -d',' -f6)
                job_name=$(echo "$row" | cut -d',' -f7)
                pod_name=$(echo "$row" | cut -d',' -f8)
                arrival=$(echo "$row"  | cut -d',' -f9)

                echo "  [RETRY] Missing data -> order=${order} job=${job_name} pod=${pod_name}"

                # Coba cari pod jika pod_name N/A atau kosong
                if [[ -z "$pod_name" || "$pod_name" == "N/A" || "$pod_name" == "NOT_FOUND" ]]; then
                    if [[ -n "$job_name" && "$job_name" != "UNKNOWN" ]]; then
                        pod_name=$(kubectl get pods -n "$NAMESPACE" \
                            --selector="job-name=${job_name}" \
                            --output=jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
                        [[ -z "$pod_name" ]] && pod_name="N/A"
                        echo "    -> pod lookup: ${pod_name}"
                    fi
                fi

                if [[ -z "$pod_name" || "$pod_name" == "N/A" || "$pod_name" == "NOT_FOUND" ]]; then
                    echo "    -> [WARN] Pod masih tidak ditemukan, skip row ini." >&2
                    echo "$row" >> "$tmp_csv"
                    continue
                fi

                local metrics
                metrics=$(fetch_pod_metrics "$pod_name" "$arrival")

                local pod_creation container_creation started finished scheduled queue_wait deadline
                pod_creation=$(echo "$metrics"       | cut -d',' -f1)
                container_creation=$(echo "$metrics" | cut -d',' -f2)
                started=$(echo "$metrics"            | cut -d',' -f3)
                finished=$(echo "$metrics"           | cut -d',' -f4)
                scheduled=$(echo "$metrics"          | cut -d',' -f5)
                queue_wait=$(echo "$metrics"         | cut -d',' -f6)
                deadline=$(echo "$metrics"           | cut -d',' -f7)

                local new_row="${order},${rho},${ori_id},${size},${fill_a},${fill_b},${job_name},${pod_name},${arrival},${pod_creation},${container_creation},${started},${finished},${scheduled},${queue_wait},${deadline}"
                echo "$new_row" >> "$tmp_csv"
                echo "    -> Updated: pod_creation=${pod_creation} finished=${finished}"
            else
                echo "$row" >> "$tmp_csv"
            fi

        done < "$csv_file"

        mv "$tmp_csv" "$csv_file"

        if [[ $has_missing -eq 1 && $attempt -lt $RETRY_MAX ]]; then
            echo "  [RETRY] Masih ada missing data, tunggu ${RETRY_WAIT}s sebelum retry..."
            sleep "$RETRY_WAIT"
        fi
    done

    if [[ $has_missing -eq 1 ]]; then
        echo "  [RETRY] Setelah ${RETRY_MAX} attempt, masih ada baris dengan missing data." >&2
        echo "  [RETRY] Periksa manual: $csv_file" >&2
    else
        echo "  [RETRY] Semua data lengkap di: $csv_file"
    fi

    echo ""
}

# ---------------------------------------------------------------------------
# Background watcher: poll Succeeded -> tulis tmp file
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

        if   [[ "$phase" == "Succeeded" ]]; then break
        elif [[ "$phase" == "Failed"    ]]; then
            echo "  [WARN] $job_name Failed" >&2
            echo "METRICS_STATUS=FAILED" > "$tmp_file"
            return
        fi
        sleep "$POLL_INTERVAL"
        elapsed=$(( elapsed + POLL_INTERVAL ))
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

    local pod_creation container_creation_timestamp container_started_at finished_at scheduled_at queue_wait deadline_timestamp

    pod_creation=$(get_field "$pod_name" '{.metadata.creationTimestamp}' 3)
    container_creation_timestamp=$(get_field "$pod_name" '{.status.startTime}' 3)
    container_started_at=$(get_field "$pod_name" \
        '{.status.containerStatuses[0].state.terminated.startedAt}' 5)
    finished_at=$(get_field "$pod_name" \
        '{.status.containerStatuses[0].state.terminated.finishedAt}' 5)

    scheduled_at=$(kubectl get pod "$pod_name" -n "$NAMESPACE" \
        -o jsonpath='{.status.conditions[?(@.type=="PodScheduled")].lastTransitionTime}' \
        2>/dev/null | cut -d' ' -f1 | tr -d '"')
    [[ -z "$scheduled_at" ]] && scheduled_at="N/A"

    deadline_timestamp=$(kubectl get pod "$pod_name" -n "$NAMESPACE" \
        -o jsonpath='{.metadata.annotations.scheduling/deadline-timestamp}' \
        2>/dev/null || echo "N/A")
    [[ -z "$deadline_timestamp" ]] && deadline_timestamp="N/A"

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
        echo "DEADLINE_TIMESTAMP=${deadline_timestamp}"
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

    # Baca EDF CSV -> isi array
    declare -a E_ORDER E_ORI_ID E_SIZE E_FILL_A E_FILL_B E_JOB_NAME E_ARRIVAL E_OFFSET

    local idx=0
    local first_arrival=""

    while IFS=',' read -r \
        e_order e_rho e_ori_id e_size e_fill_a e_fill_b \
        e_job_name e_pod_name e_arrival _rest; do

        [[ "$e_order" == "order" || -z "$e_order" ]] && continue

        e_order=$(echo "$e_order"     | xargs)
        e_ori_id=$(echo "$e_ori_id"   | xargs)
        e_size=$(echo "$e_size"       | xargs)
        e_fill_a=$(echo "$e_fill_a"   | xargs)
        e_fill_b=$(echo "$e_fill_b"   | xargs)
        e_job_name=$(echo "$e_job_name" | xargs)
        e_arrival=$(echo "$e_arrival" | xargs)

        [[ -z "$first_arrival" ]] && first_arrival="$e_arrival"

        local arr_ep fa_ep offset=0
        arr_ep=$(to_epoch "$e_arrival")
        fa_ep=$(to_epoch "$first_arrival")
        [[ -n "$arr_ep" && -n "$fa_ep" ]] && offset=$(( arr_ep - fa_ep ))

        E_ORDER[$idx]=$e_order
        E_ORI_ID[$idx]=$e_ori_id
        E_SIZE[$idx]=$e_size
        E_FILL_A[$idx]=$e_fill_a
        E_FILL_B[$idx]=$e_fill_b
        E_JOB_NAME[$idx]=$e_job_name
        E_ARRIVAL[$idx]=$e_arrival
        E_OFFSET[$idx]=$offset

        idx=$(( idx + 1 ))
    done < "$edf_csv"

    local total=${#E_ORDER[@]}
    echo "  Total job: ${total} | first_arrival: ${first_arrival}"

    declare -a T_ORDER T_ORI_ID T_SIZE T_FILL_A T_FILL_B T_DEF_JOB_NAME T_ARRIVAL_DEF
    declare -a BG_PIDS

    rm -f /tmp/metrics_def_*.txt

    local start_epoch
    start_epoch=$(date +%s)
    echo "  start_epoch: ${start_epoch}"
    echo ""

    # ==========================================================================
    # TAHAP 1: Apply stuffer jobs
    # ==========================================================================
    apply_stuffers

    # ==========================================================================
    # TAHAP 2: Apply job utama + spawn watcher
    # ==========================================================================
    echo "--- TAHAP 2: Apply jobs (replay timing EDF) ---"

    for i in $(seq 0 $(( total - 1 ))); do
        local ori_id="${E_ORI_ID[$i]}"
        local offset="${E_OFFSET[$i]}"

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
            T_SIZE[$i]="${E_SIZE[$i]}"; T_FILL_A[$i]="${E_FILL_A[$i]}"; T_FILL_B[$i]="${E_FILL_B[$i]}"
            T_DEF_JOB_NAME[$i]="UNKNOWN"
            T_ARRIVAL_DEF[$i]=$arrival_epoch
            ( echo "METRICS_STATUS=NO_DATA" > "/tmp/metrics_def_${i}.txt" ) &
            BG_PIDS[$i]=$!
            continue
        fi

        IFS=',' read -r size fill_a fill_b cpu_usage max_runtime <<< "$job_info"
        size=$(echo "$size"               | xargs)
        fill_a=$(echo "$fill_a"           | xargs)
        fill_b=$(echo "$fill_b"           | xargs)
        cpu_usage=$(echo "$cpu_usage"     | xargs)
        max_runtime=$(echo "$max_runtime" | xargs)

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
            -e "s|<scheduler_name>|deadline-default-scheduler|g" \
            "$YAML_TEMPLATE" > "$tmp_yaml"

        echo "[${E_ORDER[$i]}/${total}] APPLY ${def_job_name} | arrival=${arrival_epoch}"

        if ! kubectl apply -f "$tmp_yaml" -n "$NAMESPACE"; then
            echo "  [ERROR] Gagal apply ${def_job_name}" >&2
            rm -f "$tmp_yaml"
            T_ORDER[$i]="${E_ORDER[$i]}"
            T_ORI_ID[$i]=$ori_id
            T_SIZE[$i]=$size; T_FILL_A[$i]=$fill_a; T_FILL_B[$i]=$fill_b
            T_DEF_JOB_NAME[$i]=$def_job_name
            T_ARRIVAL_DEF[$i]=$arrival_epoch
            ( echo "METRICS_STATUS=APPLY_FAILED" > "/tmp/metrics_def_${i}.txt" ) &
            BG_PIDS[$i]=$!
            continue
        fi
        rm -f "$tmp_yaml"

        # Ambil pod name (poll max 60s)
        local pod_name=""
        local wp=0
        while [[ -z "$pod_name" && $wp -lt 60 ]]; do
            pod_name=$(kubectl get pods -n "$NAMESPACE" \
                --selector="job-name=${def_job_name}" \
                --output=jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
            if [[ -z "$pod_name" ]]; then
                sleep 2
                wp=$(( wp + 2 ))
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

        watch_job "$i" "$def_job_name" "$pod_name" "$arrival_epoch" &
        BG_PIDS[$i]=$!
        echo "  -> watcher PID: ${BG_PIDS[$i]}"
    done

    # ==========================================================================
    # TAHAP 3: Tunggu semua watcher
    # ==========================================================================
    echo ""
    echo "--- TAHAP 3: Menunggu semua watcher selesai ---"
    for i in "${!BG_PIDS[@]}"; do
        if [[ -n "${BG_PIDS[$i]}" ]]; then
            wait "${BG_PIDS[$i]}" 2>/dev/null || true
            echo "  [WATCHER DONE] index=${i} (${T_DEF_JOB_NAME[$i]:-UNKNOWN})"
        fi
    done

    # ==========================================================================
    # TAHAP 4: Tulis CSV
    # ==========================================================================
    echo ""
    echo "--- TAHAP 4: Tulis CSV (urutan apply) ---"

    for i in $(seq 0 $(( total - 1 ))); do
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
        local container_creation_timestamp="N/A"
        local container_started="N/A"
        local finished_at="N/A"
        local scheduled_at="N/A"
        local queue_wait="N/A"
        local deadline_timestamp="N/A"

        if [[ -f "$tmp_file" ]]; then
            while IFS='=' read -r key val; do
                case "$key" in
                    METRICS_STATUS)               status="$val" ;;
                    POD_NAME)                     pod_name="$val" ;;
                    POD_CREATION)                 pod_creation="$val" ;;
                    CONTAINER_CREATION_TIMESTAMP) container_creation_timestamp="$val" ;;
                    CONTAINER_STARTED_AT)         container_started="$val" ;;
                    FINISHED_AT)                  finished_at="$val" ;;
                    SCHEDULED_AT)                 scheduled_at="$val" ;;
                    QUEUE_WAIT)                   queue_wait="$val" ;;
                    DEADLINE_TIMESTAMP)           deadline_timestamp="$val" ;;
                esac
            done < "$tmp_file"
        fi

        echo "${order},${rho_label},${ori_id},${size},${fill_a},${fill_b},${def_job_name},${pod_name},${arrival},${pod_creation},${container_creation_timestamp},${container_started},${finished_at},${scheduled_at},${queue_wait},${deadline_timestamp}" >> "$out_csv"
        echo "  [WRITE] order=${order} ${def_job_name} | status=${status}"
    done

    rm -f /tmp/metrics_def_*.txt
    unset E_ORDER E_ORI_ID E_SIZE E_FILL_A E_FILL_B E_JOB_NAME E_ARRIVAL E_OFFSET
    unset T_ORDER T_ORI_ID T_SIZE T_FILL_A T_FILL_B T_DEF_JOB_NAME T_ARRIVAL_DEF BG_PIDS

    # ==========================================================================
    # TAHAP 5: Retry missing data
    # ==========================================================================
    echo ""
    retry_missing_rows "$out_csv"

    # ==========================================================================
    # TAHAP 6: Cleanup semua jobs
    # ==========================================================================
    cleanup_all_jobs

    echo "Selesai default ${rho_label} -> ${out_csv}"
    echo ""
}

# ---------------------------------------------------------------------------
# Validasi
# ---------------------------------------------------------------------------
[[ ! -f "$YAML_TEMPLATE" ]] && echo "[ERROR] YAML template tidak ada: $YAML_TEMPLATE" >&2 && exit 1
[[ ! -f "$JOBS_CSV"      ]] && echo "[ERROR] Jobs CSV tidak ada: $JOBS_CSV"             >&2 && exit 1

# Load job data
load_job_data

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
MODE="${1:-all}"
case "$MODE" in
    low)       run_scenario low ;;
    medium)    run_scenario medium ;;
    high)      run_scenario high ;;
    very_high) run_scenario very_high ;;
    all)
        run_scenario low
        run_scenario medium
        run_scenario high
        run_scenario very_high
        ;;
    *)
        echo "Usage: $0 [low|medium|high|very_high|all]"
        exit 1
        ;;
esac

echo "=== run_default.sh selesai ==="