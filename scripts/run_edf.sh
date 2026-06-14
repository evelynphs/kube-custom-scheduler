#!/bin/bash
# =============================================================================
# run_edf.sh
# Apply job dengan Poisson inter-arrival timing, catat arrival_timestamp.
# Tiap job punya background watcher — begitu pod Succeeded, langsung
# ambil metrics dan tulis ke /tmp/metrics_<i>.txt.
# CSV ditulis sesuai urutan apply setelah semua watcher selesai.
#
# Fitur tambahan:
#   - Apply stuffer jobs sebelum main jobs
#   - Retry missing CSV rows setelah semua job selesai
#   - Cleanup semua jobs setelah skenario selesai
#
# Parameter M/M/c:
#   c   = 10  (12 core / 1.1 core per job, round down)
#   mu  = 0.0019  (1 / 524.21s avg runtime)
#   lambda = rho * c * mu
#   rho: low=0.50, medium=0.75, high=0.95, very_high=0.99
#
# Usage: bash run_edf.sh [low|medium|high|very_high|all]   (default: all)
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
OUTPUT_DIR="."

# Stuffer config
STUFFER_EDF_DIR="temp-stuffer"
STUFFER_EDF_PREFIX="temp-stuffer"
STUFFER_EDF_COUNT=3

# Retry config
RETRY_MAX=3
RETRY_WAIT=10

# M/M/c parameters
C_SERVERS=10
MU=0.0019     # 1 / 524.21

declare -A RHO_MAP
RHO_MAP[low]=0.50
RHO_MAP[medium]=0.75
RHO_MAP[high]=0.95
RHO_MAP[very_high]=0.99
RHO_MAP[extreme]=2.0

CSV_HEADER="order,rho,ori_id,size,fill_a,fill_b,job_name,pod_name,arrival_timestamp,pod_creation_timestamp,container_creation_timestamp,container_started_at,finished_at,scheduled_at,queue_wait_seconds,deadline_timestamp"

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
# Generate N inter-arrival times (detik) dari distribusi Exponential(lambda)
# ---------------------------------------------------------------------------
generate_interarrivals() {
    local n=$1
    local rho=$2
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
# FITUR: Apply stuffer jobs sebelum job utama
# ---------------------------------------------------------------------------
apply_stuffers() {
    echo "--- STUFFER: Apply stuffer jobs (EDF) ---"

    if [[ ! -d "$STUFFER_EDF_DIR" ]]; then
        echo "  [WARN] Stuffer dir tidak ditemukan: $STUFFER_EDF_DIR, skip stuffer." >&2
        return 0
    fi

    local applied=0
    for n in $(seq 1 "$STUFFER_EDF_COUNT"); do
        local yaml_path="${STUFFER_EDF_DIR}/${STUFFER_EDF_PREFIX}-${n}.yaml"
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

    echo "  [STUFFER] Total applied: ${applied}/${STUFFER_EDF_COUNT}"
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
    local tmp_file="/tmp/metrics_${idx}.txt"

    local elapsed=0
    local phase="Unknown"

    while [[ $elapsed -lt $JOB_TIMEOUT ]]; do
        phase=$(kubectl get pods -n "$NAMESPACE" \
            --selector="job-name=${job_name}" \
            --output=jsonpath='{.items[0].status.phase}' 2>/dev/null || echo "Unknown")
        [[ "$phase" == "Succeeded" ]] && break
        [[ "$phase" == "Failed"    ]] && break
        sleep "$POLL_INTERVAL"
        elapsed=$(( elapsed + POLL_INTERVAL ))
    done

    if [[ "$phase" != "Succeeded" ]]; then
        echo "METRICS_STATUS=FAILED" > "$tmp_file"
        echo "  [WARN] $job_name tidak Succeeded (phase=$phase, elapsed=${elapsed}s)" >&2
        return
    fi

    # Refresh pod_name kalau masih NOT_FOUND
    if [[ "$pod_name" == "NOT_FOUND" || -z "$pod_name" ]]; then
        pod_name=$(kubectl get pods -n "$NAMESPACE" \
            --selector="job-name=${job_name}" \
            --output=jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "NOT_FOUND")
    fi

    if [[ "$pod_name" == "NOT_FOUND" || -z "$pod_name" ]]; then
        echo "METRICS_STATUS=NO_POD" > "$tmp_file"
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

    mapfile -t INTERARRIVALS < <(generate_interarrivals "$total" "$rho_val")

    echo "  [INFO] Total job: ${total}"
    echo "  [INFO] Sample inter-arrivals (3 pertama): ${INTERARRIVALS[0]}s, ${INTERARRIVALS[1]}s, ${INTERARRIVALS[2]}s"
    echo ""

    declare -a T_ORDER T_ORI_ID T_SIZE T_FILL_A T_FILL_B T_JOB_NAME T_ARRIVAL
    declare -a BG_PIDS

    rm -f /tmp/metrics_*.txt

    # ==========================================================================
    # TAHAP 1: Apply stuffer jobs
    # ==========================================================================
    apply_stuffers

    # ==========================================================================
    # TAHAP 2: Apply job utama + spawn watcher
    # ==========================================================================
    echo "--- TAHAP 2: Apply jobs (Poisson arrival) ---"

    for i in "${!JOB_LINES[@]}"; do
        local line="${JOB_LINES[$i]}"
        line=$(echo "$line" | tr -d '\r')

        IFS=',' read -r ori_id job_name size fill_a fill_b cpu_usage max_runtime <<< "$line"

        ori_id=$(echo "$ori_id"           | xargs)
        job_name=$(echo "$job_name"       | xargs)
        size=$(echo "$size"               | xargs)
        fill_a=$(echo "$fill_a"           | xargs)
        fill_b=$(echo "$fill_b"           | xargs)
        cpu_usage=$(echo "$cpu_usage"     | xargs)
        max_runtime=$(echo "$max_runtime" | xargs)

        local order=$(( i + 1 ))

        # Tunggu inter-arrival sebelum apply (kecuali job pertama)
        if [[ $i -gt 0 ]]; then
            local wait_s="${INTERARRIVALS[$i]}"
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
            -e "s|<scheduler_name>|deadline-aware-scheduler|g" \
            "$YAML_TEMPLATE" > "$tmp_yaml"

        echo "[${order}/${total}] APPLY ${job_name} | arrival=${arrival_epoch}"

        if ! kubectl apply -f "$tmp_yaml" -n "$NAMESPACE"; then
            echo "  [ERROR] Gagal apply ${job_name}" >&2
            rm -f "$tmp_yaml"
            T_ORDER[$i]=$order
            T_ORI_ID[$i]=$ori_id
            T_SIZE[$i]=$size; T_FILL_A[$i]=$fill_a; T_FILL_B[$i]=$fill_b
            T_JOB_NAME[$i]=$job_name
            T_ARRIVAL[$i]=$arrival_epoch
            ( echo "METRICS_STATUS=APPLY_FAILED" > "/tmp/metrics_${i}.txt" ) &
            BG_PIDS[$i]=$!
            continue
        fi
        rm -f "$tmp_yaml"

        # Ambil pod name (poll max 60s)
        local pod_name=""
        local wp=0
        while [[ -z "$pod_name" && $wp -lt 60 ]]; do
            pod_name=$(kubectl get pods -n "$NAMESPACE" \
                --selector="job-name=${job_name}" \
                --output=jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
            [[ -z "$pod_name" ]] && { sleep 2; wp=$(( wp + 2 )); }
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

    # ==========================================================================
    # TAHAP 3: Tunggu semua watcher
    # ==========================================================================
    echo ""
    echo "--- TAHAP 3: Menunggu semua watcher selesai ---"
    for i in "${!BG_PIDS[@]}"; do
        wait "${BG_PIDS[$i]}" || true
        echo "  [WATCHER DONE] index=${i} (${T_JOB_NAME[$i]})"
    done

    # ==========================================================================
    # TAHAP 4: Tulis CSV
    # ==========================================================================
    echo ""
    echo "--- TAHAP 4: Tulis CSV (urutan apply) ---"
    for i in "${!T_ORDER[@]}"; do
        local order="${T_ORDER[$i]}"
        local ori_id="${T_ORI_ID[$i]}"
        local size="${T_SIZE[$i]}"
        local fill_a="${T_FILL_A[$i]}"
        local fill_b="${T_FILL_B[$i]}"
        local job_name="${T_JOB_NAME[$i]}"
        local arrival="${T_ARRIVAL[$i]}"
        local tmp_file="/tmp/metrics_${i}.txt"

        local status pod_name pod_creation container_creation_timestamp container_started finished_at scheduled_at queue_wait deadline_timestamp
        status="MISSING"; pod_name="N/A"; pod_creation="N/A"; container_creation_timestamp="N/A"
        container_started="N/A"; finished_at="N/A"; scheduled_at="N/A"; queue_wait="N/A"; deadline_timestamp="N/A"

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

        echo "${order},${rho_label},${ori_id},${size},${fill_a},${fill_b},${job_name},${pod_name},${arrival},${pod_creation},${container_creation_timestamp},${container_started},${finished_at},${scheduled_at},${queue_wait},${deadline_timestamp}" >> "$out_csv"
        echo "  [WRITE] order=${order} ${job_name} | status=${status}"
    done

    rm -f /tmp/metrics_*.txt
    unset T_ORDER T_ORI_ID T_SIZE T_FILL_A T_FILL_B T_JOB_NAME T_ARRIVAL BG_PIDS

    # ==========================================================================
    # TAHAP 5: Retry missing data
    # ==========================================================================
    echo ""
    retry_missing_rows "$out_csv"

    # ==========================================================================
    # TAHAP 6: Cleanup semua jobs
    # ==========================================================================
    cleanup_all_jobs

    echo "Selesai EDF ${rho_label} -> ${out_csv}"
    echo ""
}

# ---------------------------------------------------------------------------
# Validasi
# ---------------------------------------------------------------------------
[[ ! -f "$YAML_TEMPLATE" ]] && echo "[ERROR] YAML template tidak ada: $YAML_TEMPLATE" >&2 && exit 1
[[ ! -f "$JOBS_CSV"      ]] && echo "[ERROR] Jobs CSV tidak ada: $JOBS_CSV"             >&2 && exit 1

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
MODE="${1:-all}"
case "$MODE" in
    low)       run_scenario low ;;
    medium)    run_scenario medium ;;
    high)      run_scenario high ;;
    very_high) run_scenario very_high ;;
    extreme)   run_scenario extreme ;;
    all)
        run_scenario low
        run_scenario medium
        run_scenario high
        run_scenario very_high
        run_scenario extreme
        ;;
    *)
        echo "Usage: $0 [low|medium|high|very_high|extreme|all]"
        exit 1
        ;;
esac

echo "=== run_edf.sh selesai ==="