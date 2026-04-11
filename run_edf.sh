#!/bin/bash
# =============================================================================
# run_edf.sh
# Apply semua job langsung (tanpa jeda), catat arrival_timestamp tiap job.
# Tiap job punya background watcher — begitu pod Succeeded, langsung
# ambil metrics dan tulis ke file sementara /tmp/metrics_<i>.txt.
# Setelah semua background watcher selesai, CSV ditulis sesuai urutan apply.
#
# Usage: bash run_edf.sh [low|medium|high|all]   (default: all)
# =============================================================================

set -euo pipefail

JOBS_CSV="jobs.csv"
YAML_TEMPLATE="job-template.yaml"
NAMESPACE="default"
JOB_TIMEOUT=2000
POLL_INTERVAL=5
OUTPUT_DIR="."

declare -A RHO_MAP
RHO_MAP[low]=0.5
RHO_MAP[medium]=0.75
RHO_MAP[high]=0.95

CSV_HEADER="order,rho,ori_id,size,fill_a,fill_b,job_name,pod_name,arrival_timestamp,pod_creation_timestamp,pod_start_time,container_started_at,finished_at,scheduled_at,queue_wait_seconds"

ensure_csv() {
    echo "$CSV_HEADER" > "$1"
    echo "  [CSV] Siap: $1"
}

# ---------------------------------------------------------------------------
# Ambil satu field jsonpath dari pod, dengan retry sampai nilainya non-kosong
# Usage: get_field <pod_name> <jsonpath> <max_retries>
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
# Hitung queue_wait_seconds = pod_creation (ISO) - arrival_epoch (unix float)
# Pure bash dengan date -d
# ---------------------------------------------------------------------------
calc_queue_wait() {
    local pod_creation_iso=$1   # e.g. 2026-03-12T12:04:07Z
    local arrival_epoch=$2      # e.g. 1741780647.123456

    [[ "$pod_creation_iso" == "N/A" ]] && echo "N/A" && return

    # Konversi ISO ke epoch (detik, integer) pakai date
    local pod_epoch
    pod_epoch=$(date -d "$pod_creation_iso" +%s 2>/dev/null || echo "")
    [[ -z "$pod_epoch" ]] && echo "N/A" && return

    # Hitung selisih — arrival_epoch bisa float, ambil integer bagiannya
    local arrival_int
    arrival_int=$(echo "$arrival_epoch" | cut -d'.' -f1)
    local arrival_frac
    arrival_frac=$(echo "$arrival_epoch" | cut -d'.' -f2)

    # wait = pod_epoch - arrival_epoch  (pembulatan ke 1 desimal)
    # Karena bash hanya integer: hitung dalam milidetik
    local pod_ms=$((pod_epoch * 1000))
    # arrival dalam ms: gabung integer + 3 digit pertama fraksi
    local frac3="${arrival_frac:0:3}"   # ambil 3 digit (milidetik)
    local arrival_ms=$(( arrival_int * 1000 + 10#$frac3 ))
    local wait_ms=$(( pod_ms - arrival_ms ))

    # Format jadi detik dengan 3 desimal
    local sign=""
    if [[ $wait_ms -lt 0 ]]; then
        sign="-"
        wait_ms=$(( -wait_ms ))
    fi
    printf "%s%d.%03d\n" "$sign" "$((wait_ms / 1000))" "$((wait_ms % 1000))"
}

# ---------------------------------------------------------------------------
# Background watcher untuk satu job:
#   - Poll sampai pod Succeeded (atau timeout)
#   - Ambil semua metrics via jsonpath (pure bash)
#   - Tulis ke /tmp/metrics_<idx>.txt
# ---------------------------------------------------------------------------
watch_job() {
    local idx=$1
    local job_name=$2
    local pod_name=$3
    local arrival_epoch=$4
    local tmp_file="/tmp/metrics_${idx}.txt"

    # Tunggu pod Succeeded
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

    # Ambil pod_name kalau masih NOT_FOUND (mungkin sudah muncul sekarang)
    if [[ "$pod_name" == "NOT_FOUND" ]]; then
        pod_name=$(kubectl get pods -n "$NAMESPACE" \
            --selector="job-name=${job_name}" \
            --output=jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "NOT_FOUND")
    fi

    if [[ "$pod_name" == "NOT_FOUND" ]]; then
        echo "METRICS_STATUS=NO_POD" > "$tmp_file"
        return
    fi

    # --- Ambil semua field ---
    local pod_creation
    pod_creation=$(get_field "$pod_name" '{.metadata.creationTimestamp}' 3)

    local pod_start_time
    pod_start_time=$(get_field "$pod_name" '{.status.startTime}' 3)

    local container_started_at
    container_started_at=$(get_field "$pod_name" \
        '{.status.containerStatuses[0].state.terminated.startedAt}' 5)

    local finished_at
    finished_at=$(get_field "$pod_name" \
        '{.status.containerStatuses[0].state.terminated.finishedAt}' 5)

    # scheduled_at: pakai go-template karena jsonpath tidak bisa filter conditions
    local scheduled_at
    scheduled_at=$(kubectl get pod "$pod_name" -n "$NAMESPACE" \
        -o go-template='{{range .status.conditions}}{{if eq .type "PodScheduled"}}{{.lastTransitionTime}}{{end}}{{end}}' \
        2>/dev/null || echo "N/A")
    [[ -z "$scheduled_at" ]] && scheduled_at="N/A"

    local queue_wait
    queue_wait=$(calc_queue_wait "$pod_creation" "$arrival_epoch")

    # Tulis hasil ke tmp file
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

    # Array state
    declare -a T_ORDER T_ORI_ID T_SIZE T_FILL_A T_FILL_B T_JOB_NAME T_ARRIVAL
    declare -a BG_PIDS   # PID tiap background watcher

    # Bersihkan tmp files dari run sebelumnya
    rm -f /tmp/metrics_*.txt

    # ==================================================================
    # TAHAP 1: Apply semua job, spawn background watcher tiap job
    # ==================================================================
    echo ""
    echo "--- TAHAP 1: Apply jobs + spawn watchers ---"
    for i in "${!JOB_LINES[@]}"; do
        local line="${JOB_LINES[$i]}"
        IFS=',' read -r ori_id job_name size fill_a fill_b cpu_usage max_runtime <<< "$line"
        local order=$((i + 1))

        # Catat arrival tepat sebelum apply
        local arrival_epoch
        arrival_epoch=$(date +%s%N | awk '{printf "%.6f\n", $1/1000000000}')

        # Render YAML
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

        # Ambil pod name (poll singkat, max 60s)
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

        # Simpan state
        T_ORDER[$i]=$order
        T_ORI_ID[$i]=$ori_id
        T_SIZE[$i]=$size
        T_FILL_A[$i]=$fill_a
        T_FILL_B[$i]=$fill_b
        T_JOB_NAME[$i]=$job_name
        T_ARRIVAL[$i]=$arrival_epoch

        # Spawn background watcher untuk job ini
        watch_job "$i" "$job_name" "$pod_name" "$arrival_epoch" &
        BG_PIDS[$i]=$!
        echo "  -> watcher PID: ${BG_PIDS[$i]}"
    done

    # ==================================================================
    # TAHAP 2: Tunggu semua background watcher selesai
    # (tiap watcher sudah nulis ke /tmp/metrics_<i>.txt saat pod-nya done)
    # ==================================================================
    echo ""
    echo "--- TAHAP 2: Menunggu semua watcher selesai ---"
    for i in "${!BG_PIDS[@]}"; do
        wait "${BG_PIDS[$i]}" || true
        echo "  [WATCHER DONE] job index=${i} (${T_JOB_NAME[$i]})"
    done

    # ==================================================================
    # TAHAP 3: Tulis CSV sesuai URUTAN APPLY (bukan urutan selesai)
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

        echo "  [WRITE] order=${order} ${job_name}"

        # Baca tmp file metrics
        local status pod_name pod_creation pod_start container_started finished_at scheduled_at queue_wait
        status="MISSING"
        pod_name="N/A"
        pod_creation="N/A"
        pod_start="N/A"
        container_started="N/A"
        finished_at="N/A"
        scheduled_at="N/A"
        queue_wait="N/A"

        if [[ -f "$tmp_file" ]]; then
            # Source file tmp — tiap baris KEY=VALUE
            while IFS='=' read -r key val; do
                case "$key" in
                    METRICS_STATUS)      status="$val" ;;
                    POD_NAME)            pod_name="$val" ;;
                    POD_CREATION)        pod_creation="$val" ;;
                    POD_START_TIME)      pod_start="$val" ;;
                    CONTAINER_STARTED_AT) container_started="$val" ;;
                    FINISHED_AT)         finished_at="$val" ;;
                    SCHEDULED_AT)        scheduled_at="$val" ;;
                    QUEUE_WAIT)          queue_wait="$val" ;;
                esac
            done < "$tmp_file"
        fi

        echo "${order},${rho_label},${ori_id},${size},${fill_a},${fill_b},${job_name},${pod_name},${arrival},${pod_creation},${pod_start},${container_started},${finished_at},${scheduled_at},${queue_wait}" >> "$out_csv"
        echo "     -> status=${status} | pod=${pod_name}"
    done

    # Bersihkan tmp files
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