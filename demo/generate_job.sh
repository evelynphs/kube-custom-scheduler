#!/usr/bin/env bash
set -euo pipefail

# === Config ===
N_JOBS="${N_JOBS:-10}"
NAMESPACE="${NAMESPACE:-default}"
SCHEDULER_NAME="${SCHEDULER_NAME:-deadline-aware-scheduler}"
DEADLINE_ANNOTATION_KEY="${DEADLINE_ANNOTATION_KEY:-scheduling/deadline}"

# Deadline settings (UTC, RFC3339)
# Start from "now + START_OFFSET_MIN minutes", then increment by STEP_MIN each job.
START_OFFSET_MIN="${START_OFFSET_MIN:-30}"
STEP_MIN="${STEP_MIN:-1}"

# Job runtime (seconds).
SLEEP_SECONDS="${SLEEP_SECONDS:-30}"

# === Helpers ===
need_cmd() { command -v "$1" >/dev/null 2>&1 || { echo "Missing command: $1" >&2; exit 1; }; }
need_cmd kubectl
need_cmd date

deadline_rfc3339_utc() {
  local minutes_from_now="$1"
  # Try GNU date style first (Linux)
  if out=$(date -u -d "+${minutes_from_now} minutes" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null); then
    printf '%s' "$out"
    return 0
  fi
  # Try gdate (GNU coreutils installed on macOS)
  if command -v gdate >/dev/null 2>&1; then
    gdate -u -d "+${minutes_from_now} minutes" +"%Y-%m-%dT%H:%M:%SZ"
    return 0
  fi
  # Fallback: compute epoch and use BSD date -r (macOS)
  local now target
  now=$(date -u +%s)
  target=$(( now + minutes_from_now * 60 ))
  date -u -r "$target" +"%Y-%m-%dT%H:%M:%SZ"
}

echo "Creating ${N_JOBS} Jobs in namespace=${NAMESPACE} using schedulerName=${SCHEDULER_NAME}"
echo "Deadlines: start in +${START_OFFSET_MIN}min, step +${STEP_MIN}min, annotation=${DEADLINE_ANNOTATION_KEY}"
echo

for i in $(seq 1 "$N_JOBS"); do
  # Make deadlines increase for later jobs (job 1 = earliest)
  minutes_from_now=$(( START_OFFSET_MIN - (i - 1) * STEP_MIN ))
  dl="$(deadline_rfc3339_utc "$minutes_from_now")"
  job_name="job-deadline-${i}"

  echo "Applying ${job_name} with deadline=${dl}"

  kubectl apply -n "${NAMESPACE}" -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: ${job_name}
  annotations:
    ${DEADLINE_ANNOTATION_KEY}: "${dl}"
spec:
  template:
    metadata:
      annotations:
        ${DEADLINE_ANNOTATION_KEY}: "${dl}"
    spec:
      schedulerName: "${SCHEDULER_NAME}"
      restartPolicy: Never
      containers:
      - name: compute-task
        image: busybox
        command: ["/bin/sh", "-c"]
        args:
        - |
          echo "Job ${i} starting. Deadline=${dl}"
          echo "Start(ts)=\$(date -u +%Y-%m-%dT%H:%M:%SZ)"
          sleep ${SLEEP_SECONDS}
          echo "Finish(ts)=\$(date -u +%Y-%m-%dT%H:%M:%SZ)"
EOF
done

echo
echo "Done. Check:"
echo "  kubectl get jobs -n ${NAMESPACE}"
echo "  kubectl get pods -n ${NAMESPACE}"
echo "  kubectl get events -n ${NAMESPACE} --sort-by=.lastTimestamp | tail -n 30"