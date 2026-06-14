# Kube Custom Scheduler: Deadline-Aware (EDF)

A Kubernetes custom scheduler project focused on **Deadline-Aware Scheduling** using the **Earliest Deadline First (EDF)** algorithm in the `QueueSort` phase.

## Project Structure

- `main.go`: Entry point for the custom scheduler.
- `plugins/`: Contains the `EDFQueueSort` plugin logic.
- `scripts/`: Automation scripts for running experiments (`run_edf.sh`, `run_slack.sh`, etc.).
- `experiments/`: Input data, configurations, and *stuffer jobs* used to condition the queue.
- `deploy/`: Kubernetes manifests for scheduler deployment and job templates.

## Quick Start

### 1. Build & Push Image
```bash
docker build -t <your-image-tag> .
docker push <your-image-tag>
```

### 2. Deploy to Cluster
Ensure the manifests in `deploy/manifests/` point to your image, then apply:
```bash
kubectl apply -f deploy/manifests/
```

### 3. Run Experiments
Execute the desired scenario from the scripts directory:
```bash
# Example: Running the EDF experiment
bash scripts/run_edf.sh all
```

## Key Features
- **EDF QueueSort**: Prioritizes pods based on the `scheduling/deadline` annotation.
- **Poisson Arrival Replay**: Simulates realistic pod arrival patterns.
- **Queue Stuffing**: Includes mechanisms to pre-fill the cluster state before main experiments.
