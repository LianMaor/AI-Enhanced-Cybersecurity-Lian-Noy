# Lab 2b Reflection: Evidence vs. Intuition (`burst` Profile)

- **Students:** Lian Maor & Noy Mlynek
- **Baseline Run ID (`normal`):** `bf002cbc-5dcb-4104-b692-567849759a27`
- **High-Load Run ID (`burst`):** `568d81fd-e002-47fd-ab63-200d7812889e`

## 1. What did the dashboard initially suggest?
During and immediately after the `burst` run, the Lab 2b dashboard displayed a red critical alert (`System status: OVERLOADED — Processing demand exceeds safe capacity; backlog or utilization is in the critical zone`), alongside a **Full-pipeline E2E latency of 70,182.4 ms** and **96.9% Detector utilization**. At first glance, viewing the post-drain dashboard snapshot where both `Ingress backlog` and `Alert backlog` showed `0 / 500`, it could intuitively appear as though queue congestion had never exceeded the 500-message gauge or that the pipeline's ML/agent computation itself required ~70 seconds per event. Additionally, seeing `OVERLOADED` during the `normal` run snapshot (where Detector utilization briefly reached 94.4% and E2E latency hit 3,222.0 ms) initially obscured how dramatically the `burst` workload amplified queueing delay.

## 2. What did the CSV confirm or contradict?
Analyzing the exported time-series CSVs (`lab2b_normal_bf002cbc_metrics.csv` and `lab2b_burst_568d81fd_metrics.csv`) confirmed that the **Detector** was the sole bottleneck, but contradicted the intuition that slow ML inference or Orchestrator agent processing caused the 70-second latency:
- **Queue Wait vs. Active Execution:** The CSV proved that active processing time in both stages remained fast and nearly unchanged under burst load (**44.49 ms** in the Detector and **31.16 ms** in the Orchestrator, compared to 41.05 ms and 32.21 ms in `normal`). Over **99.5%** of the 33,516.72 ms mean E2E latency came purely from messages waiting in the `raw.logs` Kafka topic for the single-threaded Detector.
- **Hidden Ingress Queue Depth:** Reconstructing cumulative counters from the CSV revealed that the `raw.logs` ingress queue actually swelled to **1,381 buffered messages** (more than 2.7x the dashboard's 500-message visual scale) and took **72.04 seconds** to drain after the generator stopped.
- **Orchestrator Headroom:** Contrary to any assumption that downstream XAI/Meta-AI orchestration is the slowest stage in an agentic SOC, the CSV demonstrated that the Orchestrator was rate-limited by the Detector's ~21 events/s ceiling and never exceeded a 13-alert backlog.

## 3. What uncertainty remains in your bottleneck diagnosis?
While the CSV isolates the Detector process as the throughput bottleneck (~21.17 events/s), two sources of measurement uncertainty remain:
1. **Internal Sub-Stage Breakdown inside the Detector:** The CSV captures aggregate `stage_latency_ms` (44.49 ms) for the Detector, without separating time spent in **Isolation Forest scoring**, **user behavioral fingerprinting state updates**, and **synchronous Kafka polling/serialization**.
2. **Local Host Resource Contention:** Because Redpanda (Docker), the Generator, Detector, Orchestrator, and Streamlit Dashboard all executed concurrently on a single local Windows machine, CPU context-switching and disk I/O contention during the 273.57 events/s burst spike may have slightly reduced effective Detector throughput compared to an isolated multi-core deployment.

## 4. What would you change in the architecture and why?
To eliminate the ingestion bottleneck without compromising detection quality, we would implement **state-aware horizontal partitioning on `raw.logs` combined with micro-batch inference in the Detector**:
- **Why:** Currently, a single Detector consumer processes events sequentially at a ~22 events/s ceiling. Expanding `raw.logs` to 4 partitions keyed by `user_id` allows 4 parallel Detector workers to scale ingestion capacity to ~85–90 events/s while preserving per-user behavioral fingerprinting state locality within each partition. Scoring events in small micro-batches (e.g., 16–32 events per `IsolationForest` call) would further amortize Python/scikit-learn per-event overhead and absorb short bursts without multi-second queueing delays.
