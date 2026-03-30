# 🏥 Virtual ICU: Real-Time Remote Patient Monitoring (RPM)
### Declarative Data Pipeline (DLT) & IoT Simulation

This project implements a production-grade **Medallion Architecture** using **Databricks Delta Live Tables (DLT)**. It simulates a "Virtual ICU" environment, ingesting raw physiological JSON data from medical IoT sensors and transforming it into high-value clinical insights and aggregates.

---

## 🏗️ System Architecture
The pipeline follows the Medallion Architecture, moving data from raw ingestion to validated records and finally to clinical summaries.

### 1. 🥉 Bronze Layer: Raw Ingestion
* **Table:** `patient_data.bronze_patient_vitals.patient_vitals`
* **Mechanism:** Uses `read_files` to stream raw JSON data from Unity Catalog Volumes.
* **Schema Evolution:** Employs a **Rescued Data Column** (`_rescued_data`) to capture unexpected fields (e.g., `spo2`, `respiratory_rate`) without failing the pipeline.
* **Auditability:** Automatically captures `file_name` and `file_modification_time`.

### 2. 🥈 Silver Layer: Cleaning & Validation
* **Table:** `patient_data.silver_patient_vitals.patient_vitals_cleaned`
* **Quality Enforcement:** Uses DLT **Expectations** to drop records that violate biological bounds:

| Constraint | Logic | Action |
| :--- | :--- | :--- |
| `valid_heart_rate` | $20 < BPM < 300$ | `DROP ROW` |
| `valid_spo2` | $50\% < SpO2 < 100\%$ | `DROP ROW` |
| `valid_resp_rate` | $4 < \text{Resp Rate} < 70$ | `DROP ROW` |

### 3. 🥇 Gold Layer: Clinical Aggregates
* **Materialized View:** `patient_data.gold_patient_vitals.patient_exercise_summary`
* **Function:** Provides a summarized view of patient health indexed by activity state (`REST`, `EXERCISE`, `CRISIS`).
* **Key Metric:** Tracks **Minimum SpO2** to alert clinicians to "Desaturation" events during physical exertion.

---

## 📊 Vitals Dashboard: Clinical Insights
The final layer of the pipeline feeds a **Databricks SQL Dashboard**, providing real-time visualization of patient health trends. This allows medical staff to distinguish between normal activity and clinical emergencies.

* **Heart Rate Trends (`avg_bpm`):** Tracks heart rate spikes. Blue markers indicate a `CRISIS` state, clearly visible against the green `REST` baseline.
* **Respiratory Analysis (`avg_resp_rate`):** Monitors breathing stability. Visualizes how respiratory rate correlates with physiological stress.
* **Oxygen Saturation (`avg_spo2`):** A critical safety metric. Tracks $SpO2$ levels to spot "Desaturation" events (drops below 95%) across different activity levels.
* **Interactive Filters:** Dropdowns for `current_state` and `patient_id` allow clinicians to drill down into specific patient profiles or event types.

---

## 🧬 Data Generation: Multi-Vital Patient Simulator
To power the pipeline, a stochastic generator simulates high-fidelity telemetry for a single patient (100 data points).

### Physiological Modeling
The simulator uses a **Stochastic Autoregression** model to ensure transitions between states feel biological rather than random:
$$V_{t} = V_{t-1} + \eta(\text{Target} - V_{t-1}) + \sigma$$

* **States:** Transitions between `REST`, `EXERCISE`, and `CRISIS`.
* **Correlations:** Respiratory rate and body temperature are logically tied to heart rate trends.
* **Drift ($\eta$):** A 10% pull toward the target state value.
* **Volatility ($\sigma$):** Gaussian noise to simulate sensor jitter.

### Schema Specification
| Field | Type | Description |
| :--- | :--- | :--- |
| `patient_id` | String | Unique identifier (e.g., "PT-542") |
| `heart_rate_bpm`| Float | Simulated heart rate |
| `spo2_percent` | Float | Blood oxygen saturation |
| `current_state` | String | `REST`, `EXERCISE`, or `CRISIS` |
| `sequence_id` | Integer | Incremental counter for ordering |

---

## 🚀 Key Features
* **Declarative ETL:** Managed lineage using `STREAM` keywords—DLT automatically builds the dependency graph.
* **Schema-on-Read:** Demonstrates flexibility by extracting metrics from rescued data columns that were not in the initial Bronze definition.
* **Incremental Processing:** Streaming tables ensure only new files are processed, optimizing cost and performance.

---

## 🛠️ Setup & Usage
1.  **Initialize Environment:** Run the `01_setup_unity_catalog_hierarchy` notebook to create the `patient_data` catalog and required schemas.
2.  **Generate Data:** Execute the **Multi-Vital Patient Simulator**. It will write 100 JSON files to the Unity Catalog Volume at 5-second intervals.
3.  **Deploy Pipeline:** Create a Delta Live Tables pipeline targeting the `Patient Vitals.sql` source file.
4.  **Monitor:** Use the DLT UI to observe data quality metrics and the flow from Bronze to Gold.

> **Note:** This project is optimized for **Databricks Unity Catalog**. Ensure your cluster has access to the `patient_data` catalog before execution.