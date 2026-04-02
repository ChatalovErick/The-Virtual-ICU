# 🏥 Virtual ICU: Real-Time Remote Patient Monitoring (RPM)
### Declarative Data Pipelines (DLT), IoT Simulation & CDC Tracking

This project implements a production-grade **Medallion Architecture** using **Databricks Delta Live Tables (DLT)**. It simulates a "Virtual ICU" environment, merging high-frequency physiological telemetry with slowly changing patient demographics to provide a holistic clinical view.

---

## 🏗️ System Architecture
The pipeline follows the Medallion Architecture, utilizing two distinct streaming flows to handle different data velocities.

| Flow | Data Source | Type | Storage Strategy |
| :--- | :--- | :--- | :--- |
| **Telemetry** | IoT Sensors | Real-time | Append-only / Aggregated |
| **Demographics** | EHR Updates | Intermittent | **SCD Type 2** (Versioned History) |

### 1. 🥉 Bronze Layer: Raw Ingestion
* **Vitals:** Streams raw JSON from Unity Catalog Volumes using `read_files`. Includes a **Rescued Data Column** (`_rescued_data`) to handle schema evolution (e.g., new sensor metrics) without pipeline failure.
* **Profiles:** Ingests Change Data Capture (CDC) logs from the `bronze_patients_cdc` table, capturing operations like updates ("U") and deletes ("D").

### 2. 🥈 Silver Layer: Cleaning & Validation
* **Vitals Validation:** Uses DLT **Expectations** to enforce biological bounds:
    * `valid_heart_rate`: $20 < BPM < 300$
    * `valid_spo2`: $50\% < SpO2 < 100\%$
* **Profiles & Quarantining:** Unlike the vitals (which drop bad rows), the patient profile pipeline uses a `is_quarantined` flag for records with invalid ages or IDs. This allows the pipeline to continue while flagging records for clinical review.
* **SCD Type 2 Implementation:** Uses `APPLY CHANGES INTO` logic to track the history of patient changes (like weight fluctuations) over time, ensuring a full audit trail.

### 3. 🥇 Gold Layer: Clinical Aggregates
* **Materialized View:** `patient_exercise_summary`
* **Function:** Indexes patient health by activity state (`REST`, `EXERCISE`, `CRISIS`).
* **Key Metric:** Tracks **Minimum SpO2** to alert clinicians to "Desaturation" events during physical exertion.

---

## 🧬 Data Generation & Simulation
The environment is powered by two specialized simulation engines:

### 📡 Multi-Vital Patient Simulator (Telemetry)
Uses a **Stochastic Autoregression** model to simulate realistic physiological transitions:
$$V_{t} = V_{t-1} + \eta(\text{Target} - V_{t-1}) + \sigma$$
* **Correlations:** Respiratory rate and body temperature are logically tied to heart rate trends.
* **States:** Transitions between `REST`, `EXERCISE`, and `CRISIS` based on 10% drift ($\eta$) and Gaussian noise ($\sigma$).

### 📂 Patient Profile Update (EHR Simulation)
A Python-based simulator mimicking updates to an Electronic Health Record.
* **Logic:** Randomly fluctuates a patient's `weight_kg` by $\pm 2.0kg$.
* **CDC Signaling:** Appends records to Bronze with appropriate `op` codes to trigger the SCD Type 2 logic in the DLT pipeline.

---

## 📊 Vitals Dashboard: Clinical Insights
The final layer feeds a **Databricks SQL Dashboard**, providing real-time visualization:
* **Heart Rate Trends:** Visualizes spikes, specifically highlighting `CRISIS` states.
* **Oxygen Saturation:** Tracks $SpO2$ drops below 95% (Desaturation) across activity levels.
* **Interactive Filters:** Allows clinicians to drill down by `patient_id` or `current_state`.

---

## 🚀 Key Features
* **Declarative ETL:** Fully managed lineage using DLT—the system automatically builds the dependency graph.
* **CDC Excellence:** Seamlessly handles out-of-order data updates using `SEQUENCE BY` on the `updated_at` timestamp.
* **Schema-on-Read:** Extracts metrics from rescued data columns that weren't in the initial definition.
* **Incremental Processing:** Optimized for cost by only processing new files or changed rows.

---

## 🛠️ Setup & Usage
1.  **Initialize Environment:** Run the `01_setup_unity_catalog_hierarchy` notebook.
2.  **Generate Data:** * Execute the **Multi-Vital Patient Simulator** for real-time telemetry.
    * Run the **Patient Profile Update** notebook to generate CDC entries.
3.  **Deploy Pipeline:** Create a DLT pipeline targeting both `Patient Vitals.sql` and `Patient Profile.sql`.
4.  **Monitor:** Use the DLT UI to observe data quality metrics and the flow from Bronze to Gold.

> **Note:** This project is optimized for **Databricks Unity Catalog**. Ensure your cluster has access to the `patient_data` catalog before execution.