# 🏥 Virtual ICU: Real-Time Remote Patient Monitoring (RPM)
### Declarative Data Pipelines (DLT), IoT Simulation & CDC Tracking

This project implements a production-grade **Medallion Architecture** using **Databricks Delta Live Tables (DLT)**. It simulates a "Virtual ICU" environment, merging high-frequency physiological telemetry with slowly changing patient demographics to provide a holistic clinical view.

> 💡 **Now GitOps-Ready**: This project uses **Databricks Asset Bundles (DABs)** for declarative infrastructure. Deploy pipelines, jobs, and configurations from code via `databricks.yml`.

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
* **GitOps-Ready Infrastructure**: Deploy pipelines, jobs, and configs from code via `databricks.yml` + Databricks CLI.

---

## ⚙️ Declarative Infrastructure: `databricks.yml`
This project uses **Databricks Asset Bundles (DABs)** to define all resources as code. The `databricks.yml` file declares:

### 📦 Resources Defined
| Resource Type | Name | Purpose |
|--------------|------|---------|
| `pipelines` | `patient_vitals_declarative` | DLT pipeline for telemetry (Bronze→Silver→Gold) |
| `pipelines` | `patient_profiles_scd2` | DLT pipeline for patient profiles (SCD Type 2) |
| `jobs` | `etl_pipeline_patient_vitals` | Scheduled job: Generate data → Run vitals pipeline (every 6h) |
| `jobs` | `ingestion_layer_cleanup` | Daily cleanup of raw JSON volume |
| `jobs` | `patient_data_silver_scd2_job` | Daily refresh of patient profile pipeline |
| `jobs` | `patient_profile_update_spark_job` | CDC update simulation (every 3h) |