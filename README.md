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

---

## 🛠️ Setup & Configuration

### Prerequisites
- Databricks CLI installed and authenticated
- Access to a Databricks workspace with Unity Catalog enabled
- Python 3.8+ for data generation scripts

### ⚠️ Required Configuration Changes

Before deploying this project, you **must** update the following placeholders in `databricks.yml`:

1. **Workspace URL** (Line 6):
   ```yaml
   workspace:
     host: https://<your-workspace-url>  # Replace with your actual workspace URL
   ```
   Example: `https://adb-1234567890123456.15.azuredatabricks.net`

2. **Notification Email** (Line 24):
   ```yaml
   on_failure:
     - email: <your-email>  # Replace with your email address for failure alerts
   ```
   Example: `john.doe@company.com`

Failure to update these values will result in deployment errors or missing notifications.

### 🚀 Deployment Steps

1. **Clone and Navigate**:
   ```bash
   git clone <repository-url>
   cd <project-directory>
   ```

2. **Configure `databricks.yml`**:
   Edit the file and replace the placeholder values mentioned above.

3. **Validate Bundle**:
   ```bash
   databricks bundle validate
   ```

4. **Deploy to Workspace**:
   ```bash
   databricks bundle deploy
   ```

5. **Run Jobs**:
   Once deployed, jobs will run automatically based on their schedules, or you can trigger them manually from the Databricks UI.

---

## 📁 Project Structure

```
├── databricks.yml              # DABs configuration (update workspace & email here!)
├── README.md                   # This file
├── Vitals Dashboard.lvdash.json # Lakeview dashboard definition
├── 01_setup_unity_catalog_hierarchy.ipynb  # Initial catalog/schema setup
├── querytests.dbquery.ipynb    # Query validation tests
├── data-generator/             # Data simulation scripts
│   ├── data_generator.ipynb           # Multi-vital patient telemetry simulator
│   ├── patient_profile_update.ipynb   # EHR/CDC update simulator
│   └── injestion_cleanup.ipynb        # Volume cleanup utilities
└── declarative-pipelines/
    └── transformations/        # DLT pipeline SQL definitions
        ├── patient_vitals.sql       # Telemetry pipeline (Bronze→Silver→Gold)
        └── patient_profile.sql      # SCD Type 2 profile pipeline
```

---

## 🔍 Monitoring & Alerts

- **Pipeline Health**: Monitor DLT pipeline runs in the Databricks Workflows UI
- **Failure Notifications**: Email alerts are configured in `databricks.yml` (ensure you've updated the email address)
- **Data Quality**: Review DLT Expectations violations in the pipeline event logs
- **Dashboard Refresh**: The Vitals Dashboard auto-refreshes based on Gold layer updates