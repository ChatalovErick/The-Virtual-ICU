CREATE OR REFRESH STREAMING TABLE patient_data.bronze_patient_vitals.patient_vitals
AS SELECT *, 
  _metadata.file_name AS source_file_name, -- Explicitly promote the hidden column
  _metadata.file_modification_time AS ingestion_time
FROM STREAM read_files(
  "/Volumes/patient_data/ingestion_patient_vitals/raw_jsons", -- Use the direct path
  format => "json",
  schema => "patient_id STRING, timestamp STRING, heart_rate_bpm DOUBLE, current_state STRING, sequence_id INT",
  rescuedDataColumn => "_rescued_data"
);

-- The Flagging Table
CREATE OR REFRESH STREAMING TABLE patient_data.silver_patient_vitals.vitals_prepared
(
  CONSTRAINT valid_heart_rate EXPECT (heart_rate > 20 AND heart_rate < 300),
  CONSTRAINT valid_spo2       EXPECT (spo2 >= 50 AND spo2 <= 100),
  CONSTRAINT valid_resp_rate   EXPECT (resp_rate > 4 AND resp_rate < 70)
)
COMMENT "Intermediate table flagging physiological outliers for clinical review"
AS SELECT 
  *,
  -- Logic: If ANY constraint is violated, mark as quarantined
  NOT (
    (heart_rate > 20 AND heart_rate < 300) AND
    (spo2 >= 50 AND spo2 <= 100) AND
    (resp_rate > 4 AND resp_rate < 70)
  ) AS is_quarantined
FROM (
  SELECT
    patient_id,
    CAST(timestamp AS TIMESTAMP) as event_time,
    heart_rate_bpm AS heart_rate,
    CAST(_rescued_data:respiratory_rate AS DOUBLE) as resp_rate,
    CAST(_rescued_data:spo2_percent AS DOUBLE) as spo2,
    current_state,
    source_file_name as source_file,
    ingestion_time
  FROM STREAM(patient_data.bronze_patient_vitals.patient_vitals)
);

-- clean data 
CREATE OR REFRESH STREAMING TABLE patient_data.silver_patient_vitals.patient_vitals_cleaned
COMMENT "Cleaned telemetry data safe for clinical dashboards"
AS SELECT * EXCEPT (is_quarantined)
FROM STREAM(patient_data.silver_patient_vitals.vitals_prepared)
WHERE is_quarantined = FALSE;

-- quarantine data
CREATE OR REFRESH STREAMING TABLE patient_data.silver_patient_vitals.patient_vitals_quarantine
COMMENT "Isolated records failing biological bounds for sensor debugging"
AS SELECT * FROM STREAM(patient_data.silver_patient_vitals.vitals_prepared)
WHERE is_quarantined = TRUE;

-- 1. Daily Summary per Patient
CREATE OR REFRESH MATERIALIZED VIEW patient_data.gold_patient_vitals.patient_daily_summary
AS SELECT
  patient_id,
  date_trunc('day', event_time) as report_day,
  current_state,
  -- Averages for all tracked vitals
  AVG(heart_rate) as avg_bpm,
  AVG(resp_rate) as avg_resp_rate,
  AVG(spo2) as avg_spo2,
  -- Use ingestion time as a metadata tracker
  MAX(ingestion_time) as last_processed_at 
FROM patient_data.silver_patient_vitals.patient_vitals_cleaned
GROUP BY patient_id, current_state, report_day;

-- 2. Population Benchmarks
CREATE OR REFRESH MATERIALIZED VIEW patient_data.gold_patient_vitals.global_vital_stats
COMMENT "Population-level benchmarks for heart rate, SpO2, and Respiratory Rate across all patients."
AS SELECT
  date_trunc('day', event_time) as observation_date,
  current_state,
  -- Heart Rate Stats
  AVG(heart_rate) as pop_avg_hr,
  STDDEV(heart_rate) as hr_standard_deviation,
  -- SpO2 Stats
  AVG(spo2) as pop_avg_spo2,
  STDDEV(spo2) as spo2_standard_deviation,
  -- Respiratory Rate Stats
  AVG(resp_rate) as pop_avg_resp_rate,
  STDDEV(resp_rate) as resp_standard_deviation,
  -- Metadata
  COUNT(DISTINCT patient_id) as unique_patients_monitored
FROM patient_data.silver_patient_vitals.patient_vitals_cleaned
GROUP BY observation_date, current_state;