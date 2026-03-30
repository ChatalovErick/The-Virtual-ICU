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

CREATE OR REFRESH STREAMING TABLE patient_data.silver_patient_vitals.patient_vitals_cleaned
(
  -- Existing Heart Rate Constraint
  CONSTRAINT valid_heart_rate EXPECT (heart_rate > 20 AND heart_rate < 300) ON VIOLATION DROP ROW,
  
  -- New Oxygen Saturation Constraint
  CONSTRAINT valid_spo2 EXPECT (spo2 >= 50 AND spo2 <= 100) ON VIOLATION DROP ROW,
  
  -- New Respiratory Rate Constraint
  CONSTRAINT valid_resp_rate EXPECT (resp_rate > 4 AND resp_rate < 70) ON VIOLATION DROP ROW
)
AS SELECT
  patient_id,
  CAST(timestamp AS TIMESTAMP) as event_time,
  heart_rate_bpm AS heart_rate,
  -- Extracting from the JSON object in _rescued_data
  CAST(_rescued_data:respiratory_rate AS DOUBLE) as resp_rate,
  CAST(_rescued_data:spo2_percent AS DOUBLE) as spo2,
  current_state,
  -- CHANGE THIS LINE: Remove '_metadata.' and use the name from Bronze
  source_file_name as source_file,
  ingestion_time
FROM STREAM patient_data.bronze_patient_vitals.patient_vitals; 

CREATE OR REFRESH MATERIALIZED VIEW patient_data.gold_patient_vitals.patient_exercise_summary
AS SELECT
  patient_id,
  current_state,
  AVG(heart_rate) as avg_bpm,
  AVG(spo2) as avg_spo2,
  MIN(spo2) as min_spo2, -- Critical for identifying "Desaturation" events
  AVG(resp_rate) as avg_resp_rate,
  COUNT(*) as total_readings
FROM patient_data.silver_patient_vitals.patient_vitals_cleaned
GROUP BY patient_id, current_state