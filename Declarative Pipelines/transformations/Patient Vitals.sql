CREATE OR REFRESH STREAMING TABLE patient_data.bronze_patient_vitals.patient_vitals
AS SELECT * FROM STREAM read_files(
  "/Volumes/patient_data/ingestion_patient_vitals/raw_jsons", -- Use the direct path
  format => "json",
  rescuedDataColumn => "_rescued_data"
)