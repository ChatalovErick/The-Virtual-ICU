
CREATE OR REFRESH STREAMING TABLE patient_data.silver_patients.patients_prepared
(
  CONSTRAINT valid_id  EXPECT (patient_id IS NOT NULL),
  CONSTRAINT valid_age EXPECT (age > 0 AND age < 150)
)
PARTITIONED BY (is_quarantined)
COMMENT "Intermediate table flagging data quality issues"
AS SELECT *,
  NOT (
    patient_id IS NOT NULL AND 
    age > 0 AND 
    age < 150
  ) AS is_quarantined
FROM STREAM(patient_data.bronze_patients.bronze_patients_cdc);

CREATE OR REFRESH STREAMING TABLE patient_data.silver_patients.patients_profiles
COMMENT "Final SCD Type 2 table containing only valid patient history";

CREATE FLOW apply_cdc AS AUTO CDC INTO
  patient_data.silver_patients.patients_profiles
FROM
  (SELECT * FROM STREAM(patient_data.silver_patients.patients_prepared) WHERE is_quarantined = FALSE)
KEYS
  (patient_id)
APPLY AS DELETE WHEN
  op = "D"
SEQUENCE BY
  updated_at
COLUMNS * EXCEPT
  (op, is_quarantined)
STORED AS
  SCD TYPE 2