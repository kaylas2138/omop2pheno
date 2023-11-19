-- Set the database names and person_id
DECLARE @db NVARCHAR(128) = 'patient_db'; -- Replace with the patient database name 
DECLARE @ohdsi_db NVARCHAR(128) = 'ohdsi_vocabulary_2023q3r1'; -- Replace with the OHDSI vocabulary database name 
DECLARE @pid INT = 123456; -- Replace with the desired person_id (pid)

-- Create a dynamic SQL query with placeholders for the ohdsi_db name and pid
DECLARE @sql NVARCHAR(MAX);

SET @sql = N'
SELECT m.person_id,
       m.measurement_concept_id,
     -- ASSAY
    CONCAT(c.vocabulary_id, '':'', c.concept_code) AS assay_id,
    c.concept_name AS assay_label,
    -- VALUE - QUANTITY/NUMERIC
    m.value_as_number,
     -- VALUE - ORDINAL/Categorical/OntologyClass
    CONCAT(c3.vocabulary_id, '':'', c3.concept_code) AS value_id,
    c3.concept_name AS value_label,
    -- RANGe
    m.range_low,
    m.range_high,
    -- time_observed
    m.measurement_datetime,
    -- UNIT
    CONCAT(c2.vocabulary_id, '':'', c2.concept_code) AS unit_id,
    c2.concept_name AS unit_label,
    c2.concept_id,
    m.unit_source_value,
    m.visit_occurrence_id,
    ROW_NUMBER() OVER (PARTITION BY m.person_id, m.measurement_datetime, m.visit_occurrence_id ORDER BY m.person_id) AS row_number
FROM ' + QUOTENAME(@db) + N'.dbo.measurement m
LEFT JOIN ' + QUOTENAME(@ohdsi_db) + N'.dbo.concept c ON c.concept_id = m.measurement_concept_id
LEFT JOIN ' + QUOTENAME(@ohdsi_db) + N'.dbo.concept c2 ON c2.concept_id = m.unit_concept_id
LEFT JOIN ' + QUOTENAME(@ohdsi_db) + N'.dbo.concept c3 ON c3.concept_id = m.value_as_concept_id
WHERE m.person_id = @pid;';

-- Execute the dynamic SQL query with parameters
EXEC sp_executesql @sql, N'@pid NVARCHAR(50)', @pid;
