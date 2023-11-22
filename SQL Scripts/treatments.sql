
-- Set the database names and person_id
DECLARE @db NVARCHAR(128) = 'patient_db'; -- Replace with the patient database name 
DECLARE @ohdsi_db NVARCHAR(128) = 'ohdsi_vocabulary_db'; -- Replace with the OHDSI vocabulary database name 
DECLARE @pid INT = 123456; -- Replace with the desired person_id (pid)


-- Create a dynamic SQL query with placeholders for the ohdsi_db name and pid
DECLARE @sql NVARCHAR(MAX);

SET @sql = N'
SELECT a.person_id,
      a.agent_id,
      a.agent_label,
      CASE WHEN a.route_administration_code IS NULL THEN NULL
          ELSE CONCAT(a.route_administration_vocab, '':'', a.route_administration_code) END AS route_of_adminsitration_id,
      a.route_of_adminsitration_label,
      CASE WHEN a.quantity_code_id IS NULL THEN NULL
          ELSE CONCAT(a.quantity_vocab_id, '':'', a.quantity_code_id) END AS quantity_id,
     a.quantity_unit_label,
     a.quantity_value,
     a.interval_start,
     a.interval_end,
     a.drug_type_id,
     a.sched_freq
FROM (SELECT de.person_id,
        -- Agent
        CONCAT(c.vocabulary_id, '':'', c.concept_code) AS agent_id,
        c.concept_name AS agent_label,
        -- Route of administration
        c2.vocabulary_id AS route_administration_vocab,
        c2.concept_code AS route_administration_code,
        c2.concept_name AS route_of_adminsitration_label,
        -- schedule_Freq
        CASE WHEN de.days_supply = 0 THEN 0 ELSE CEILING(de.quantity / de.days_supply) END AS sched_freq,
        -- Dose Intervals
        -- dose intervals: dosage
        ds.amount_value AS quantity_value,
        c3.vocabulary_id AS quantity_vocab_id,
        c3.concept_code AS quantity_code_id,
        c3.concept_name AS quantity_unit_label,
        -- dose intervals: interval start/end
        de.drug_exposure_start_date AS interval_start,
        DATEADD(day, de.days_supply, de.drug_exposure_start_date) AS interval_end,
        -- Drug_type
        c4.concept_id AS drug_type_id,
        c4.concept_name AS drug_type_name
    FROM ' + QUOTENAME(@db) + N'.dbo.drug_exposure de
    LEFT JOIN ' + QUOTENAME(@ohdsi_db) + N'.dbo.concept c
    ON c.concept_id = de.drug_concept_id
    LEFT JOIN ' + QUOTENAME(@ohdsi_db) + N'.dbo.concept c2 ON c2.concept_id = de.route_concept_id
    LEFT JOIN ' + QUOTENAME(@ohdsi_db) + N'.dbo.drug_strength ds
    ON ds.drug_concept_id = de.drug_concept_id
    LEFT JOIN ' + QUOTENAME(@ohdsi_db) + N'.dbo.concept c3
    ON c3.concept_id = ds.amount_unit_concept_id
    LEFT JOIN ' + QUOTENAME(@ohdsi_db) + N'.dbo.concept c4
    ON c4.concept_id = de.drug_type_concept_id
    WHERE de.person_id = @pid) a;';

-- Execute the dynamic SQL query with parameters
EXEC sp_executesql @sql, N'@pid NVARCHAR(50)', @pid;
