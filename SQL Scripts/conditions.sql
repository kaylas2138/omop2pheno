-- Set the database names and person_id
DECLARE @db NVARCHAR(128) = 'patient_db'; -- Replace with the patient database name 
DECLARE @ohdsi_db NVARCHAR(128) = 'ohdsi_vocabulary_db'; -- Replace with the OHDSI vocabulary database name 
DECLARE @pid INT = 123456; -- Replace with the desired person_id (pid)

-- Create a dynamic SQL query with placeholders for the ohdsi_db name and pid
DECLARE @sql NVARCHAR(MAX);

SET @sql = N'
SELECT a.person_id,
      a.term_id,
      a.term_label,
      a.condition_source_value,
      a.excluded,
      a.onset_timestamp,
      a.resolution,
      a.clinical_tnm_finding_id,
      a.clinical_tnm_finding_label,
      CASE WHEN a.primary_site_concept IS NULL THEN NULL
          ELSE CONCAT(primary_site_vocab, '':'', primary_site_code)
          END AS primary_site_id,
      a.primary_site_label,
      a.concept_id
FROM (SELECT co.person_id,
        -- TERM
        CONCAT(c.vocabulary_id, '':'', c.concept_code) AS term_id,
        c.concept_name AS term_label,
        c.concept_id,
        co.condition_source_value,
        -- Excluded
        0 AS excluded,
        -- Onset
        co.condition_start_date AS onset_timestamp,
        co.condition_end_date AS resolution,
        -- Clinical_tnm_finding
        NULL AS clinical_tnm_finding_id,
        NULL AS clinical_tnm_finding_label,
        -- Primary Site
        cr.concept_id_2 AS primary_site_concept,
        c2.concept_name AS primary_site_label,
        c2.vocabulary_id AS primary_site_vocab,
        c2.concept_code AS primary_site_code
    FROM ' + QUOTENAME(@db) + N'.dbo.condition_occurrence co
    LEFT JOIN ' + QUOTENAME(@ohdsi_db) + N'.dbo.concept c
    ON co.condition_concept_id = c.concept_id
    LEFT JOIN ' + QUOTENAME(@ohdsi_db) + N'.dbo.concept_relationship cr
    ON cr.concept_id_1 = co.condition_concept_id AND cr.relationship_id = ''Has finding site''
    LEFT JOIN ' + QUOTENAME(@ohdsi_db) + N'.dbo.concept c2
    ON cr.concept_id_2 = c2.concept_id
    WHERE co.person_id = @pid) a;';

-- Execute the dynamic SQL query with parameters
EXEC sp_executesql @sql, N'@pid NVARCHAR(50)', @pid;
