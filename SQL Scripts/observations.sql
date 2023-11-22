-- Set the database names and person_id
DECLARE @db NVARCHAR(128) = 'patient_db'; -- Replace with the patient database name 
DECLARE @ohdsi_db NVARCHAR(128) = 'ohdsi_vocabulary_db'; -- Replace with the OHDSI vocabulary database name 
DECLARE @pid INT = 123456; -- Replace with the desired person_id (pid)

-- Create a dynamic SQL query with placeholders for the database names and person_id
DECLARE @sql NVARCHAR(MAX);

SET @sql = N'
SELECT a.type_id,
      a.type_label,
      CASE WHEN a.value_as_concept_id IS NULL THEN NULL
          ELSE CONCAT(a.modifier_vocab, '':'', a.modifier_code) END AS modifier_id,
      a.modifier_label,
      CASE WHEN a.value_as_string IS NULL THEN NULL
          ELSE a.value_as_string END AS description,
	  a.observation_datetime AS onset_timestamp,
      CONCAT(''P'', DATEDIFF(YEAR, a.birth_datetime, a.observation_datetime), ''Y'') AS onset_age
FROM (SELECT c.concept_name AS type_label,
        CONCAT(c.vocabulary_id, '':'', c.concept_code) AS type_id,
        obs.observation_concept_id,
        obs.value_as_concept_id,
        c2.concept_name AS modifier_label,
        c2.vocabulary_id AS modifier_vocab,
        c2.concept_code AS modifier_code,
        obs.value_as_string,
        obs.value_as_number,
        obs.observation_datetime,
        p.birth_datetime
    FROM ' + QUOTENAME(@db) + N'.dbo.observation obs
    LEFT JOIN ' + QUOTENAME(@ohdsi_db) + N'.dbo.concept c
    ON obs.observation_concept_id = c.concept_id
    LEFT JOIN ' + QUOTENAME(@db) + N'.dbo.person p
    ON obs.person_id = p.person_id
    LEFT JOIN ' + QUOTENAME(@ohdsi_db) + N'.dbo.concept c2
    ON obs.value_as_concept_id = c2.concept_id
    WHERE obs.person_id = @pid) a;';

-- Execute the dynamic SQL query with parameters
EXEC sp_executesql @sql, N'@pid INT', @pid;

