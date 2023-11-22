

-- Set the database names and person_id
DECLARE @db NVARCHAR(128) = 'patient_db'; -- Replace with the patient database name 
DECLARE @ohdsi_db NVARCHAR(128) = 'ohdsi_vocabulary_db'; -- Replace with the OHDSI vocabulary database name 
DECLARE @pid INT = 123456; -- Replace with the desired person_id (pid)

-- Create a dynamic SQL query with placeholders for the ohdsi_db name and pid
DECLARE @sql NVARCHAR(MAX);

SET @sql = N'
SELECT a.person_id,
      a.code_id,
      a.code_label,
      CASE WHEN a.body_site_concept_id IS NULL THEN NULL
          ELSE CONCAT(body_site_vocab_id, '':'', body_site_concept_id) END AS body_site_id,
      a.body_site_label,
      a.procedure_datetime AS performed_timestamp,
      CONCAT(''P'', DATEDIFF(YEAR, a.birth_datetime, a.procedure_datetime), ''Y'') AS performed_age
FROM (SELECT po.person_id,
        -- code
        CONCAT(c.vocabulary_id, '':'', c.concept_code) AS code_id,
        c.concept_name AS code_label,
        c2.concept_id AS body_site_concept_id,
        c2.vocabulary_id AS body_site_vocab_id,
        c2.concept_name AS body_site_label,
        po.procedure_datetime,
        p.birth_datetime
    FROM ' + QUOTENAME(@db) + N'.dbo.procedure_occurrence po
    LEFT JOIN ' + QUOTENAME(@ohdsi_db) + N'.dbo.concept c
    ON c.concept_id = po.procedure_concept_id
    LEFT JOIN ' + QUOTENAME(@ohdsi_db) + N'.dbo.concept_relationship cr
    ON cr.concept_id_1 = po.procedure_concept_id AND cr.relationship_id = ''Has proc site''
    LEFT JOIN ' + QUOTENAME(@ohdsi_db) + N'.dbo.concept c2
    ON c2.concept_id = cr.concept_id_2
    LEFT JOIN ' + QUOTENAME(@db) + N'.dbo.person p
    ON po.person_id = p.person_id
    WHERE po.person_id = @pid) a;';

-- Execute the dynamic SQL query with parameters
EXEC sp_executesql @sql, N'@pid NVARCHAR(50)', @pid;
