
-- Set the database names and person_id
DECLARE @db NVARCHAR(128) = 'patient_db'; -- Replace with the patient database name 
DECLARE @ohdsi_db NVARCHAR(128) = 'ohdsi_vocabulary_2023q3r1'; -- Replace with the OHDSI vocabulary database name 
DECLARE @pid INT = 123456; -- Replace with the desired person_id (pid)

-- Create a dynamic SQL query with placeholders for the db name and pid
DECLARE @sql NVARCHAR(MAX);

SET @sql = N'
SELECT p3.id,
       NULL AS alternate_ids,
       p3.date_of_birth,
       MAX(vo.visit_start_date) AS time_at_last_encounter,
       p3.vital_status,
       p3.sex,
       NULL AS karyotypic_sex,
       NULL AS gender,
       ''NCBITaxon:9606'' AS taxonomy_id,
       ''human'' AS taxonomy_label
FROM (
    SELECT p2.id, p2.date_of_birth, p2.sex,
           (CASE WHEN p2.death_pid IS NULL THEN 0 ELSE 2 END) AS vital_status
    FROM (
        SELECT p1.*, d.person_id AS death_pid
        FROM (
            SELECT p.person_id AS id,
                   p.birth_datetime AS date_of_birth,
                   (CASE WHEN p.gender_concept_id IS NULL THEN 0
                         WHEN p.gender_concept_id = 8532 THEN 1
                         WHEN p.gender_concept_id = 8507 THEN 2
                         ELSE 3 END) AS sex
            FROM ' + QUOTENAME(@db) + N'.dbo.person p
            WHERE p.person_id = @pid
        ) p1
        LEFT JOIN ' + QUOTENAME(@db) + N'.dbo.death d
        ON p1.id = d.person_id
    ) p2
) p3
LEFT JOIN ' + QUOTENAME(@db) + N'.dbo.visit_occurrence vo
ON p3.id = vo.person_id
GROUP BY p3.id, p3.sex, p3.date_of_birth, p3.vital_status;';

-- Execute the dynamic SQL query with parameters
EXEC sp_executesql @sql, N'@pid INT', @pid;
