-- Set the database names and person_id
DECLARE @db NVARCHAR(128) = 'patient_db'; -- Replace with the patient database name 
DECLARE @ohdsi_db NVARCHAR(128) = 'ohdsi_vocabulary_2023q3r1'; -- Replace with the OHDSI vocabulary database name 
DECLARE @pid INT = 123456; -- Replace with the desired person_id (pid)

-- Create a dynamic SQL query with placeholders for the db name and pid
DECLARE @sql NVARCHAR(MAX);

SET @sql = N'
SELECT *
FROM (
    SELECT id AS person_id,
           (CASE WHEN pid_death.death_pid IS NULL THEN 0 ELSE 2 END) AS vital_status,
           pid_death.time_of_death,
           NULL AS cause_of_death_id,
           NULL AS cause_of_death_label
    FROM (
        SELECT p.person_id AS id, d.person_id AS death_pid, d.death_datetime AS time_of_death
        FROM ' + QUOTENAME(@db) + N'.dbo.person p
        LEFT JOIN ' + QUOTENAME(@db) + N'.dbo.death d
        ON p.person_id = d.person_id
        WHERE p.person_id = @pid
    ) pid_death
) p1
GROUP BY person_id, vital_status, time_of_death, cause_of_death_id, cause_of_death_label;';

-- Execute the dynamic SQL query with parameters
EXEC sp_executesql @sql, N'@pid INT', @pid;
