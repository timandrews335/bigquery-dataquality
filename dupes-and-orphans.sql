

CREATE TABLE duplicate_keys (
	time_detected_UTC timestamp,
	table_schema STRING,
	table_name STRING,
	cols STRING,
	duplicates INT
);


CREATE TABLE orphaned_facts (
	time_detected_UTC timestamp,
	table_schema STRING,
	table_name STRING,
	column_name STRING,
	orphans INT
);




CREATE PROCEDURE detect_uniques_and_orphans()
BEGIN

DECLARE stmt STRING;
SET stmt = '';


CREATE OR REPLACE TEMPORARY TABLE temp_uniques
AS
SELECT 
table_schema
, table_name
, CASE
    WHEN COLUMN_NAME LIKE 'pksk%' THEN 'primary key / surroage key'
    ELSE 'business key'
  END AS unique_type
, STRING_AGG(column_name) AS cols
, CONCAT('SELECT ', STRING_AGG(column_name), ' , COUNT(1) AS Ct FROM ', table_schema, '.', table_name, ' GROUP BY ', STRING_AGG(column_name), ' HAVING COUNT(1) > 1;' )AS debug_statement
, CONCAT('''
        INSERT INTO misc.duplicate_keys
        (time_detected_utc, table_schema ,table_name,cols,duplicates)
        SELECT current_timestamp() AS time_detected,  "''',  table_schema, '","' ,table_name , '","', STRING_AGG(column_name), '", COUNT(1) AS duplicates FROM (SELECT ', STRING_AGG(column_name), ' , COUNT(1) AS Ct FROM ', table_schema, '.', table_name, ' GROUP BY ', STRING_AGG(column_name), ' HAVING COUNT(1) > 1) x ; ') AS statement
, 0 AS processed
FROM misc.INFORMATION_SCHEMA.COLUMNS
WHERE COLUMN_NAME LIKE 'pksk_%' OR COLUMN_NAME LIKE 'bk_%'
GROUP BY 1, 2, 3;

--Get a list of foreign key columns to dimension surrogate keys that are being used in our fact tables
CREATE OR REPLACE TEMP TABLE temp_foreign_keys
AS
SELECT table_schema, table_name, column_name
FROM misc.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME LIKE 'fact%'
AND COLUMN_NAME LIKE 'fksk%';

--Determine which dimension table these go with
CREATE OR REPLACE TEMP TABLE temp_foreign_keys
AS
SELECT y.*
,CONCAT('INSERT INTO misc.orphaned_facts SELECT CURRENT_TIMESTAMP(), "', table_schema, '", "', table_name, '", "', column_name, '", (', statement_calc, ');') AS statement
FROM
(
  SELECT x.*
  ,CONCAT('SELECT COUNT(1) FROM ', table_schema, '.', table_name, ' WHERE ', column_name, ' NOT IN (SELECT ', dim_column ,' FROM ', dim_schema, '.', dim_table, ')') AS statement_calc
  , 0 AS processed
  FROM
  (
    SELECT a.*, b.table_schema AS dim_schema, b.table_name AS dim_table, b.cols AS dim_column
    FROM temp_foreign_keys a
    INNER JOIN temp_uniques b
      ON REPLACE(a.column_name, 'fksk', 'pksk') = b.cols
  ) x
) y;


--Process the possible duplicate keys
WHILE (SELECT COUNT(1) FROM temp_uniques WHERE Processed = 0) > 0 DO
  SET stmt = (SELECT MIN(Statement) FROM temp_uniques WHERE Processed = 0);
  EXECUTE IMMEDIATE (stmt);
  UPDATE temp_uniques SET Processed = 1 WHERE Statement = stmt;
END WHILE;

--Process the possible orphaned facts
WHILE (SELECT COUNT(1) FROM temp_foreign_keys WHERE Processed = 0) > 0 DO
  SET stmt = (SELECT MIN(Statement) FROM temp_foreign_keys WHERE Processed = 0);
  EXECUTE IMMEDIATE (stmt);
  UPDATE temp_foreign_keys SET Processed = 1 WHERE Statement = stmt;
END WHILE;

END;
