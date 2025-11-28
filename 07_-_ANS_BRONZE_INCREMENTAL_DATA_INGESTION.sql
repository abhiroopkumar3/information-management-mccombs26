-- TERM PROJECT
-- TEAM NAME: ANS
-- MEMBER NAMES: Abhiroop Kumar (ak56448), Nikhil Kumar (nk25627), Simoni K Dalal (skd939)
-- DATABASE: DB_TEAM_ANS
-- ROLE: ROLE_TEAM_ANS
-- DUE: Dec 8 at 11:59pm

-- FILE NAME: ANS_BRONZE_INCREMENTAL_DATA_INGESTION.sql

-- SOLUTION

------------------------------------------------------------------
-- 0. SET CONTEXT
------------------------------------------------------------------
USE ROLE ROLE_TEAM_ANS;
USE WAREHOUSE ANIMAL_TASK_WH;
USE DATABASE DB_TEAM_ANS;
USE SCHEMA DB_TEAM_ANS.BRONZE;

------------------------------------------------------------------
-- 1. CREATE INTERNAL NAMED STAGE IN BRONZE
------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS DCAMP_BRONZE_STAGE_INCREMENT;

-- Optional sanity checks
DESCRIBE STAGE DCAMP_BRONZE_STAGE_INCREMENT;

-- After upload
LIST @DCAMP_BRONZE_STAGE_INCREMENT;

-- Optional check: File Format status
DESCRIBE FILE FORMAT DCAMP_BRONZE_CSV_FF;

------------------------------------------------------------------
-- 2. CREATE SNOWPIPES
------------------------------------------------------------------

------------------------------------------------------------------
-- 2.1 COURSES_INCREMENTAL
------------------------------------------------------------------
CREATE or REPLACE PIPE COURSES_INGEST_PIPE
AUTO_INGEST = TRUE
AS 
COPY INTO DCAMP_COURSES_BRONZE
FROM @dcamp_bronze_stage_increment/
PATTERN = '.*courses_.*\\.csv'
FILE_FORMAT = (FORMAT_NAME = DCAMP_BRONZE_CSV_FF);

-- Optional check
SELECT SYSTEM$PIPE_STATUS('COURSES_INGEST_PIPE'); 

-- Upload the CSV file and view the Bronze table
SELECT * FROM DCAMP_COURSES_BRONZE;

------------------------------------------------------------------
-- 2.2 ALL_TRACKS_INCREMENTAL
------------------------------------------------------------------
CREATE or REPLACE PIPE ALL_TRACKS_INGEST_PIPE
AUTO_INGEST = TRUE
AS 
COPY INTO DCAMP_ALL_TRACKS_BRONZE
FROM @dcamp_bronze_stage_increment/
PATTERN = '.*all_tracks_.*\\.csv'
FILE_FORMAT = (FORMAT_NAME = DCAMP_BRONZE_CSV_FF);

-- Optional check
SELECT SYSTEM$PIPE_STATUS('ALL_TRACKS_INGEST_PIPE'); 

-- Upload the CSV file and view the Bronze table
SELECT * FROM DCAMP_ALL_TRACKS_BRONZE;

------------------------------------------------------------------
-- 2.3 TOPIC_MAPPING_INCREMENTAL
------------------------------------------------------------------
CREATE or REPLACE PIPE TOPIC_MAPPING_INGEST_PIPE
AUTO_INGEST = TRUE
AS 
COPY INTO DCAMP_TOPIC_MAPPING_BRONZE
FROM @dcamp_bronze_stage_increment/
PATTERN = '.*topic_mapping_.*\\.csv'
FILE_FORMAT = (FORMAT_NAME = DCAMP_BRONZE_CSV_FF);

-- Optional check
SELECT SYSTEM$PIPE_STATUS('TOPIC_MAPPING_INGEST_PIPE'); 

-- Upload the CSV file and view the Bronze table
SELECT * FROM DCAMP_TOPIC_MAPPING_BRONZE;

------------------------------------------------------------------
-- 2.4 TECHNOLOGY_MAPPING_INCREMENTAL
------------------------------------------------------------------
CREATE or REPLACE PIPE TECHNOLOGY_MAPPING_INGEST_PIPE
AUTO_INGEST = TRUE
AS 
COPY INTO DCAMP_TECHNOLOGY_MAPPING_BRONZE
FROM @dcamp_bronze_stage_increment/
PATTERN = '.*technology_mapping_.*\\.csv'
FILE_FORMAT = (FORMAT_NAME = DCAMP_BRONZE_CSV_FF);

-- Optional check
SELECT SYSTEM$PIPE_STATUS('TECHNOLOGY_MAPPING_INGEST_PIPE'); 

-- Upload the CSV file and view the Bronze table
SELECT * FROM DCAMP_TECHNOLOGY_MAPPING_BRONZE;

------------------------------------------------------------------
-- END OF SCRIPT
------------------------------------------------------------------