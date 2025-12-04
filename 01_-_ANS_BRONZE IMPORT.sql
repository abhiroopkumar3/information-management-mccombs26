-- TERM PROJECT
-- TEAM NAME: ANS
-- MEMBER NAMES: Abhiroop Kumar (ak56448), Nikhil Kumar (nk25627), Simoni K Dalal (skd939)
-- DATABASE: DB_TEAM_ANS
-- ROLE: ROLE_TEAM_ANS
-- DUE: Dec 8 at 11:59pm

-- FILE NAME: ANS_BRONZE_IMPORT.sql

-- SOLUTION

------------------------------------------------------------------
-- 0. SET CONTEXT
------------------------------------------------------------------
USE ROLE ROLE_TEAM_ANS;
USE WAREHOUSE ANIMAL_TASK_WH;
USE DATABASE DB_TEAM_ANS;

------------------------------------------------------------------
-- 1. CREATE BRONZE SCHEMA
------------------------------------------------------------------
-- Drop schema before recreating it
DROP SCHEMA IF EXISTS BRONZE CASCADE;

CREATE SCHEMA IF NOT EXISTS BRONZE;

USE SCHEMA DB_TEAM_ANS.BRONZE;

------------------------------------------------------------------
-- 2. CREATE INTERNAL NAMED STAGE IN BRONZE
------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS DCAMP_BRONZE_STAGE;

-- Optional sanity checks
DESCRIBE STAGE DCAMP_BRONZE_STAGE;

-- After upload
LIST @DCAMP_BRONZE_STAGE;

------------------------------------------------------------------
-- 3. CREATE A COMMON FILE FORMAT FOR BRONZE CSV FILES
------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT DCAMP_BRONZE_CSV_FF
    TYPE                           = CSV
    FIELD_DELIMITER                = ','
    SKIP_HEADER                    = 1
    FIELD_OPTIONALLY_ENCLOSED_BY   = '"'
    TRIM_SPACE                     = TRUE
    EMPTY_FIELD_AS_NULL            = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- Optional sanity checks
DESCRIBE FILE FORMAT DCAMP_BRONZE_CSV_FF;

------------------------------------------------------------------
-- 4. CREATE BRONZE TABLES
------------------------------------------------------------------

------------------------------------------------------------------
-- 4.1 COURSES BRONZE TABLE
------------------------------------------------------------------
CREATE OR REPLACE TABLE DCAMP_COURSES_BRONZE (
    id                   VARCHAR,
    title                VARCHAR,
    description          VARCHAR,
    short_description    VARCHAR,
    programming_language VARCHAR,
    difficulty_level     VARCHAR,
    xp                   VARCHAR,
    time_needed_in_hours VARCHAR,
    topic_id             VARCHAR,
    technology_id        VARCHAR,
    content_area         VARCHAR,
    link                 VARCHAR,
    image_url            VARCHAR,
    last_updated_on      VARCHAR,
    nb_of_subscriptions  VARCHAR,
    num_chapters         VARCHAR,
    num_exercises        VARCHAR,
    num_videos           VARCHAR,
    datasets_count       VARCHAR,
    instructors_names    VARCHAR,
    collaborators_names  VARCHAR,
    tracks_titles        VARCHAR,
    prerequisites_titles VARCHAR
);

------------------------------------------------------------------
-- 4.2 ALL_TRACKS BRONZE TABLE
------------------------------------------------------------------
CREATE OR REPLACE TABLE DCAMP_ALL_TRACKS_BRONZE (
    track_id                 VARCHAR,
    track_title              VARCHAR,
    is_career                VARCHAR,
    course_count             VARCHAR,
    total_chapters           VARCHAR,
    total_exercises          VARCHAR,
    total_videos             VARCHAR,
    total_xp                 VARCHAR,
    avg_xp_per_course        VARCHAR,
    avg_time_hours           VARCHAR,
    total_duration_hours     VARCHAR,
    datasets_count           VARCHAR,
    programming_language     VARCHAR,
    course_difficulty_levels VARCHAR,
    predominant_difficulty   VARCHAR,
    course_titles            VARCHAR,
    instructors              VARCHAR,
    participant_count        VARCHAR
);

------------------------------------------------------------------
-- 4.3 TOPIC_MAPPING BRONZE TABLE
------------------------------------------------------------------
CREATE OR REPLACE TABLE DCAMP_TOPIC_MAPPING_BRONZE (
    topic_id   VARCHAR,
    topic_name VARCHAR
);

------------------------------------------------------------------
-- 4.4 TECHNOLOGY_MAPPING BRONZE TABLE
------------------------------------------------------------------
CREATE OR REPLACE TABLE DCAMP_TECHNOLOGY_MAPPING_BRONZE (
    technology_id   VARCHAR,
    technology_name VARCHAR
);

------------------------------------------------------------------
-- 5. LOAD DATA FROM STAGE INTO BRONZE TABLES (COPY INTO)
------------------------------------------------------------------

------------------------------------------------------------------
-- 5.1 LOAD COURSES
------------------------------------------------------------------
-- Ensure table is empty before data load
TRUNCATE TABLE DCAMP_COURSES_BRONZE;

-- Load data
COPY INTO DCAMP_COURSES_BRONZE
FROM @dcamp_bronze_stage/
FILES = ('courses.csv')
FILE_FORMAT = (FORMAT_NAME = DCAMP_BRONZE_CSV_FF)
ON_ERROR = ABORT_STATEMENT;

-- Optional check
SELECT * FROM DCAMP_COURSES_BRONZE;

------------------------------------------------------------------
-- 5.2 LOAD ALL_TRACKS
------------------------------------------------------------------
-- Ensure table is empty before data load
TRUNCATE TABLE DCAMP_ALL_TRACKS_BRONZE;

-- Load data
COPY INTO DCAMP_ALL_TRACKS_BRONZE
FROM @dcamp_bronze_stage/
FILES = ('all_tracks.csv')
FILE_FORMAT = (FORMAT_NAME = DCAMP_BRONZE_CSV_FF)
ON_ERROR = ABORT_STATEMENT;

-- Optional check
SELECT * FROM DCAMP_ALL_TRACKS_BRONZE;

------------------------------------------------------------------
-- 5.3 LOAD TOPIC_MAPPING
------------------------------------------------------------------
-- Ensure table is empty before data load
TRUNCATE TABLE DCAMP_TOPIC_MAPPING_BRONZE;

-- Load data
COPY INTO DCAMP_TOPIC_MAPPING_BRONZE
FROM @dcamp_bronze_stage/
FILES = ('topic_mapping.csv')
FILE_FORMAT = (FORMAT_NAME = DCAMP_BRONZE_CSV_FF)
ON_ERROR = ABORT_STATEMENT;

-- Optional check
SELECT * FROM DCAMP_TOPIC_MAPPING_BRONZE;

------------------------------------------------------------------
-- 5.4 LOAD TECHNOLOGY_MAPPING
------------------------------------------------------------------
-- Ensure table is empty before data load
TRUNCATE TABLE DCAMP_TECHNOLOGY_MAPPING_BRONZE;

-- Load data
COPY INTO DCAMP_TECHNOLOGY_MAPPING_BRONZE
FROM @dcamp_bronze_stage/
FILES = ('technology_mapping.csv')
FILE_FORMAT = (FORMAT_NAME = DCAMP_BRONZE_CSV_FF)
ON_ERROR = ABORT_STATEMENT;

-- Optional check
SELECT * FROM DCAMP_TECHNOLOGY_MAPPING_BRONZE;

------------------------------------------------------------------
-- END OF SCRIPT
------------------------------------------------------------------
