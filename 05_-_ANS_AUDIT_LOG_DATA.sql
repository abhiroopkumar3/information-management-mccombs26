-- TERM PROJECT
-- TEAM NAME: ANS
-- MEMBER NAMES: Abhiroop Kumar (ak56448), Nikhil Kumar (nk25627), Simoni K Dalal (skd939)
-- DATABASE: DB_TEAM_ANS
-- ROLE: ROLE_TEAM_ANS
-- DUE: Dec 8 at 11:59pm

-- FILE NAME: ANS_AUDIT_LOG_DATA.sql

-- SOLUTION

------------------------------------------------------------------
-- 0. SET CONTEXT
------------------------------------------------------------------
USE ROLE ROLE_TEAM_ANS;
USE WAREHOUSE ANIMAL_TASK_WH;
USE DATABASE DB_TEAM_ANS;

------------------------------------------------------------------
-- 1. CREATE STORED PROCEDURES
------------------------------------------------------------------

------------------------------------------------------------------
-- 1.1 LOG_SILVER_LOAD_METRICS()
------------------------------------------------------------------
USE SCHEMA DB_TEAM_ANS.SILVER;

CREATE OR REPLACE PROCEDURE LOG_SILVER_LOAD_METRICS()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
BEGIN
    INSERT INTO SILVER_LOAD_AUDIT (
        load_ts,
        bronze_courses_row_count,
        silver_dim_course_row_count,
        silver_fact_course_rows,
        bronze_tracks_row_count,
        silver_dim_track_row_count,
        silver_fact_track_rows
    )
    SELECT
        CURRENT_TIMESTAMP()                                         AS load_ts,

        -- Courses
        (SELECT COUNT(*) FROM DB_TEAM_ANS.BRONZE.DCAMP_COURSES_BRONZE),
        (SELECT COUNT(*) FROM DIM_COURSE),
        (SELECT COUNT(*) FROM FACT_COURSE_SNAPSHOT_SILVER),

        -- Tracks
        (SELECT COUNT(*) FROM DB_TEAM_ANS.BRONZE.DCAMP_ALL_TRACKS_BRONZE),
        (SELECT COUNT(*) FROM DIM_TRACK),
        (SELECT COUNT(*) FROM FACT_TRACK_SUMMARY_SILVER);

    RETURN 'Silver load metrics recorded in SILVER_LOAD_AUDIT.';
END;

-- Call LOG_SILVER_LOAD_METRICS()
CALL DB_TEAM_ANS.SILVER.LOG_SILVER_LOAD_METRICS();

-- Optional check
SELECT
    audit_id,
    load_ts,
    bronze_courses_row_count,
    silver_dim_course_row_count,
    silver_fact_course_rows,
    bronze_tracks_row_count,
    silver_dim_track_row_count,
    silver_fact_track_rows
FROM DB_TEAM_ANS.SILVER.SILVER_LOAD_AUDIT
ORDER BY audit_id;

------------------------------------------------------------------
-- 1.2 LOG_GOLD_LOAD_METRICS()
------------------------------------------------------------------
USE SCHEMA DB_TEAM_ANS.GOLD;

CREATE OR REPLACE PROCEDURE LOG_GOLD_LOAD_METRICS()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
BEGIN
    INSERT INTO GOLD_LOAD_AUDIT (
        load_ts,
        gold_language_rows,
        gold_track_content_rows,
        gold_difficulty_content_rows,
        silver_fact_course_rows,
        silver_fact_track_rows
    )
    SELECT
        CURRENT_TIMESTAMP()                                              AS load_ts,

        -- Gold dynamic tables
        (SELECT COUNT(*) FROM DB_TEAM_ANS.GOLD.G_LANGUAGE_INSTRUCTIONAL_EFFORT),
        (SELECT COUNT(*) FROM DB_TEAM_ANS.GOLD.G_TRACK_CONTENT_SUMMARY),
        (SELECT COUNT(*) FROM DB_TEAM_ANS.GOLD.G_DIFFICULTY_CONTENT_SUMMARY),

        -- Silver facts (for upstream comparison)
        (SELECT COUNT(*) FROM DB_TEAM_ANS.SILVER.FACT_COURSE_SNAPSHOT_SILVER),
        (SELECT COUNT(*) FROM DB_TEAM_ANS.SILVER.FACT_TRACK_SUMMARY_SILVER);

    RETURN 'Gold metrics recorded in GOLD_LOAD_AUDIT.';
END;

-- Call LOG_GOLD_LOAD_METRICS()
CALL DB_TEAM_ANS.GOLD.LOG_GOLD_LOAD_METRICS();

-- Optional check
SELECT
    audit_id,
    load_ts,
    gold_language_rows,
    gold_track_content_rows,
    gold_difficulty_content_rows,
    silver_fact_course_rows,
    silver_fact_track_rows
FROM DB_TEAM_ANS.GOLD.GOLD_LOAD_AUDIT
ORDER BY audit_id;

------------------------------------------------------------------
-- END OF SCRIPT
------------------------------------------------------------------