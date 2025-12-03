-- TERM PROJECT
-- TEAM NAME: ANS
-- MEMBER NAMES: Abhiroop Kumar (ak56448), Nikhil Kumar (nk25627), Simoni K Dalal (skd939)
-- DATABASE: DB_TEAM_ANS
-- ROLE: ROLE_TEAM_ANS
-- DUE: Dec 8 at 11:59pm

-- FILE NAME: ANS_AUDIT_LOG_DATA_INCREMENTAL.sql

-- SOLUTION

------------------------------------------------------------------
-- 0. SET CONTEXT
------------------------------------------------------------------
USE ROLE ROLE_TEAM_ANS;
USE WAREHOUSE ANIMAL_TASK_WH;
USE DATABASE DB_TEAM_ANS;

------------------------------------------------------------------
-- 1. CALL AUDIT LOG STORED PROCEDURES
------------------------------------------------------------------

------------------------------------------------------------------
-- 1.1 LOG_SILVER_LOAD_METRICS_INCREMENTAL()
------------------------------------------------------------------
USE SCHEMA DB_TEAM_ANS.SILVER;

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
    silver_fact_track_rows,
    gold_language_total_courses,
    gold_language_total_subs,
    gold_track_total_courses,
    gold_track_total_subs,
    gold_difficulty_total_subs
FROM DB_TEAM_ANS.GOLD.GOLD_LOAD_AUDIT
ORDER BY audit_id;

------------------------------------------------------------------
-- END OF SCRIPT
------------------------------------------------------------------