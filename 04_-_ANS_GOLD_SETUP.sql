-- TERM PROJECT
-- TEAM NAME: ANS
-- MEMBER NAMES: Abhiroop Kumar (ak56448), Nikhil Kumar (nk25627), Simoni K Dalal (skd939)
-- DATABASE: DB_TEAM_ANS
-- ROLE: ROLE_TEAM_ANS
-- DUE: Dec 8 at 11:59pm

-- FILE NAME: ANS_GOLD_SETUP.sql

-- SOLUTION

------------------------------------------------------------------
-- 0. SET CONTEXT
------------------------------------------------------------------
USE ROLE ROLE_TEAM_ANS;
USE WAREHOUSE ANIMAL_TASK_WH;
USE DATABASE DB_TEAM_ANS;

------------------------------------------------------------------
-- 1. CREATE GOLD SCHEMA
------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS GOLD;

USE SCHEMA DB_TEAM_ANS.GOLD;

------------------------------------------------------------------
-- 2. BUSINESS QUESTIONS
------------------------------------------------------------------
-- Use case 1: Programming Language Instructional Effort
-- Business question: Across all DataCamp courses, which programming languages require the most instructional effort (time, chapters, exercises, videos)?
-- Silver tables required: FACT_COURSE_SNAPSHOT_SILVER, DIM_PROGRAMMING_LANGUAGE, DIM_DIFFICULTY, DIM_COURSE

-- Use case 2: Track-level summary
-- Business question: For each career or skill track, what is the total learning content provided—time, chapters, exercises, and videos—and how many courses does each track include?
-- Silver tables required: BRIDGE_COURSE_TRACK, DIM_TRACK, FACT_COURSE_SNAPSHOT_SILVER, DIM_COURSE

-- Use case 3: Course difficulty distribution and curriculum depth
-- Business Question: How is the course catalog distributed across difficulty levels (Beginner, Intermediate, Advanced (1,2,3)), and which difficulty level contributes the most total learning content ( chapters, exercises, videos)?
-- Silver tables required: FACT_COURSE_SNAPSHOT_SILVER, DIM_COURSE, DIM_DIFFICULTY

------------------------------------------------------------------
-- 3. G_LANGUAGE_INSTRUCTIONAL_EFFORT
------------------------------------------------------------------
-- Use Case 1:
-- Across all DataCamp courses, which programming languages require
-- the most instructional effort (time, chapters, exercises, videos)?
--
-- Grain: 1 row per programming language.
-- Sources:
--   SILVER.FACT_COURSE_SNAPSHOT_SILVER
--   SILVER.DIM_COURSE
--   SILVER.DIM_PROGRAMMING_LANGUAGE
------------------------------------------------------------------
CREATE OR REPLACE TABLE G_LANGUAGE_INSTRUCTIONAL_EFFORT AS
WITH latest_snapshot AS (
    SELECT MAX(snapshot_date_sk) AS snapshot_date_sk
    FROM DB_TEAM_ANS.SILVER.FACT_COURSE_SNAPSHOT_SILVER
)
SELECT
    pl.language_sk,
    pl.language_name,

    COUNT(DISTINCT c.course_sk)                        AS course_count,

    SUM(c.time_needed_hours)                           AS total_time_hours,
    SUM(f.num_chapters)                                AS total_chapters,
    SUM(f.num_exercises)                               AS total_exercises,
    SUM(f.num_videos)                                  AS total_videos,

    AVG(c.time_needed_hours)                           AS avg_time_hours_per_course,
    AVG(f.num_chapters)                                AS avg_chapters_per_course,
    AVG(f.num_exercises)                               AS avg_exercises_per_course,
    AVG(f.num_videos)                                  AS avg_videos_per_course,

    SUM(f.nb_of_subscriptions)                         AS total_nb_of_subscriptions,

    ls.snapshot_date_sk                                AS snapshot_date_sk
FROM DB_TEAM_ANS.SILVER.FACT_COURSE_SNAPSHOT_SILVER f
JOIN DB_TEAM_ANS.SILVER.DIM_COURSE c
  ON f.course_sk = c.course_sk
JOIN DB_TEAM_ANS.SILVER.DIM_PROGRAMMING_LANGUAGE pl
  ON c.programming_language_sk = pl.language_sk
JOIN latest_snapshot ls
  ON f.snapshot_date_sk = ls.snapshot_date_sk
GROUP BY
    pl.language_sk,
    pl.language_name,
    ls.snapshot_date_sk;

-- Optional check
SELECT * FROM G_LANGUAGE_INSTRUCTIONAL_EFFORT;

------------------------------------------------------------------
-- 4. G_TRACK_CONTENT_SUMMARY
------------------------------------------------------------------
-- Use Case 2:
-- For each career or skill track, what is the total learning content
-- (time, chapters, exercises, videos) and how many courses does each
-- track include?
--
-- Grain: 1 row per track.
-- Sources:
--   SILVER.FACT_COURSE_SNAPSHOT_SILVER
--   SILVER.DIM_COURSE
--   SILVER.BRIDGE_COURSE_TRACK
--   SILVER.DIM_TRACK
------------------------------------------------------------------
CREATE OR REPLACE TABLE G_TRACK_CONTENT_SUMMARY AS
WITH latest_snapshot AS (
    SELECT MAX(snapshot_date_sk) AS snapshot_date_sk
    FROM DB_TEAM_ANS.SILVER.FACT_COURSE_SNAPSHOT_SILVER
)
SELECT
    t.track_sk,
    t.track_title,
    t.is_career_flag,

    COUNT(DISTINCT c.course_sk)                        AS course_count,

    SUM(c.time_needed_hours)                           AS total_time_hours,
    SUM(f.num_chapters)                                AS total_chapters,
    SUM(f.num_exercises)                               AS total_exercises,
    SUM(f.num_videos)                                  AS total_videos,

    AVG(c.time_needed_hours)                           AS avg_time_hours_per_course,
    AVG(f.num_chapters)                                AS avg_chapters_per_course,
    AVG(f.num_exercises)                               AS avg_exercises_per_course,
    AVG(f.num_videos)                                  AS avg_videos_per_course,

    SUM(f.nb_of_subscriptions)                         AS total_nb_of_subscriptions,
    SUM(f.datasets_count)                              AS total_datasets_count,

    ls.snapshot_date_sk                                AS snapshot_date_sk
FROM DB_TEAM_ANS.SILVER.FACT_COURSE_SNAPSHOT_SILVER f
JOIN DB_TEAM_ANS.SILVER.DIM_COURSE c
  ON f.course_sk = c.course_sk
JOIN DB_TEAM_ANS.SILVER.BRIDGE_COURSE_TRACK bct
  ON c.course_sk = bct.course_sk
JOIN DB_TEAM_ANS.SILVER.DIM_TRACK t
  ON bct.track_sk = t.track_sk
JOIN latest_snapshot ls
  ON f.snapshot_date_sk = ls.snapshot_date_sk
GROUP BY
    t.track_sk,
    t.track_title,
    t.is_career_flag,
    ls.snapshot_date_sk;

-- Optional check
SELECT * FROM G_TRACK_CONTENT_SUMMARY;

------------------------------------------------------------------
-- 5. G_DIFFICULTY_CONTENT_SUMMARY
------------------------------------------------------------------
-- Use Case 3:
-- How is the course catalog distributed across difficulty levels
-- (Beginner, Intermediate, Advanced), and which difficulty level
-- contributes the most total learning content (chapters, exercises,
-- videos, time)?
--
-- Grain: 1 row per difficulty level.
-- Sources:
--   SILVER.FACT_COURSE_SNAPSHOT_SILVER
--   SILVER.DIM_COURSE
--   SILVER.DIM_DIFFICULTY
------------------------------------------------------------------
CREATE OR REPLACE TABLE G_DIFFICULTY_CONTENT_SUMMARY AS
WITH latest_snapshot AS (
    SELECT MAX(snapshot_date_sk) AS snapshot_date_sk
    FROM DB_TEAM_ANS.SILVER.FACT_COURSE_SNAPSHOT_SILVER
)
SELECT
    d.difficulty_sk,
    d.difficulty_code,
    d.difficulty_order,

    COUNT(DISTINCT c.course_sk)                        AS course_count,

    SUM(c.time_needed_hours)                           AS total_time_hours,
    SUM(f.num_chapters)                                AS total_chapters,
    SUM(f.num_exercises)                               AS total_exercises,
    SUM(f.num_videos)                                  AS total_videos,

    AVG(c.time_needed_hours)                           AS avg_time_hours_per_course,
    AVG(f.num_chapters)                                AS avg_chapters_per_course,
    AVG(f.num_exercises)                               AS avg_exercises_per_course,
    AVG(f.num_videos)                                  AS avg_videos_per_course,

    SUM(f.nb_of_subscriptions)                         AS total_nb_of_subscriptions,
    SUM(f.datasets_count)                              AS total_datasets_count,

    ls.snapshot_date_sk                                AS snapshot_date_sk
FROM DB_TEAM_ANS.SILVER.FACT_COURSE_SNAPSHOT_SILVER f
JOIN DB_TEAM_ANS.SILVER.DIM_COURSE c
  ON f.course_sk = c.course_sk
JOIN DB_TEAM_ANS.SILVER.DIM_DIFFICULTY d
  ON c.difficulty_sk = d.difficulty_sk
JOIN latest_snapshot ls
  ON f.snapshot_date_sk = ls.snapshot_date_sk
GROUP BY
    d.difficulty_sk,
    d.difficulty_code,
    d.difficulty_order,
    ls.snapshot_date_sk;

-- Optional check
SELECT * FROM G_DIFFICULTY_CONTENT_SUMMARY;

------------------------------------------------------------------
-- END OF SCRIPT
------------------------------------------------------------------