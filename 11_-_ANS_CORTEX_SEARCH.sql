-- TERM PROJECT
-- TEAM NAME: ANS
-- MEMBER NAMES: Abhiroop Kumar (ak56448), Nikhil Kumar (nk25627), Simoni K Dalal (skd939)
-- DATABASE: DB_TEAM_ANS
-- ROLE: ROLE_TEAM_ANS
-- DUE: Dec 8 at 11:59pm

-- FILE NAME: ANS_CORTEX_SEARCH.sql

-- SOLUTION

------------------------------------------------------------------
-- 0. SET CONTEXT
------------------------------------------------------------------
USE ROLE ROLE_TEAM_ANS;
USE WAREHOUSE ANIMAL_TASK_WH;
USE DATABASE DB_TEAM_ANS;
USE SCHEMA DB_TEAM_ANS.BRONZE;

-- Optional: List all Cortex Search services in DB_TEAM_ANS.BRONZE
SHOW CORTEX SEARCH SERVICES
  IN SCHEMA DB_TEAM_ANS.BRONZE;

------------------------------------------------------------------
-- 1. CREATE CORTEX SEARCH SERVICE
------------------------------------------------------------------

------------------------------------------------------------------
-- 1.1 ANS_DATACAMP_COURSES_CORTEX_SEARCH
------------------------------------------------------------------
-- Search field: TITLE
-- We’ll search the TITLE text, and expose key attributes for filtering / display.
------------------------------------------------------------------
CREATE OR REPLACE CORTEX SEARCH SERVICE ANS_DATACAMP_COURSES_CORTEX_SEARCH
  ON TITLE
  ATTRIBUTES
      id,
      description,
      short_description,
      programming_language,
      difficulty_level,
      content_area,
      link,
      image_url
  WAREHOUSE = ANIMAL_TASK_WH
  TARGET_LAG = '1 MINUTE'
  EMBEDDING_MODEL = 'snowflake-arctic-embed-m-v1.5'
  INITIALIZE = ON_CREATE
AS
SELECT
    id,
    title,
    description,
    short_description,
    programming_language,
    difficulty_level,
    content_area,
    link,
    image_url
FROM DB_TEAM_ANS.BRONZE.DCAMP_COURSES_BRONZE;

-- View ANS_DATACAMP_COURSES_CORTEX_SEARCH service in DB_TEAM_ANS.BRONZE
SHOW CORTEX SEARCH SERVICES
  IN SCHEMA DB_TEAM_ANS.BRONZE;

-- Optional sanity check
DESCRIBE CORTEX SEARCH SERVICE DB_TEAM_ANS.BRONZE.ANS_DATACAMP_COURSES_CORTEX_SEARCH;

------------------------------------------------------------------
-- 1.2 ANS_DATACAMP_TRACKS_CORTEX_SEARCH
------------------------------------------------------------------
-- Search field: TRACK_TITLE
-- Here we search over TRACK_TITLE so we can find tracks by their names.
------------------------------------------------------------------
CREATE OR REPLACE CORTEX SEARCH SERVICE ANS_DATACAMP_TRACKS_CORTEX_SEARCH
  ON TRACK_TITLE
  ATTRIBUTES
      track_id,
      is_career,
      programming_language,
      course_count,
      course_titles,
      predominant_difficulty,
      total_chapters,
      total_exercises,
      total_videos,
      total_duration_hours
  WAREHOUSE = ANIMAL_TASK_WH
  TARGET_LAG = '1 MINUTE'
  EMBEDDING_MODEL = 'snowflake-arctic-embed-m-v1.5'
  INITIALIZE = ON_CREATE
AS
SELECT
    track_id,
    track_title,
    is_career,
    programming_language,
    course_count,
    course_titles,
    predominant_difficulty,
    total_chapters,
    total_exercises,
    total_videos,
    total_duration_hours
FROM DB_TEAM_ANS.BRONZE.DCAMP_ALL_TRACKS_BRONZE;

-- View ANS_DATACAMP_TRACKS_CORTEX_SEARCH service in DB_TEAM_ANS.BRONZE
SHOW CORTEX SEARCH SERVICES
  IN SCHEMA DB_TEAM_ANS.BRONZE;
  
-- Optional sanity check
DESCRIBE CORTEX SEARCH SERVICE DB_TEAM_ANS.BRONZE.ANS_DATACAMP_TRACKS_CORTEX_SEARCH;

------------------------------------------------------------------
-- 2. OPTIONAL: CORTEX SEARCH SERVICE PREVIEW
------------------------------------------------------------------

------------------------------------------------------------------
-- 2.1 COURSES SEARCH PREVIEW
------------------------------------------------------------------
SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'DB_TEAM_ANS.BRONZE.ANS_DATACAMP_COURSES_CORTEX_SEARCH',
    '{
       "query": "python beginner",
       "columns": ["TITLE","PROGRAMMING_LANGUAGE","DIFFICULTY_LEVEL"],
       "limit": 5
     }'
  )
)['results'];

------------------------------------------------------------------
-- 2.2 ALL_TRACKS SEARCH PREVIEW
------------------------------------------------------------------
SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'DB_TEAM_ANS.BRONZE.ANS_DATACAMP_TRACKS_CORTEX_SEARCH',
    '{
       "query": "machine learning",
       "columns": ["TRACK_TITLE","COURSE_COUNT","IS_CAREER"],
       "limit": 5
     }'
  )
)['results'];

------------------------------------------------------------------
-- 3 OPTIONAL: GRANTS
------------------------------------------------------------------
GRANT USAGE ON CORTEX SEARCH SERVICE ANS_DATACAMP_COURSES_CORTEX_SEARCH TO ROLE ROLE_TEAM_ANS;
GRANT USAGE ON CORTEX SEARCH SERVICE ANS_DATACAMP_TRACKS_CORTEX_SEARCH  TO ROLE ROLE_TEAM_ANS;

------------------------------------------------------------------
-- END OF SCRIPT
------------------------------------------------------------------