-- TERM PROJECT
-- TEAM NAME: ANS
-- MEMBER NAMES: Abhiroop Kumar (ak56448), Nikhil Kumar (nk25627), Simoni K Dalal (skd939)
-- DATABASE: DB_TEAM_ANS
-- ROLE: ROLE_TEAM_ANS
-- DUE: Dec 8 at 11:59pm

-- FILE NAME: ANS_SILVER_STATIC_DATA.sql

-- SOLUTION

------------------------------------------------------------------
-- 0. SET CONTEXT
------------------------------------------------------------------
USE ROLE ROLE_TEAM_ANS;
USE WAREHOUSE ANIMAL_TASK_WH;
USE DATABASE DB_TEAM_ANS;
USE SCHEMA DB_TEAM_ANS.SILVER;

------------------------------------------------------------------
-- 1. LOAD DIMENSION TABLES
------------------------------------------------------------------

------------------------------------------------------------------
-- 1.1 DIM_DATE
------------------------------------------------------------------
-- Load all distinct LAST_UPDATED_ON dates from COURSES,
-- plus today's date as a snapshot date.
------------------------------------------------------------------
-- Ensure table is empty before data load
TRUNCATE TABLE DIM_DATE;

-- Insert Data
INSERT INTO DIM_DATE (
    date_value,
    year,
    quarter,
    month,
    month_name,
    day_of_month,
    day_of_week,
    day_name,
    week_of_year,
    is_weekend
)
SELECT
    d                                           AS date_value,
    YEAR(d)                                     AS year,
    QUARTER(d)                                  AS quarter,
    MONTH(d)                                    AS month,
    TO_CHAR(d, 'MON')                           AS month_name,
    DAY(d)                                      AS day_of_month,
    DAYOFWEEK(d)                                AS day_of_week,
    TO_CHAR(d, 'DY')                            AS day_name,
    WEEKOFYEAR(d)                               AS week_of_year,
    CASE WHEN DAYOFWEEK(d) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend
FROM (
    SELECT DISTINCT 
        TO_DATE(c.LAST_UPDATED_ON, 'DD/MM/YYYY') AS d
    FROM DB_TEAM_ANS.BRONZE.DCAMP_COURSES_BRONZE c
) src
LEFT JOIN DIM_DATE dd
    ON dd.date_value = src.d
WHERE dd.date_value IS NULL;

-- Optional check
SELECT * FROM DIM_DATE;

------------------------------------------------------------------
-- 1.2 DIM_PROGRAMMING_LANGUAGE
------------------------------------------------------------------
-- One row per language (python, r, sql, shell, etc.)
-- Distinct across COURSES and TRACKS, splitting comma lists.
------------------------------------------------------------------
-- Ensure table is empty before data load
TRUNCATE TABLE DIM_PROGRAMMING_LANGUAGE;

-- Insert Data
INSERT INTO DIM_PROGRAMMING_LANGUAGE (
    language_code,
    language_name
)
SELECT DISTINCT
    LOWER(TRIM(f.value::string))                         AS language_code,
    INITCAP(LOWER(TRIM(f.value::string)))               AS language_name
FROM (
    -- bring all raw programming_language strings together
    SELECT PROGRAMMING_LANGUAGE
    FROM   DB_TEAM_ANS.BRONZE.DCAMP_COURSES_BRONZE

    UNION

    SELECT PROGRAMMING_LANGUAGE
    FROM   DB_TEAM_ANS.BRONZE.DCAMP_ALL_TRACKS_BRONZE
) src,
LATERAL FLATTEN(SPLIT(src.PROGRAMMING_LANGUAGE, ',')) f
WHERE f.value IS NOT NULL
  AND TRIM(f.value::string) <> ''
  AND NOT EXISTS (
        SELECT 1
        FROM   DIM_PROGRAMMING_LANGUAGE d
        WHERE  d.language_code = LOWER(TRIM(f.value::string))
  );

-- Optional check
SELECT * FROM DIM_PROGRAMMING_LANGUAGE;

------------------------------------------------------------------
-- 1.3 DIM_DIFFICULTY
-- Distinct difficulty labels from courses and tracks.
------------------------------------------------------------------
-- Ensure table is empty before data load
TRUNCATE TABLE DIM_DIFFICULTY;

-- Insert Data
INSERT INTO DIM_DIFFICULTY (
    difficulty_code, 
    difficulty_order
)
VALUES
    ('Beginner', 1),
    ('Intermediate', 2),
    ('Advanced', 3);

-- Optional check
SELECT * FROM DIM_DIFFICULTY;

------------------------------------------------------------------
-- 1.4 DIM_CONTENT_AREA
-- Distinct content areas from COURSES.
------------------------------------------------------------------
-- Ensure table is empty before data load
TRUNCATE TABLE DIM_CONTENT_AREA;

-- Insert Data
INSERT INTO DIM_CONTENT_AREA (
    content_area_name
)
SELECT DISTINCT
    TRIM(CONTENT_AREA) AS content_area_name
FROM DB_TEAM_ANS.BRONZE.DCAMP_COURSES_BRONZE c
WHERE CONTENT_AREA IS NOT NULL
  AND TRIM(CONTENT_AREA) <> ''
  AND NOT EXISTS (
        SELECT 1
        FROM   DIM_CONTENT_AREA d
        WHERE  d.content_area_name = TRIM(c.CONTENT_AREA)
  );

-- Optional check
SELECT * FROM DIM_CONTENT_AREA;

------------------------------------------------------------------
-- 1.5 DIM_TOPIC
-- From DCAMP_TOPIC_MAPPING_BRONZE.
------------------------------------------------------------------
-- Ensure table is empty before data load
TRUNCATE TABLE DIM_TOPIC;

-- Insert Data
INSERT INTO DIM_TOPIC (
    topic_id,
    topic_name
)
SELECT DISTINCT
    t.TOPIC_ID,
    t.TOPIC_NAME
FROM DB_TEAM_ANS.BRONZE.DCAMP_TOPIC_MAPPING_BRONZE t
LEFT JOIN DIM_TOPIC dt
  ON dt.topic_id = t.TOPIC_ID
WHERE dt.topic_id IS NULL
ORDER BY t.TOPIC_ID ASC;

-- Optional check
SELECT * FROM DIM_TOPIC;

------------------------------------------------------------------
-- 1.6 DIM_TECHNOLOGY
-- From DCAMP_TECHNOLOGY_MAPPING_BRONZE.
------------------------------------------------------------------
-- Ensure table is empty before data load
TRUNCATE TABLE DIM_TECHNOLOGY;

-- Insert Data
INSERT INTO DIM_TECHNOLOGY (
    technology_id,
    technology_name
)
SELECT DISTINCT
    tech.TECHNOLOGY_ID,
    tech.TECHNOLOGY_NAME
FROM DB_TEAM_ANS.BRONZE.DCAMP_TECHNOLOGY_MAPPING_BRONZE tech
LEFT JOIN DIM_TECHNOLOGY dt
  ON dt.technology_id = tech.TECHNOLOGY_ID
WHERE dt.technology_id IS NULL
ORDER BY tech.technology_id ASC;

-- Optional check
SELECT * FROM DIM_TECHNOLOGY;

------------------------------------------------------------------
-- 1.7 DIM_INSTRUCTOR
------------------------------------------------------------------
-- Distinct instructor names across:
--   - COURSES.instructors_names
--   - ALL_TRACKS.instructors
------------------------------------------------------------------
-- Ensure table is empty before data load
TRUNCATE TABLE DIM_INSTRUCTOR;

-- Insert Data
INSERT INTO DIM_INSTRUCTOR (
    instructor_name
)
SELECT DISTINCT
    instructor_name
FROM (
    -- Instructors from COURSES (INSTRUCTORS_NAMES, semicolon-delimited)
    SELECT
        REGEXP_REPLACE(TRIM(f.VALUE::STRING), '\\s+', ' ') AS instructor_name
    FROM DB_TEAM_ANS.BRONZE.DCAMP_COURSES_BRONZE c,
         LATERAL FLATTEN(SPLIT(c.INSTRUCTORS_NAMES, ';')) f

    UNION ALL

    -- Instructors from TRACKS (INSTRUCTORS, comma-delimited)
    SELECT
        REGEXP_REPLACE(TRIM(f.VALUE::STRING), '\\s+', ' ') AS instructor_name
    FROM DB_TEAM_ANS.BRONZE.DCAMP_ALL_TRACKS_BRONZE t,
         LATERAL FLATTEN(SPLIT(t.INSTRUCTORS, ',')) f
) src
WHERE instructor_name IS NOT NULL
  AND instructor_name <> '';

-- Optional check
SELECT * FROM DIM_INSTRUCTOR;

------------------------------------------------------------------
-- 1.8 DIM_COLLABORATOR
-- From DCAMP_ALL_TRACKS_BRONZE.
------------------------------------------------------------------
-- Ensure table is empty before data load
TRUNCATE TABLE DIM_COLLABORATOR;

-- Insert Data
INSERT INTO DIM_COLLABORATOR (
    collaborator_name
)
SELECT DISTINCT
    TRIM(f.VALUE)::STRING AS collaborator_name
FROM DB_TEAM_ANS.BRONZE.DCAMP_COURSES_BRONZE c,
     LATERAL FLATTEN(SPLIT(c.COLLABORATORS_NAMES, ';')) f
WHERE collaborator_name IS NOT NULL
  AND collaborator_name <> '';

-- Optional check
SELECT * FROM DIM_COLLABORATOR;

------------------------------------------------------------------
-- 1.9 DIM_TRACK
-- One row per track, metadata only (no linking!)
------------------------------------------------------------------
-- Ensure table is empty before data load
TRUNCATE TABLE DIM_TRACK;

-- Insert Data
INSERT INTO DIM_TRACK (
    track_id,
    track_title,
    is_career_flag,
    programming_language_sk,
    predominant_difficulty_sk,
    raw_course_difficulty_levels,
    raw_course_titles,
    raw_instructors,
    raw_programming_languages
)
SELECT
    t.TRACK_ID,
    t.TRACK_TITLE,

    -- convert text boolean into real BOOLEAN
    IFF(LOWER(t.IS_CAREER) IN ('true','t','1','yes','y'), TRUE, FALSE),

    -- choose FIRST programming language in the list for FK lookup
    pl.language_sk,

    -- difficulty is numeric: 1=Beginner, 2=Intermediate, 3=Advanced
    dd.difficulty_sk,

    -- raw lineage fields (kept as-is)
    t.COURSE_DIFFICULTY_LEVELS,
    t.COURSE_TITLES,
    t.INSTRUCTORS,
    t.PROGRAMMING_LANGUAGE

FROM DB_TEAM_ANS.BRONZE.DCAMP_ALL_TRACKS_BRONZE t

LEFT JOIN DIM_PROGRAMMING_LANGUAGE pl
    ON pl.language_code = LOWER(TRIM(SPLIT_PART(t.PROGRAMMING_LANGUAGE, ',', 1)))

LEFT JOIN DIM_DIFFICULTY dd
    ON dd.difficulty_order = TRY_TO_NUMBER(t.PREDOMINANT_DIFFICULTY)

LEFT JOIN DIM_TRACK existing
    ON existing.track_id = t.TRACK_ID

WHERE existing.track_id IS NULL;

-- Optional check
SELECT * FROM DIM_TRACK;

------------------------------------------------------------------
-- 1.10 DIM_COURSE
------------------------------------------------------------------
-- One row per course, metadata only.
-- Multi-valued columns stored raw; linking done in bridge tables.
------------------------------------------------------------------
-- Ensure table is empty before data load
TRUNCATE TABLE DIM_COURSE;

-- Insert Data
INSERT INTO DIM_COURSE (
    course_id,
    title,
    short_description,
    description,
    xp,
    time_needed_hours,
    programming_language_sk,
    difficulty_sk,
    topic_sk,
    technology_sk,
    content_area_sk,
    last_updated_date_sk,
    course_url,
    image_url,
    raw_instructors_names,
    raw_collaborators_names,
    raw_tracks_titles,
    raw_prerequisites_titles
)
SELECT
    c.ID                                         AS course_id,
    c.TITLE                                      AS title,
    c.SHORT_DESCRIPTION                          AS short_description,
    c.DESCRIPTION                                AS description,
    TRY_TO_NUMBER(c.XP)                          AS xp,
    TRY_TO_NUMBER(c.TIME_NEEDED_IN_HOURS)        AS time_needed_hours,

    -- programming language lookup
    pl.language_sk                               AS programming_language_sk,

    -- corrected difficulty mapping
    dd.difficulty_sk                             AS difficulty_sk,

    -- FIX #1: TOPIC LOOKUP WORKING (cast float '3.0' → 3)
    dt.topic_sk                                  AS topic_sk,

    -- technology lookup
    te.technology_sk                             AS technology_sk,

    -- content area lookup
    ca.content_area_sk                           AS content_area_sk,

    -- FIX #2: DATE PARSING (DD/MM/YYYY)
    -- and join to DIM_DATE
    ddate.date_sk                                AS last_updated_date_sk,

    c.LINK                                       AS course_url,
    c.IMAGE_URL                                  AS image_url,

    -- store multi-value fields raw
    c.INSTRUCTORS_NAMES                          AS raw_instructors_names,
    c.COLLABORATORS_NAMES                        AS raw_collaborators_names,
    c.TRACKS_TITLES                              AS raw_tracks_titles,
    c.PREREQUISITES_TITLES                       AS raw_prerequisites_titles

FROM DB_TEAM_ANS.BRONZE.DCAMP_COURSES_BRONZE c

LEFT JOIN DIM_PROGRAMMING_LANGUAGE pl
    ON pl.language_code = LOWER(TRIM(c.PROGRAMMING_LANGUAGE))

LEFT JOIN DIM_DIFFICULTY dd
    ON dd.difficulty_order = TRY_TO_NUMBER(c.DIFFICULTY_LEVEL)

-- FIX #1: Convert Bronze topic_id to NUMBER before joining
LEFT JOIN DIM_TOPIC dt
    ON dt.topic_id = TRY_TO_NUMBER(c.TOPIC_ID)

LEFT JOIN DIM_TECHNOLOGY te
    ON te.technology_id = c.TECHNOLOGY_ID

LEFT JOIN DIM_CONTENT_AREA ca
    ON ca.content_area_name = c.CONTENT_AREA

-- FIX #2: Parse Bronze date string and join to DIM_DATE
LEFT JOIN DIM_DATE ddate
    ON ddate.date_value = TRY_TO_DATE(c.LAST_UPDATED_ON, 'DD/MM/YYYY')

LEFT JOIN DIM_COURSE existing
    ON existing.course_id = c.ID

WHERE existing.course_id IS NULL;

-- Optional check
SELECT * FROM DIM_COURSE;

------------------------------------------------------------------
-- 2. LOAD BRIDGE (SUB-DIMENSION) TABLES
------------------------------------------------------------------

------------------------------------------------------------------
-- 2.1 BRIDGE_COURSE_INSTRUCTOR
------------------------------------------------------------------
--   - One row per (course, person, role)
--   - Uses:
--       DCAMP_COURSES_BRONZE.INSTRUCTORS_NAMES  (semicolon)
--       DCAMP_COURSES_BRONZE.COLLABORATORS_NAMES (semicolon)
------------------------------------------------------------------
-- Ensure table is empty before data load
TRUNCATE TABLE BRIDGE_COURSE_INSTRUCTOR;

-- Insert Data
INSERT INTO BRIDGE_COURSE_INSTRUCTOR (
    course_sk,
    instructor_sk,
    role,
    instructor_order
)
SELECT DISTINCT
    dc.course_sk,
    di.instructor_sk,
    src.role,
    src.instructor_order
FROM (
    -- Instructors
    SELECT
        c.ID AS course_id,
        REGEXP_REPLACE(TRIM(f.VALUE::STRING), '\\s+', ' ') AS person_name,
        'INSTRUCTOR' AS role,
        ROW_NUMBER() OVER (
            PARTITION BY c.ID, 'INSTRUCTOR'
            ORDER BY SEQ8()
        ) AS instructor_order
    FROM DB_TEAM_ANS.BRONZE.DCAMP_COURSES_BRONZE c,
         LATERAL FLATTEN(SPLIT(c.INSTRUCTORS_NAMES, ';')) f

    UNION ALL

    -- Collaborators (also stored in this bridge with role = 'COLLABORATOR')
    SELECT
        c.ID AS course_id,
        REGEXP_REPLACE(TRIM(f.VALUE::STRING), '\\s+', ' ') AS person_name,
        'COLLABORATOR' AS role,
        ROW_NUMBER() OVER (
            PARTITION BY c.ID, 'COLLABORATOR'
            ORDER BY SEQ8()
        ) AS instructor_order
    FROM DB_TEAM_ANS.BRONZE.DCAMP_COURSES_BRONZE c,
         LATERAL FLATTEN(SPLIT(c.COLLABORATORS_NAMES, ';')) f
) src
JOIN DIM_COURSE dc
  ON dc.course_id = src.course_id
JOIN DIM_INSTRUCTOR di
  ON di.instructor_name = src.person_name;

-- Optional check
SELECT * FROM BRIDGE_COURSE_INSTRUCTOR;

------------------------------------------------------------------
-- 2.2 BRIDGE_COURSE_TRACK
------------------------------------------------------------------
--   - One row per (course, track)
--   - Uses DCAMP_COURSES_BRONZE.TRACKS_TITLES (semicolon-separated)
------------------------------------------------------------------
-- Ensure table is empty before data load
TRUNCATE TABLE BRIDGE_COURSE_TRACK;

-- Insert Data
INSERT INTO BRIDGE_COURSE_TRACK (
    course_sk,
    track_sk
)
SELECT DISTINCT
    dc.course_sk,
    dt.track_sk
FROM DB_TEAM_ANS.BRONZE.DCAMP_COURSES_BRONZE c
CROSS JOIN LATERAL FLATTEN(SPLIT(c.TRACKS_TITLES, ';')) f
JOIN DIM_COURSE dc
    ON dc.course_id = c.id
JOIN DIM_TRACK dt
    ON dt.track_title = TRIM(f.VALUE::STRING)
WHERE TRIM(f.VALUE::STRING) IS NOT NULL
  AND TRIM(f.VALUE::STRING) <> '';

-- Optional check
SELECT * FROM BRIDGE_COURSE_TRACK;

------------------------------------------------------------------
-- 2.3 BRIDGE_COURSE_PREREQUISITE
------------------------------------------------------------------
--   - One row per prerequisite relationship:
--     (course_sk -> prerequisite_course_sk)
--   - Uses DCAMP_COURSES_BRONZE.PREREQUISITES_TITLES
--     where multiple titles are separated by ';' and/or ','.
------------------------------------------------------------------
-- Ensure table is empty before data load
TRUNCATE TABLE BRIDGE_COURSE_PREREQUISITE;

-- Insert Data
INSERT INTO BRIDGE_COURSE_PREREQUISITE (
    course_sk,
    prerequisite_course_sk
)
SELECT DISTINCT
    dc_main.course_sk              AS course_sk,
    dc_pre.course_sk               AS prerequisite_course_sk
FROM DB_TEAM_ANS.BRONZE.DCAMP_COURSES_BRONZE c
CROSS JOIN LATERAL FLATTEN(
    SPLIT(
        REGEXP_REPLACE(c.PREREQUISITES_TITLES, ',', ';'),
        ';'
    )
) f
JOIN DIM_COURSE dc_main
    ON dc_main.course_id = c.id
JOIN DIM_COURSE dc_pre
    ON dc_pre.title = TRIM(f.VALUE::STRING)
WHERE TRIM(f.VALUE::STRING) IS NOT NULL
  AND TRIM(f.VALUE::STRING) <> '';

-- Optional check
SELECT * FROM BRIDGE_COURSE_PREREQUISITE;

------------------------------------------------------------------
-- 2.4 BRIDGE_COURSE_COLLABORATOR
------------------------------------------------------------------
--   - One row per (course, person, role)
--   - Uses:
--       DCAMP_COURSES_BRONZE.COLLABORATORS_NAMES (semicolon)
------------------------------------------------------------------

-- Ensure table is empty before data load
TRUNCATE TABLE BRIDGE_COURSE_COLLABORATOR;

-- Insert Data
INSERT INTO BRIDGE_COURSE_COLLABORATOR (
    course_sk,
    collaborator_sk
)
SELECT DISTINCT
    dc.course_sk,
    dcol.collaborator_sk
FROM DB_TEAM_ANS.BRONZE.DCAMP_COURSES_BRONZE c
JOIN DIM_COURSE dc
    ON dc.course_id = c.ID

-- Normalize cell values and split into multiple rows
CROSS JOIN LATERAL FLATTEN(
    SPLIT(
        REGEXP_REPLACE(c.COLLABORATORS_NAMES, ',', ';'),  -- normalize commas → semicolons if any
        ';'
    )
) f

-- Join to collaborator dimension
JOIN DIM_COLLABORATOR dcol
    ON dcol.collaborator_name = TRIM(f.VALUE::STRING)

WHERE TRIM(f.VALUE::STRING) IS NOT NULL
  AND TRIM(f.VALUE::STRING) <> '';

-- Optional check
SELECT * FROM BRIDGE_COURSE_COLLABORATOR;

------------------------------------------------------------------
-- 3. LOAD FACT TABLES
------------------------------------------------------------------

------------------------------------------------------------------
-- 3.1 FACT_COURSE_SNAPSHOT_SILVER
------------------------------------------------------------------
-- Ensure table is empty before data load
TRUNCATE TABLE FACT_COURSE_SNAPSHOT_SILVER;

-- Insert Data
INSERT INTO FACT_COURSE_SNAPSHOT_SILVER (
    course_sk,
    snapshot_date_sk,
    nb_of_subscriptions,
    num_chapters,
    num_exercises,
    num_videos,
    datasets_count,
    load_ts
)
SELECT
    dc.course_sk                                         AS course_sk,
    dd.date_sk                                           AS snapshot_date_sk,
    TRY_TO_NUMBER(c.NB_OF_SUBSCRIPTIONS)                 AS nb_of_subscriptions,
    TRY_TO_NUMBER(c.NUM_CHAPTERS)                        AS num_chapters,
    TRY_TO_NUMBER(c.NUM_EXERCISES)                       AS num_exercises,
    TRY_TO_NUMBER(c.NUM_VIDEOS)                          AS num_videos,
    TRY_TO_NUMBER(c.DATASETS_COUNT)                      AS datasets_count,
    CURRENT_TIMESTAMP()                                  AS load_ts
FROM DB_TEAM_ANS.BRONZE.DCAMP_COURSES_BRONZE c
JOIN DIM_COURSE dc
    ON dc.course_id = c.ID
JOIN DIM_DATE dd
    ON dd.date_value = (SELECT MAX(date_value) FROM DIM_DATE);

-- Optional check
SELECT * FROM FACT_COURSE_SNAPSHOT_SILVER;

------------------------------------------------------------------
-- 3.2 FACT_TRACK_SUMMARY_SILVER
------------------------------------------------------------------

-- Ensure table is empty before data load
TRUNCATE TABLE FACT_TRACK_SUMMARY_SILVER;

-- Insert Data
INSERT INTO FACT_TRACK_SUMMARY_SILVER (
    track_sk,
    snapshot_date_sk,
    course_count,
    total_chapters,
    total_exercises,
    total_videos,
    total_xp,
    avg_xp_per_course,
    avg_time_hours,
    total_duration_hours,
    datasets_count,
    participant_count,
    is_career_flag,
    predominant_difficulty_sk,
    load_ts
)
SELECT
    dt.track_sk                                          AS track_sk,
    dd.date_sk                                           AS snapshot_date_sk,

    TRY_TO_NUMBER(t.COURSE_COUNT)                        AS course_count,
    TRY_TO_NUMBER(t.TOTAL_CHAPTERS)                      AS total_chapters,
    TRY_TO_NUMBER(t.TOTAL_EXERCISES)                     AS total_exercises,
    TRY_TO_NUMBER(t.TOTAL_VIDEOS)                        AS total_videos,
    TRY_TO_NUMBER(t.TOTAL_XP)                            AS total_xp,
    TRY_TO_NUMBER(t.AVG_XP_PER_COURSE)                   AS avg_xp_per_course,
    TRY_TO_NUMBER(t.AVG_TIME_HOURS)                      AS avg_time_hours,
    TRY_TO_NUMBER(t.TOTAL_DURATION_HOURS)                AS total_duration_hours,
    TRY_TO_NUMBER(t.DATASETS_COUNT)                      AS datasets_count,

    -- some participant_count values may have commas
    TRY_TO_NUMBER(REPLACE(t.PARTICIPANT_COUNT, ',', '')) AS participant_count,

    -- reuse flags/FKs from DIM_TRACK
    dt.is_career_flag                                    AS is_career_flag,
    dt.predominant_difficulty_sk                         AS predominant_difficulty_sk,

    CURRENT_TIMESTAMP()                                  AS load_ts
FROM DB_TEAM_ANS.BRONZE.DCAMP_ALL_TRACKS_BRONZE t
JOIN DIM_TRACK dt
  ON dt.track_id = t.TRACK_ID
JOIN DIM_DATE dd
  ON dd.date_value = (SELECT MAX(date_value) FROM DIM_DATE);

-- Optional check
SELECT * FROM FACT_TRACK_SUMMARY_SILVER;

------------------------------------------------------------------
-- 4. OPTIONAL VALIDATIN QUERIES
------------------------------------------------------------------

------------------------------------------------------------------
-- 4.1 FACT_COURSE_SNAPSHOT_SILVER vs DCAMP_COURSES_BRONZE
------------------------------------------------------------------
-- 1) Row counts should match
SELECT 
  (SELECT COUNT(*) FROM DB_TEAM_ANS.BRONZE.DCAMP_COURSES_BRONZE) AS bronze_courses,
  (SELECT COUNT(*) FROM FACT_COURSE_SNAPSHOT_SILVER)             AS fact_courses;

-- 2) All fact courses should have a DIM_COURSE row
SELECT COUNT(*) AS orphan_facts
FROM FACT_COURSE_SNAPSHOT_SILVER f
LEFT JOIN DIM_COURSE dc
  ON dc.course_sk = f.course_sk
WHERE dc.course_sk IS NULL;

-- 3) Spot check measure equality for a few random courses
SELECT
  c.ID,
  c.NB_OF_SUBSCRIPTIONS AS bronze_subs,
  f.NB_OF_SUBSCRIPTIONS AS fact_subs,
  c.NUM_CHAPTERS,
  f.NUM_CHAPTERS
FROM DB_TEAM_ANS.BRONZE.DCAMP_COURSES_BRONZE c
JOIN DIM_COURSE dc
  ON dc.course_id = c.ID
JOIN FACT_COURSE_SNAPSHOT_SILVER f
  ON f.course_sk = dc.course_sk
ORDER BY RANDOM()
LIMIT 10;

------------------------------------------------------------------
-- 4.2 FACT_TRACK_SUMMARY_SILVER vs DCAMP_ALL_TRACKS_BRONZE
------------------------------------------------------------------
-- 1) Row counts
SELECT 
  (SELECT COUNT(*) FROM DB_TEAM_ANS.BRONZE.DCAMP_ALL_TRACKS_BRONZE) AS bronze_tracks,
  (SELECT COUNT(*) FROM FACT_TRACK_SUMMARY_SILVER)                  AS fact_tracks;

-- 2) Orphan track facts
SELECT COUNT(*) AS orphan_track_facts
FROM FACT_TRACK_SUMMARY_SILVER f
LEFT JOIN DIM_TRACK dt
  ON dt.track_sk = f.track_sk
WHERE dt.track_sk IS NULL;

-- 3) Random spot check measures vs Bronze
SELECT
  t.TRACK_ID,
  t.COURSE_COUNT    AS bronze_courses,
  f.COURSE_COUNT    AS fact_courses,
  t.TOTAL_XP        AS bronze_total_xp,
  f.TOTAL_XP        AS fact_total_xp
FROM DB_TEAM_ANS.BRONZE.DCAMP_ALL_TRACKS_BRONZE t
JOIN DIM_TRACK dt
  ON dt.track_id = t.TRACK_ID
JOIN FACT_TRACK_SUMMARY_SILVER f
  ON f.track_sk = dt.track_sk
ORDER BY RANDOM()
LIMIT 10;

------------------------------------------------------------------
-- END OF SCRIPT
------------------------------------------------------------------
