-- TERM PROJECT
-- TEAM NAME: ANS
-- MEMBER NAMES: Abhiroop Kumar (ak56448), Nikhil Kumar (nk25627), Simoni K Dalal (skd939)
-- DATABASE: DB_TEAM_ANS
-- ROLE: ROLE_TEAM_ANS
-- DUE: Dec 8 at 11:59pm

-- FILE NAME: ANS_SILVER_INCREMENRAL_DATA_UPDATE.sql

-- SOLUTION

------------------------------------------------------------------
-- 0. SET CONTEXT
------------------------------------------------------------------
USE ROLE ROLE_TEAM_ANS;
USE WAREHOUSE ANIMAL_TASK_WH;
USE DATABASE DB_TEAM_ANS;
USE SCHEMA DB_TEAM_ANS.SILVER;

------------------------------------------------------------------
-- 1. CREATE STORED PROCEDURES
------------------------------------------------------------------

------------------------------------------------------------------
-- 1.1 LOAD_SILVER_FACT_TABLES()
------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE LOAD_SILVER_FACT_TABLES()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
BEGIN


    -- FACT_COURSE_SNAPSHOT_SILVER
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
        ON dd.date_value = (SELECT MAX(date_value) FROM DIM_DATE)

    -- ✅ only insert if this (course, snapshot_date) does NOT already exist
    LEFT JOIN FACT_COURSE_SNAPSHOT_SILVER f
        ON f.course_sk        = dc.course_sk
       AND f.snapshot_date_sk = dd.date_sk
    WHERE f.course_sk IS NULL;


    -- 3.2 FACT_TRACK_SUMMARY_SILVER
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
        TRY_TO_NUMBER(REPLACE(t.PARTICIPANT_COUNT, ',', '')) AS participant_count,
        dt.is_career_flag                                    AS is_career_flag,
        dt.predominant_difficulty_sk                         AS predominant_difficulty_sk,
        CURRENT_TIMESTAMP()                                  AS load_ts
    FROM DB_TEAM_ANS.BRONZE.DCAMP_ALL_TRACKS_BRONZE t
    JOIN DIM_TRACK dt
      ON dt.track_id = t.TRACK_ID
    JOIN DIM_DATE dd
      ON dd.date_value = (SELECT MAX(date_value) FROM DIM_DATE)

    -- ✅ only insert if this (track, snapshot_date) does NOT already exist
    LEFT JOIN FACT_TRACK_SUMMARY_SILVER f
      ON f.track_sk        = dt.track_sk
     AND f.snapshot_date_sk = dd.date_sk
    WHERE f.track_sk IS NULL;

    RETURN 'Silver fact tables incrementally loaded.';
END;

------------------------------------------------------------------
-- 1.2 LOAD_SILVER_DIM_TABLES()
------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE LOAD_SILVER_DIM_TABLES()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
BEGIN


    -- DIM_DATE
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
        src.d                                         AS date_value,
        YEAR(src.d)                                   AS year,
        QUARTER(src.d)                                AS quarter,
        MONTH(src.d)                                  AS month,
        TO_CHAR(src.d, 'Month')                       AS month_name,
        DAY(src.d)                                    AS day_of_month,
        DAYOFWEEK(src.d)                              AS day_of_week,
        TO_CHAR(src.d, 'Day')                         AS day_name,
        WEEKOFYEAR(src.d)                             AS week_of_year,
        CASE WHEN DAYOFWEEK(src.d) IN (1,7)
             THEN TRUE ELSE FALSE END                 AS is_weekend
    FROM (
        SELECT DISTINCT 
            TO_DATE(c.LAST_UPDATED_ON, 'DD/MM/YYYY') AS d
        FROM DB_TEAM_ANS.BRONZE.DCAMP_COURSES_BRONZE c
        WHERE c.LAST_UPDATED_ON IS NOT NULL
    ) src
    LEFT JOIN DIM_DATE dd
        ON dd.date_value = src.d
    WHERE dd.date_value IS NULL;


    -- DIM_PROGRAMMING_LANGUAGE
    INSERT INTO DIM_PROGRAMMING_LANGUAGE (
        language_code,
        language_name
    )
    SELECT DISTINCT
        LOWER(TRIM(f.value::string))                  AS language_code,
        INITCAP(LOWER(TRIM(f.value::string)))         AS language_name
    FROM (
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


    -- DIM_DIFFICULTY  (static small table – no need to recreate)


    -- DIM_CONTENT_AREA
    INSERT INTO DIM_CONTENT_AREA (
        content_area_name
    )
    SELECT DISTINCT
        TRIM(c.CONTENT_AREA) AS content_area_name
    FROM DB_TEAM_ANS.BRONZE.DCAMP_COURSES_BRONZE c
    WHERE c.CONTENT_AREA IS NOT NULL
      AND TRIM(c.CONTENT_AREA) <> ''
      AND NOT EXISTS (
            SELECT 1
            FROM   DIM_CONTENT_AREA d
            WHERE  d.content_area_name = TRIM(c.CONTENT_AREA)
      );


    -- DIM_TOPIC
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


    -- DIM_TECHNOLOGY
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


    -- DIM_INSTRUCTOR
    INSERT INTO DIM_INSTRUCTOR (
        instructor_name
    )
    SELECT DISTINCT
        instructor_name
    FROM (
        SELECT
            REGEXP_REPLACE(TRIM(f.VALUE::STRING), '\\s+', ' ') AS instructor_name
        FROM DB_TEAM_ANS.BRONZE.DCAMP_COURSES_BRONZE c,
             LATERAL FLATTEN(SPLIT(c.INSTRUCTORS_NAMES, ';')) f

        UNION ALL

        SELECT
            REGEXP_REPLACE(TRIM(f.VALUE::STRING), '\\s+', ' ') AS instructor_name
        FROM DB_TEAM_ANS.BRONZE.DCAMP_ALL_TRACKS_BRONZE t,
             LATERAL FLATTEN(SPLIT(t.INSTRUCTORS, ',')) f
    ) src
    WHERE instructor_name IS NOT NULL
      AND instructor_name <> ''
      AND NOT EXISTS (
            SELECT 1
            FROM   DIM_INSTRUCTOR d
            WHERE  d.instructor_name = src.instructor_name
      );


    -- DIM_COLLABORATOR
    INSERT INTO DIM_COLLABORATOR (
        collaborator_name
    )
    SELECT DISTINCT
        REGEXP_REPLACE(TRIM(f.VALUE::STRING), '\\s+', ' ') AS collaborator_name
    FROM DB_TEAM_ANS.BRONZE.DCAMP_COURSES_BRONZE c,
         LATERAL FLATTEN(SPLIT(c.COLLABORATORS_NAMES, ';')) f
    WHERE collaborator_name IS NOT NULL
      AND collaborator_name <> ''
      AND NOT EXISTS (
            SELECT 1
            FROM   DIM_COLLABORATOR d
            WHERE  d.collaborator_name =
                    REGEXP_REPLACE(TRIM(f.VALUE::STRING), '\\s+', ' ')
      );


    -- DIM_TRACK
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
        IFF(LOWER(t.IS_CAREER) IN ('true','t','1','yes','y'), TRUE, FALSE),
        pl.language_sk,
        dd.difficulty_sk,
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


    -- DIM_COURSE
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
        pl.language_sk                               AS programming_language_sk,
        dd.difficulty_sk                             AS difficulty_sk,
        dt.topic_sk                                  AS topic_sk,
        te.technology_sk                             AS technology_sk,
        ca.content_area_sk                           AS content_area_sk,
        ddate.date_sk                                AS last_updated_date_sk,
        c.LINK                                       AS course_url,
        c.IMAGE_URL                                  AS image_url,
        c.INSTRUCTORS_NAMES                          AS raw_instructors_names,
        c.COLLABORATORS_NAMES                        AS raw_collaborators_names,
        c.TRACKS_TITLES                              AS raw_tracks_titles,
        c.PREREQUISITES_TITLES                       AS raw_prerequisites_titles
    FROM DB_TEAM_ANS.BRONZE.DCAMP_COURSES_BRONZE c
    LEFT JOIN DIM_PROGRAMMING_LANGUAGE pl
        ON pl.language_code = LOWER(TRIM(c.PROGRAMMING_LANGUAGE))
    LEFT JOIN DIM_DIFFICULTY dd
        ON dd.difficulty_order = TRY_TO_NUMBER(c.DIFFICULTY_LEVEL)
    LEFT JOIN DIM_TOPIC dt
        ON dt.topic_id = TRY_TO_NUMBER(c.TOPIC_ID)
    LEFT JOIN DIM_TECHNOLOGY te
        ON te.technology_id = c.TECHNOLOGY_ID
    LEFT JOIN DIM_CONTENT_AREA ca
        ON ca.content_area_name = c.CONTENT_AREA
    LEFT JOIN DIM_DATE ddate
        ON ddate.date_value = TRY_TO_DATE(c.LAST_UPDATED_ON, 'DD/MM/YYYY')
    LEFT JOIN DIM_COURSE existing
        ON existing.course_id = c.ID
    WHERE existing.course_id IS NULL;

    RETURN 'Silver dimension tables incrementally loaded.';
END;

------------------------------------------------------------------
-- 1.3 LOAD_SILVER_BRIDGE_TABLES()
------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE LOAD_SILVER_BRIDGE_TABLES()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
BEGIN


    -- BRIDGE_COURSE_INSTRUCTOR
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

        -- Collaborators (stored in same bridge with role = 'COLLABORATOR')
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
      ON di.instructor_name = src.person_name
    LEFT JOIN BRIDGE_COURSE_INSTRUCTOR bci
      ON bci.course_sk     = dc.course_sk
     AND bci.instructor_sk = di.instructor_sk
     AND bci.role          = src.role
    WHERE bci.course_sk IS NULL;


    -- BRIDGE_COURSE_TRACK
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
    LEFT JOIN BRIDGE_COURSE_TRACK bct
        ON bct.course_sk = dc.course_sk
       AND bct.track_sk  = dt.track_sk
    WHERE TRIM(f.VALUE::STRING) IS NOT NULL
      AND TRIM(f.VALUE::STRING) <> ''
      AND bct.course_sk IS NULL;


    -- BRIDGE_COURSE_PREREQUISITE
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
    LEFT JOIN BRIDGE_COURSE_PREREQUISITE bcp
        ON bcp.course_sk              = dc_main.course_sk
       AND bcp.prerequisite_course_sk = dc_pre.course_sk
    WHERE TRIM(f.VALUE::STRING) IS NOT NULL
      AND TRIM(f.VALUE::STRING) <> ''
      AND bcp.course_sk IS NULL;


    -- BRIDGE_COURSE_COLLABORATOR
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
    CROSS JOIN LATERAL FLATTEN(
        SPLIT(
            REGEXP_REPLACE(c.COLLABORATORS_NAMES, ',', ';'),
            ';'
        )
    ) f
    JOIN DIM_COLLABORATOR dcol
        ON dcol.collaborator_name =
           REGEXP_REPLACE(TRIM(f.VALUE::STRING), '\\s+', ' ')
    LEFT JOIN BRIDGE_COURSE_COLLABORATOR bcc
        ON bcc.course_sk       = dc.course_sk
       AND bcc.collaborator_sk = dcol.collaborator_sk
    WHERE TRIM(f.VALUE::STRING) IS NOT NULL
      AND TRIM(f.VALUE::STRING) <> ''
      AND bcc.course_sk IS NULL;

    RETURN 'Silver bridge tables incrementally loaded.';
END;

------------------------------------------------------------------
-- 1.5 REFRESH_SILVER_TABLES()
------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE REFRESH_SILVER_TABLES()
RETURNS VARCHAR NOT NULL
LANGUAGE SQL
AS
BEGIN

    CALL LOAD_SILVER_DIM_TABLES();
    CALL LOAD_SILVER_BRIDGE_TABLES();
    CALL LOAD_SILVER_FACT_TABLES();

    RETURN 'Silver dimensions, bridges, and facts refreshed.';
END;

-- Call REFRESH_SILVER_TABLES()
CALL REFRESH_SILVER_TABLES();

------------------------------------------------------------------
-- 2. CREATE SCHEDULED TASK (Dropped!)
------------------------------------------------------------------
-- Unusable due to insufficient role privileges.
-- Unable to grant EXECUTE TASK privilege to `ROLE_TEAM_ANS`.
-- Getting error, `Grant not executed: Insufficient privileges.`
-- As such, could not proceed with auto-ingestion of SILVER tables
------------------------------------------------------------------
-- CREATE OR REPLACE TASK REFRESH_SILVER_TABLES_TASK
--   WAREHOUSE = ANIMAL_TASK_WH
--   SCHEDULE = '1 MINUTE'  -- adjust as needed: '5 MINUTE', '1 HOUR', etc.
-- AS
--   CALL DB_TEAM_ANS.SILVER.REFRESH_SILVER_TABLES();   -- Call REFRESH_SILVER_TABLES()

-- -- Optional sanity check
-- SHOW TASKS IN SCHEMA DB_TEAM_ANS.SILVER;

-- -- Grant EXECUTE TASK to the task owner role
-- GRANT EXECUTE TASK ON ACCOUNT TO ROLE ROLE_TEAM_ANS;

-- -- Enable the task (by default a task is created suspended)
-- ALTER TASK REFRESH_SILVER_TABLES_TASK RESUME;

-- -- Temporarily stop it
-- -- ALTER TASK REFRESH_SILVER_TABLES_TASK SUSPEND;

-- -- Optional: see history once it starts running
-- SELECT *
-- FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
--     TASK_NAME => 'REFRESH_SILVER_TABLES_TASK',
--     RESULT_LIMIT => 20
-- ));

-- DROP TASK REFRESH_SILVER_TABLES_TASK;

------------------------------------------------------------------
-- END OF SCRIPT
------------------------------------------------------------------