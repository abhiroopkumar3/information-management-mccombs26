-- TERM PROJECT
-- TEAM NAME: ANS
-- MEMBER NAMES: Abhiroop Kumar (ak56448), Nikhil Kumar (nk25627), Simoni K Dalal (skd939)
-- DATABASE: DB_TEAM_ANS
-- ROLE: ROLE_TEAM_ANS
-- DUE: Dec 8 at 11:59pm

-- FILE NAME: ANS_TERM_PROJECT_MERGED_SCRIPT.sql

------------------------------------------------------------------------------------------------------
-- TABLE OF CONTENTS
--      [I] BRONZE_IMPORT
--      [II] ANS_SILVER_SETUP
--      [III] ANS_SILVER_STATIC_DATA
--      [IV] ANS_GOLD_SETUP
--      [V] ANS_AUDIT_LOG_DATA
--      [VI]    (a) ANS_GOLD_VISUALIZATION (contains Python code for Streamlit dashboard setup)
--      [VI]    (b) ANS_GOLD_STREAMLIT_SETUP (contains Python code Streamlit app setup)
--      [VII] ANS_BRONZE_INCREMENTAL_DATA_INGESTION
--      [VIII] ANS_SILVER_INCREMENRAL_DATA_UPDATE
--      [IX] ANS_AUDIT_LOG_DATA_INCREMENTAL
--      [X] ANS_AI_SQL
--      [XI] ANS_CORTEX_SEARCH
--      [XII] ANS_CORTEX_ANALYST (contains YAML code for Cortex Analyst)
--      [XIII] ALL_TABLES_QUERYING
------------------------------------------------------------------------------------------------------


-- SOLUTION

------------------------------------------------------------------------------------------------------
-- [I] BRONZE_IMPORT
------------------------------------------------------------------------------------------------------


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




------------------------------------------------------------------------------------------------------
-- [II] ANS_SILVER_SETUP
------------------------------------------------------------------------------------------------------


------------------------------------------------------------------
-- 0. SET CONTEXT
------------------------------------------------------------------
USE ROLE ROLE_TEAM_ANS;
USE WAREHOUSE ANIMAL_TASK_WH;
USE DATABASE DB_TEAM_ANS;

------------------------------------------------------------------
-- 1. CREATE SILVER SCHEMA
------------------------------------------------------------------
-- Drop schema before recreating it
DROP SCHEMA IF EXISTS SILVER CASCADE;

CREATE SCHEMA IF NOT EXISTS SILVER;

USE SCHEMA DB_TEAM_ANS.SILVER;

------------------------------------------------------------------
-- 2. OPTIONAL REVIEW OF BRONZE SCHEMA TABLES
------------------------------------------------------------------
-- View DCAMP_COURSES_BRONZE Table
SELECT * FROM DB_TEAM_ANS.BRONZE.DCAMP_COURSES_BRONZE;

-- View DCAMP_ALL_TRACKS_BRONZE Table
SELECT * FROM DB_TEAM_ANS.BRONZE.DCAMP_ALL_TRACKS_BRONZE;

-- View DCAMP_TOPIC_MAPPING_BRONZE Table
SELECT * FROM DB_TEAM_ANS.BRONZE.DCAMP_TOPIC_MAPPING_BRONZE;

-- View DCAMP_TECHNOLOGY_MAPPING_BRONZE Table
SELECT * FROM DB_TEAM_ANS.BRONZE.DCAMP_TECHNOLOGY_MAPPING_BRONZE;

------------------------------------------------------------------
-- 3. CREATE SEQUENCES FOR PRIMARY KEY
------------------------------------------------------------------
-- For Dimension Tables
CREATE OR REPLACE SEQUENCE DIM_PK_SEQ_1 START with 1 INCREMENT by 1 order;
CREATE OR REPLACE SEQUENCE DIM_PK_SEQ_2 START with 1 INCREMENT by 1 order;
CREATE OR REPLACE SEQUENCE DIM_PK_SEQ_3 START with 1 INCREMENT by 1 order;
CREATE OR REPLACE SEQUENCE DIM_PK_SEQ_4 START with 1 INCREMENT by 1 order;
CREATE OR REPLACE SEQUENCE DIM_PK_SEQ_5 START with 1 INCREMENT by 1 order;
CREATE OR REPLACE SEQUENCE DIM_PK_SEQ_6 START with 1 INCREMENT by 1 order;
CREATE OR REPLACE SEQUENCE DIM_PK_SEQ_7 START with 1 INCREMENT by 1 order;
CREATE OR REPLACE SEQUENCE DIM_PK_SEQ_8 START with 1 INCREMENT by 1 order;
CREATE OR REPLACE SEQUENCE DIM_PK_SEQ_9 START with 1 INCREMENT by 1 order;
CREATE OR REPLACE SEQUENCE DIM_PK_SEQ_10 START with 1 INCREMENT by 1 order;

-- For Fact Tables
CREATE OR REPLACE SEQUENCE FACT_PK_SEQ_1 START with 1 INCREMENT by 1 order;
CREATE OR REPLACE SEQUENCE FACT_PK_SEQ_2 START with 1 INCREMENT by 1 order;

-- For Audit Log Table
CREATE OR REPLACE SEQUENCE S_AUDIT_PK_SEQ_1 START with 1 INCREMENT by 1 order;

------------------------------------------------------------------
-- 4. CREATE SILVER DIMENSION TABLES
------------------------------------------------------------------

------------------------------------------------------------------
-- 4.1 DIM_DATE
------------------------------------------------------------------
-- Purpose:
--   Standard calendar dimension used for snapshot dates and
--   course last_updated_on dates.
-- Source:
--   Not directly from Bronze; usually generated via a date spine.
-- FKs / Usage:
--   Referenced by FACT_COURSE_SNAPSHOT_SILVER.snapshot_date_sk,
--   FACT_TRACK_SUMMARY_SILVER.snapshot_date_sk,
--   DIM_COURSE.last_updated_date_sk.
------------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_DATE (
    date_sk        NUMBER PRIMARY KEY DEFAULT DIM_PK_SEQ_1.nextval, -- surrogate PK
    date_value     DATE,                                    -- actual calendar date
    year           NUMBER,
    quarter        NUMBER,
    month          NUMBER,
    month_name     VARCHAR,
    day_of_month   NUMBER,
    day_of_week    NUMBER,
    day_name       VARCHAR,
    week_of_year   NUMBER,
    is_weekend     BOOLEAN
);

------------------------------------------------------------------
-- 4.2 DIM_PROGRAMMING_LANGUAGE
------------------------------------------------------------------
-- Purpose:
--   Lookup dimension for programming_language values found in
--   DCAMP_COURSES_BRONZE and DCAMP_ALL_TRACKS_BRONZE.
-- Source:
--   programming_language (courses + tracks).
-- FKs / Usage:
--   Referenced by DIM_COURSE.programming_language_sk and DIM_TRACK.primary_language_sk.
------------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_PROGRAMMING_LANGUAGE (
    language_sk    NUMBER PRIMARY KEY DEFAULT DIM_PK_SEQ_2.nextval, -- surrogate PK
    language_code  VARCHAR,  -- e.g. 'py', 'r'
    language_name  VARCHAR  -- e.g. 'Python', 'R'
);

------------------------------------------------------------------
-- 4.3 DIM_DIFFICULTY
------------------------------------------------------------------
-- Purpose:
--   Normalized difficulty level lookup, capturing distinct difficulty
--   labels across courses and tracks.
-- Source:
--   DCAMP_COURSES_BRONZE.difficulty_level,
--   DCAMP_ALL_TRACKS_BRONZE.course_difficulty_levels,
--   DCAMP_ALL_TRACKS_BRONZE.predominant_difficulty.
-- FKs / Usage:
--   Referenced by DIM_COURSE.difficulty_sk and DIM_TRACK.predominant_difficulty_sk.
------------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_DIFFICULTY (
    difficulty_sk    NUMBER PRIMARY KEY DEFAULT DIM_PK_SEQ_3.nextval, -- surrogate PK
    difficulty_code  VARCHAR,  -- e.g. 'Beginner', 'Intermediate', 'Advanced'
    difficulty_order NUMBER   -- numeric ordering, e.g. 1,2,3
);

------------------------------------------------------------------
-- 4.4 DIM_CONTENT_AREA
------------------------------------------------------------------
-- Purpose:
--   Content area lookup (e.g., SQL, Python, Machine Learning).
-- Source:
--   DCAMP_COURSES_BRONZE.content_area.
-- FKs / Usage:
--   Referenced by DIM_COURSE.content_area_sk.
------------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_CONTENT_AREA (
    content_area_sk   NUMBER PRIMARY KEY DEFAULT DIM_PK_SEQ_4.nextval, -- surrogate PK
    content_area_name VARCHAR
);

------------------------------------------------------------------
-- 4.5 DIM_TOPIC
------------------------------------------------------------------
-- Purpose:
--   Topic lookup dimension with IDs and names.
-- Source:
--   DCAMP_TOPIC_MAPPING_BRONZE (topic_id, topic_name).
-- FKs / Usage:
--   DIM_COURSE.topic_sk references this table.
------------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_TOPIC (
    topic_sk   NUMBER PRIMARY KEY DEFAULT DIM_PK_SEQ_5.nextval, -- surrogate PK
    topic_id   VARCHAR,  -- natural key from Bronze
    topic_name VARCHAR
);

------------------------------------------------------------------
-- 4.6 DIM_TECHNOLOGY
------------------------------------------------------------------
-- Purpose:
--   Technology lookup dimension with IDs and names.
-- Source:
--   DCAMP_TECHNOLOGY_MAPPING_BRONZE (technology_id, technology_name).
-- FKs / Usage:
--   DIM_COURSE.technology_sk references this table.
------------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_TECHNOLOGY (
    technology_sk   NUMBER PRIMARY KEY DEFAULT DIM_PK_SEQ_6.nextval, -- surrogate PK
    technology_id   VARCHAR,  -- natural key from Bronze
    technology_name VARCHAR
);

------------------------------------------------------------------
-- 4.7 DIM_INSTRUCTOR
------------------------------------------------------------------
-- Purpose:
--   One row per unique instructor across both courses and tracks.
-- Source:
--   DCAMP_COURSES_BRONZE.instructors_names (semicolon-separated),
--   DCAMP_ALL_TRACKS_BRONZE.instructors (comma-separated)
--   split & deduped during ETL in 03 - ANS_SILVER_STATIC_DATA.sql.
-- FKs / Usage:
--   BRIDGE_COURSE_INSTRUCTOR.instructor_sk references this table.
------------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_INSTRUCTOR (
    instructor_sk   NUMBER PRIMARY KEY DEFAULT DIM_PK_SEQ_7.nextval, -- surrogate PK
    instructor_name VARCHAR  -- canonical instructor name
);

------------------------------------------------------------------
-- 4.8 DIM_COLLABORATOR
------------------------------------------------------------------
-- Purpose:
--   One row per unique collaborator from course-level data.
-- Source:
--   DCAMP_COURSES_BRONZE.collaborators_names (semicolon-separated)
--   split & deduped during ETL.
-- Usage:
--   Currently used for lookup / reporting; you can add a
--   dedicated BRIDGE_COURSE_COLLABORATOR later if needed.
------------------------------------------------------------------

CREATE OR REPLACE TABLE DIM_COLLABORATOR (
    collaborator_sk   NUMBER PRIMARY KEY DEFAULT DIM_PK_SEQ_8.nextval, -- surrogate PK
    collaborator_name VARCHAR   -- collaborator name from COLLABORATORS_NAMES
);

------------------------------------------------------------------
-- 4.9 DIM_TRACK
------------------------------------------------------------------
-- Purpose:
--   Track-level descriptive attributes: title, type, difficulty,
--   etc., separate from track-level metrics in FACT_TRACK_SUMMARY_SILVER.
-- Source:
--   DCAMP_ALL_TRACKS_BRONZE.* plus cross-links from courses.tracks_titles.
-- FKs / Usage:
--   Referenced by FACT_TRACK_SUMMARY_SILVER.track_sk
--   and BRIDGE_COURSE_TRACK.track_sk.
------------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_TRACK (
    track_sk                     NUMBER PRIMARY KEY DEFAULT DIM_PK_SEQ_9.nextval, -- surrogate PK
    track_id                     VARCHAR,  -- natural key from Bronze
    track_title                  VARCHAR,
    is_career_flag               BOOLEAN,  -- parsed from is_career
    programming_language_sk      NUMBER,   -- FK to DIM_PROGRAMMING_LANGUAGE
    predominant_difficulty_sk    NUMBER,   -- FK to DIM_DIFFICULTY

    -- optional "raw" attributes carried for lineage/debugging
    raw_course_difficulty_levels VARCHAR,
    raw_course_titles            VARCHAR,
    raw_instructors              VARCHAR,
    raw_programming_languages    VARCHAR,  -- full multi-valued PROGRAMMING_LANGUAGE from Bronze


    CONSTRAINT FK_DIM_TRACK_LANGUAGE
        FOREIGN KEY (programming_language_sk) REFERENCES DIM_PROGRAMMING_LANGUAGE(language_sk),
    CONSTRAINT FK_DIM_TRACK_DIFFICULTY
        FOREIGN KEY (predominant_difficulty_sk) REFERENCES DIM_DIFFICULTY(difficulty_sk)
);

------------------------------------------------------------------
-- 4.10 DIM_COURSE
------------------------------------------------------------------
-- Purpose:
--   Central course dimension with stable surrogate key and
--   descriptive attributes (titles, difficulty, topic, tech, etc.).
-- Source:
--   DCAMP_COURSES_BRONZE.* (1 row per course).
-- FKs / Usage:
--   Referenced by FACT_COURSE_SNAPSHOT_SILVER.course_sk,
--   BRIDGE_COURSE_INSTRUCTOR.course_sk,
--   BRIDGE_COURSE_TRACK.course_sk,
--   BRIDGE_COURSE_PREREQUISITE.course_sk & prerequisite_course_sk.
------------------------------------------------------------------
CREATE OR REPLACE TABLE DIM_COURSE (
    course_sk               NUMBER PRIMARY KEY DEFAULT DIM_PK_SEQ_10.nextval, -- surrogate PK
    course_id               VARCHAR,   -- natural key from DCAMP_COURSES_BRONZE.id

    -- core descriptive attributes
    title                   VARCHAR,
    short_description       VARCHAR,
    description             VARCHAR,
    xp                      NUMBER,    -- cast from VARCHAR xp
    time_needed_hours       NUMBER,    -- cast from VARCHAR time_needed_in_hours

    -- foreign key references to normalized lookups
    programming_language_sk NUMBER,    -- FK -> DIM_PROGRAMMING_LANGUAGE
    difficulty_sk           NUMBER,    -- FK -> DIM_DIFFICULTY
    topic_sk                NUMBER,    -- FK -> DIM_TOPIC
    technology_sk           NUMBER,    -- FK -> DIM_TECHNOLOGY
    content_area_sk         NUMBER,    -- FK -> DIM_CONTENT_AREA
    last_updated_date_sk    NUMBER,    -- FK -> DIM_DATE

    course_url              VARCHAR,   -- from link
    image_url               VARCHAR,   -- from image_url

    -- raw list-like attributes preserved for lineage / debugging (not used in joins)
    raw_instructors_names   VARCHAR,   -- original instructors_names string
    raw_collaborators_names VARCHAR,
    raw_tracks_titles       VARCHAR,
    raw_prerequisites_titles VARCHAR,

    CONSTRAINT FK_DIM_COURSE_LANGUAGE
        FOREIGN KEY (programming_language_sk) REFERENCES DIM_PROGRAMMING_LANGUAGE(language_sk),
    CONSTRAINT FK_DIM_COURSE_DIFFICULTY
        FOREIGN KEY (difficulty_sk) REFERENCES DIM_DIFFICULTY(difficulty_sk),
    CONSTRAINT FK_DIM_COURSE_TOPIC
        FOREIGN KEY (topic_sk) REFERENCES DIM_TOPIC(topic_sk),
    CONSTRAINT FK_DIM_COURSE_TECHNOLOGY
        FOREIGN KEY (technology_sk) REFERENCES DIM_TECHNOLOGY(technology_sk),
    CONSTRAINT FK_DIM_COURSE_CONTENT_AREA
        FOREIGN KEY (content_area_sk) REFERENCES DIM_CONTENT_AREA(content_area_sk),
    CONSTRAINT FK_DIM_COURSE_LAST_UPDATED_DATE
        FOREIGN KEY (last_updated_date_sk) REFERENCES DIM_DATE(date_sk)
);

------------------------------------------------------------------
-- 5. CREATE SILVER FACT TABLES
------------------------------------------------------------------

------------------------------------------------------------------
-- 5.1 FACT_COURSE_SNAPSHOT_SILVER
------------------------------------------------------------------
-- Purpose:
--   Main course-level fact table, storing numeric measures for
--   each course at each snapshot/load date.
-- Source:
--   DCAMP_COURSES_BRONZE metrics:
--      nb_of_subscriptions, num_chapters, num_exercises,
--      num_videos, datasets_count.
--   One row per (course, snapshot_date).
-- FKs / Usage:
--   course_sk -> DIM_COURSE
--   snapshot_date_sk -> DIM_DATE
--   Supports time-series analytics of course popularity and size.
------------------------------------------------------------------
CREATE OR REPLACE TABLE FACT_COURSE_SNAPSHOT_SILVER (
    course_snapshot_sk  NUMBER PRIMARY KEY DEFAULT FACT_PK_SEQ_1.nextval, -- surrogate PK

    course_sk           NUMBER NOT NULL, -- FK to DIM_COURSE
    snapshot_date_sk    NUMBER NOT NULL, -- FK to DIM_DATE

    -- measures, casted from VARCHAR in Bronze
    nb_of_subscriptions NUMBER,  -- from DCAMP_COURSES_BRONZE.nb_of_subscriptions
    num_chapters        NUMBER,  -- from num_chapters
    num_exercises       NUMBER,  -- from num_exercises
    num_videos          NUMBER,  -- from num_videos
    datasets_count      NUMBER,  -- from datasets_count

    load_ts             TIMESTAMP_NTZ,   -- ETL load timestamp

    CONSTRAINT FK_FCS_COURSE
        FOREIGN KEY (course_sk) REFERENCES DIM_COURSE(course_sk),
    CONSTRAINT FK_FCS_SNAPSHOT_DATE
        FOREIGN KEY (snapshot_date_sk) REFERENCES DIM_DATE(date_sk)
);

------------------------------------------------------------------
-- 5.2 FACT_TRACK_SUMMARY_SILVER
------------------------------------------------------------------
-- Purpose:
--   Track-level fact table with aggregated metrics such as
--   course_count, total_xp, avg_time_hours, etc.
-- Source:
--   DCAMP_ALL_TRACKS_BRONZE.* (already aggregated at track level).
--   One row per (track, snapshot_date).
-- FKs / Usage:
--   track_sk -> DIM_TRACK
--   snapshot_date_sk -> DIM_DATE
--   Supports portfolio analysis of tracks over time.
------------------------------------------------------------------
CREATE OR REPLACE TABLE FACT_TRACK_SUMMARY_SILVER (
    track_summary_sk       NUMBER PRIMARY KEY DEFAULT FACT_PK_SEQ_2.nextval, -- surrogate PK

    track_sk               NUMBER NOT NULL, -- FK to DIM_TRACK
    snapshot_date_sk       NUMBER NOT NULL, -- FK to DIM_DATE

    -- measures, casted from VARCHAR in Bronze
    course_count           NUMBER,
    total_chapters         NUMBER,
    total_exercises        NUMBER,
    total_videos           NUMBER,
    total_xp               NUMBER,
    avg_xp_per_course      NUMBER(18, 2),
    avg_time_hours         NUMBER(18, 2),
    total_duration_hours   NUMBER(18, 2),
    datasets_count         NUMBER,
    participant_count      NUMBER,

    -- descriptive / flag attributes
    is_career_flag         BOOLEAN,  -- parsed from is_career
    predominant_difficulty_sk NUMBER, -- FK to DIM_DIFFICULTY

    load_ts                TIMESTAMP_NTZ, -- ETL load timestamp

    CONSTRAINT FK_FTS_TRACK
        FOREIGN KEY (track_sk) REFERENCES DIM_TRACK(track_sk),
    CONSTRAINT FK_FTS_SNAPSHOT_DATE
        FOREIGN KEY (snapshot_date_sk) REFERENCES DIM_DATE(date_sk),
    CONSTRAINT FK_FTS_PREDOMINANT_DIFFICULTY
        FOREIGN KEY (predominant_difficulty_sk) REFERENCES DIM_DIFFICULTY(difficulty_sk)
);

------------------------------------------------------------------
-- 6. CREATE SILVER BRIDGE (SUB-DIMENSION) TABLES
------------------------------------------------------------------

------------------------------------------------------------------
-- 6.1 BRIDGE_COURSE_INSTRUCTOR
------------------------------------------------------------------
-- Purpose:
--   Many-to-many relationship between courses and instructors:
--   each course can have multiple instructors/collaborators, and
--   each instructor can teach multiple courses.
-- Source:
--   DCAMP_COURSES_BRONZE.instructors_names,
--   DCAMP_COURSES_BRONZE.collaborators_names,
--   DCAMP_ALL_TRACKS_BRONZE.instructors (for track-level teaching).
-- FKs / Usage:
--   course_sk -> DIM_COURSE
--   instructor_sk -> DIM_INSTRUCTOR
--   role distinguishes instructor vs collaborator, etc.
------------------------------------------------------------------
CREATE OR REPLACE TABLE BRIDGE_COURSE_INSTRUCTOR (
    course_sk      NUMBER NOT NULL, -- FK to DIM_COURSE
    instructor_sk  NUMBER NOT NULL, -- FK to DIM_INSTRUCTOR
    role           VARCHAR,         -- e.g. 'INSTRUCTOR', 'COLLABORATOR', 'TRACK_INSTRUCTOR'
    instructor_order NUMBER,        -- optional ordering within a course

    CONSTRAINT PK_BRIDGE_COURSE_INSTRUCTOR
        PRIMARY KEY (course_sk, instructor_sk, role),
    CONSTRAINT FK_BCI_COURSE
        FOREIGN KEY (course_sk) REFERENCES DIM_COURSE(course_sk),
    CONSTRAINT FK_BCI_INSTRUCTOR
        FOREIGN KEY (instructor_sk) REFERENCES DIM_INSTRUCTOR(instructor_sk)
);

------------------------------------------------------------------
-- 6.2 BRIDGE_COURSE_TRACK
------------------------------------------------------------------
-- Purpose:
--   Many-to-many relationship between courses and tracks:
--   courses can belong to multiple tracks, tracks contain multiple courses.
-- Source:
--   DCAMP_COURSES_BRONZE.tracks_titles (parsed),
--   DCAMP_ALL_TRACKS_BRONZE.track_title (for ID mapping).
-- FKs / Usage:
--   course_sk -> DIM_COURSE
--   track_sk -> DIM_TRACK
--   Enables joining course-level metrics with track-level context.
------------------------------------------------------------------
CREATE OR REPLACE TABLE BRIDGE_COURSE_TRACK (
    course_sk  NUMBER NOT NULL, -- FK to DIM_COURSE
    track_sk   NUMBER NOT NULL, -- FK to DIM_TRACK

    CONSTRAINT PK_BRIDGE_COURSE_TRACK
        PRIMARY KEY (course_sk, track_sk),
    CONSTRAINT FK_BCT_COURSE
        FOREIGN KEY (course_sk) REFERENCES DIM_COURSE(course_sk),
    CONSTRAINT FK_BCT_TRACK
        FOREIGN KEY (track_sk) REFERENCES DIM_TRACK(track_sk)
);

------------------------------------------------------------------
-- 6.3 BRIDGE_COURSE_PREREQUISITE
------------------------------------------------------------------
-- Purpose:
--   Self-referencing many-to-many relationship between courses
--   indicating prerequisite chains.
-- Source:
--   DCAMP_COURSES_BRONZE.prerequisites_titles (parsed and resolved
--   back to DIM_COURSE via course titles/IDs).
-- FKs / Usage:
--   course_sk -> DIM_COURSE (the course requiring the prereq)
--   prerequisite_course_sk -> DIM_COURSE (the required course)
--   Enables graph-style analysis of prerequisite networks.
------------------------------------------------------------------
CREATE OR REPLACE TABLE BRIDGE_COURSE_PREREQUISITE (
    course_sk              NUMBER NOT NULL, -- FK to DIM_COURSE (dependent course)
    prerequisite_course_sk NUMBER NOT NULL, -- FK to DIM_COURSE (required course)

    CONSTRAINT PK_BRIDGE_COURSE_PREREQUISITE
        PRIMARY KEY (course_sk, prerequisite_course_sk),
    CONSTRAINT FK_BCP_COURSE
        FOREIGN KEY (course_sk) REFERENCES DIM_COURSE(course_sk),
    CONSTRAINT FK_BCP_PREREQUISITE_COURSE
        FOREIGN KEY (prerequisite_course_sk) REFERENCES DIM_COURSE(course_sk)
);

------------------------------------------------------------------
-- 6.4 BRIDGE_COURSE_COLLABORATOR
------------------------------------------------------------------
-- Purpose:
--   Many-to-many relationship between courses and collaborators.
--   A collaborator is not necessarily an instructor.
-- Source:
--   DCAMP_COURSES_BRONZE.COLLABORATORS_NAMES (split by ';')
-- FKs / Usage:
--   course_sk  -> DIM_COURSE
--   collaborator_sk -> DIM_COLLABORATOR
------------------------------------------------------------------

CREATE OR REPLACE TABLE BRIDGE_COURSE_COLLABORATOR (
    course_sk         NUMBER NOT NULL,
    collaborator_sk   NUMBER NOT NULL,

    CONSTRAINT PK_BRIDGE_COURSE_COLLABORATOR
        PRIMARY KEY (course_sk, collaborator_sk),

    CONSTRAINT FK_BCC_COURSE
        FOREIGN KEY (course_sk) REFERENCES DIM_COURSE(course_sk),

    CONSTRAINT FK_BCC_COLLABORATOR
        FOREIGN KEY (collaborator_sk) REFERENCES DIM_COLLABORATOR(collaborator_sk)
);

------------------------------------------------------------------
-- 7. CREATE AUDIT TABLE FOR ROW-COUNT COMPARISONS (INCREMENTAL DATA)
------------------------------------------------------------------
CREATE OR REPLACE TABLE SILVER_LOAD_AUDIT (
    audit_id                       NUMBER PRIMARY KEY DEFAULT S_AUDIT_PK_SEQ_1.nextval,
    load_ts                        TIMESTAMP_NTZ,

    -- Courses side
    bronze_courses_row_count       NUMBER,
    silver_dim_course_row_count    NUMBER,
    silver_fact_course_rows        NUMBER,

    -- Tracks side
    bronze_tracks_row_count        NUMBER,
    silver_dim_track_row_count     NUMBER,
    silver_fact_track_rows         NUMBER
);




------------------------------------------------------------------------------------------------------
-- [III] ANS_SILVER_STATIC_DATA
------------------------------------------------------------------------------------------------------


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
    TO_CHAR(d, 'Month')                         AS month_name,
    DAY(d)                                      AS day_of_month,
    DAYOFWEEK(d)                                AS day_of_week,
    TO_CHAR(d, 'Day')                           AS day_name,
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




------------------------------------------------------------------------------------------------------
-- [IV] ANS_GOLD_SETUP
------------------------------------------------------------------------------------------------------


------------------------------------------------------------------
-- 0. SET CONTEXT
------------------------------------------------------------------
USE ROLE ROLE_TEAM_ANS;
USE WAREHOUSE ANIMAL_TASK_WH;
USE DATABASE DB_TEAM_ANS;

------------------------------------------------------------------
-- 1. CREATE GOLD SCHEMA
------------------------------------------------------------------
-- Drop schema before recreating it
DROP SCHEMA IF EXISTS GOLD CASCADE;

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
-- 3. CREATE GOLD TABLES
------------------------------------------------------------------

-- NOTE: Gold tables will be created as Dynamic tables to ensure that the incremental data from silver tables is updated in the gold tables immediately (TARGET_LAG = '1 MINUTE')

------------------------------------------------------------------
-- 3.1 G_LANGUAGE_INSTRUCTIONAL_EFFORT
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
CREATE OR REPLACE DYNAMIC TABLE G_LANGUAGE_INSTRUCTIONAL_EFFORT
TARGET_LAG = '1 MINUTE'
WAREHOUSE = 'ANIMAL_TASK_WH'
AS
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
-- 3.2. G_TRACK_CONTENT_SUMMARY
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
CREATE OR REPLACE DYNAMIC TABLE G_TRACK_CONTENT_SUMMARY
TARGET_LAG = '1 MINUTE'
WAREHOUSE = 'ANIMAL_TASK_WH'
AS
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
-- 3.3. G_DIFFICULTY_CONTENT_SUMMARY
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
CREATE OR REPLACE DYNAMIC TABLE G_DIFFICULTY_CONTENT_SUMMARY
TARGET_LAG = '1 MINUTE'
WAREHOUSE = 'ANIMAL_TASK_WH'
AS
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
-- 8. CREATE AUDIT TABLE FOR ROW-COUNT COMPARISONS (INCREMENTAL DATA)
------------------------------------------------------------------
-- Create Sequence for Audit Log Table
CREATE OR REPLACE SEQUENCE G_AUDIT_PK_SEQ_1 START with 1 INCREMENT by 1 order;

CREATE TABLE IF NOT EXISTS GOLD_LOAD_AUDIT (
    audit_id                              NUMBER PRIMARY KEY DEFAULT G_AUDIT_PK_SEQ_1.nextval,
    load_ts                               TIMESTAMP_NTZ,

    -- Gold layer row counts
    gold_language_rows                    NUMBER,
    gold_track_content_rows               NUMBER,
    gold_difficulty_content_rows          NUMBER,

    -- Silver layer comparison (optional but nice for validation)
    silver_fact_course_rows               NUMBER,
    silver_fact_track_rows                NUMBER
);




------------------------------------------------------------------------------------------------------
-- [V] ANS_AUDIT_LOG_DATA
------------------------------------------------------------------------------------------------------


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




------------------------------------------------------------------------------------------------------
-- [VI](a) ANS_GOLD_STREAMLIT_SETUP
------------------------------------------------------------------------------------------------------


------------------------------------------------------------------
-- 0. SET CONTEXT
------------------------------------------------------------------

USE ROLE ROLE_TEAM_ANS;
USE WAREHOUSE ANIMAL_TASK_WH;
USE DATABASE DB_TEAM_ANS;
USE SCHEMA GOLD;

------------------------------------------------------------------
-- 1. LANGUAGE INSTRUCTIONAL EFFORT (bar chart)
------------------------------------------------------------------
-- Gold visualization 1: Programming language instructional effort
-- This powers Visualization 1: “Total learning hours by programming language”.
------------------------------------------------------------------
SELECT
    LANGUAGE_NAME,
    COURSE_COUNT,
    TOTAL_TIME_HOURS,
    TOTAL_CHAPTERS,
    TOTAL_EXERCISES,
    TOTAL_VIDEOS
FROM G_LANGUAGE_INSTRUCTIONAL_EFFORT
ORDER BY TOTAL_TIME_HOURS DESC;

------------------------------------------------------------------
-- 2. TRACK-LEVEL SUMMARY (bar chart with optional filter)
------------------------------------------------------------------
-- Gold visualization 2: Track-level content summary
-- This powers Visualization 2: “Total learning hours by track”.
-- (optional filter for 'Career vs Skill' in Streamlit)
------------------------------------------------------------------
SELECT
    TRACK_TITLE,
    IS_CAREER_FLAG,
    COURSE_COUNT,
    TOTAL_TIME_HOURS,
    TOTAL_CHAPTERS,
    TOTAL_EXERCISES,
    TOTAL_VIDEOS
FROM DB_TEAM_ANS.GOLD.G_TRACK_CONTENT_SUMMARY
ORDER BY TOTAL_TIME_HOURS DESC;

------------------------------------------------------------------
-- 3. DIFFICULTY DISTRIBUTION (bar chart)
------------------------------------------------------------------
-- Gold visualization 3: Difficulty-level content and distribution
-- This powers Visualization 3: “Course distribution and content by difficulty level.”
------------------------------------------------------------------
SELECT
    DIFFICULTY_CODE,
    DIFFICULTY_ORDER,
    COURSE_COUNT,
    TOTAL_TIME_HOURS,
    TOTAL_CHAPTERS,
    TOTAL_EXERCISES,
    TOTAL_VIDEOS
FROM DB_TEAM_ANS.GOLD.G_DIFFICULTY_CONTENT_SUMMARY
ORDER BY DIFFICULTY_ORDER;

-- PYTHON CODE FOR STREAMLIT DASHBOARD
-- CODE BLOCK BEGINS BELOW
-- [

-- ##################################################################
-- ## 4. STREAMLIT DASHBOARD FROM GOLD TABLES
-- ##################################################################

-- # Import python packages
-- import streamlit as st
-- import pandas as pd
-- from snowflake.snowpark.context import get_active_session

-- # Get active Snowflake session (needed to run additional session.sql())
-- session = get_active_session()

-- # Convert SQL cell outputs to pandas DataFrames
-- # IMPORTANT: names must match the SQL cell "name" fields:
-- #   lang_effort_sql, track_content_sql, difficulty_content_sql
-- lang_df = lang_effort_sql.to_pandas()
-- track_df = track_content_sql.to_pandas()
-- diff_df = difficulty_content_sql.to_pandas()

-- # Basic Streamlit page configuration
-- st.title("DataCamp Learning Content – Gold Layer Dashboard")
-- st.caption(
--     "Visualizations built from GOLD tables: "
--     "`G_LANGUAGE_INSTRUCTIONAL_EFFORT`, "
--     "`G_TRACK_CONTENT_SUMMARY`, "
--     "`G_DIFFICULTY_CONTENT_SUMMARY`."
-- )

-- # ---- Tabs for three Gold layer visualizations ----
-- tab_lang, tab_track, tab_diff = st.tabs(
--     ["1. By Programming Language", "2. By Track", "3. By Difficulty"]
-- )

-- # -------------------------------------------------------------------
-- # TAB 1: Programming Language Instructional Effort
-- # (Gold: G_LANGUAGE_INSTRUCTIONAL_EFFORT)
-- # -------------------------------------------------------------------
-- with tab_lang:
--     st.subheader("Instructional Effort by Programming Language")

--     # Optional: show data table
--     st.dataframe(lang_df)

--     # Main visualization: bar chart of total time by language
--     st.markdown("**Total learning hours per programming language**")
--     st.bar_chart(
--         lang_df,
--         x="LANGUAGE_NAME",
--         y="TOTAL_TIME_HOURS"
--     )

--     # Secondary metric: total chapters or exercises (if you want)
--     st.markdown("**Total chapters per programming language**")
--     st.bar_chart(
--         lang_df,
--         x="LANGUAGE_NAME",
--         y="TOTAL_CHAPTERS"
--     )

-- # -------------------------------------------------------------------
-- # TAB 2: Track-level Summary
-- # (Gold: G_TRACK_CONTENT_SUMMARY)
-- # -------------------------------------------------------------------
-- with tab_track:
--     st.subheader("Content Summary by Track")

--     # Filter career vs skill tracks (IS_CAREER_FLAG assumed 1 = Career, 0 = Skill)
--     track_type = st.radio(
--         "Filter tracks by type:",
--         options=["All", "Career Tracks", "Skill Tracks"],
--         horizontal=True
--     )

--     track_filtered = track_df.copy()
--     if track_type == "Career Tracks":
--         track_filtered = track_filtered[track_filtered["IS_CAREER_FLAG"] == 1]
--     elif track_type == "Skill Tracks":
--         track_filtered = track_filtered[track_filtered["IS_CAREER_FLAG"] == 0]

--     st.dataframe(track_filtered)

--     st.markdown("**Total learning hours per track**")
--     st.bar_chart(
--         track_filtered,
--         x="TRACK_TITLE",
--         y="TOTAL_TIME_HOURS"
--     )

--     # Optional: show course count as a second chart
--     st.markdown("**Number of courses per track**")
--     st.bar_chart(
--         track_filtered,
--         x="TRACK_TITLE",
--         y="COURSE_COUNT"
--     )

-- # -------------------------------------------------------------------
-- # TAB 3: Difficulty Distribution & Depth
-- # (Gold: G_DIFFICULTY_CONTENT_SUMMARY)
-- # -------------------------------------------------------------------
-- with tab_diff:
--     st.subheader("Course Distribution and Content by Difficulty")

--     st.dataframe(diff_df)

--     col1, col2 = st.columns(2)

--     with col1:
--         st.markdown("**Course count by difficulty level**")
--         st.bar_chart(
--             diff_df,
--             x="DIFFICULTY_CODE",
--             y="COURSE_COUNT"
--         )

--     with col2:
--         st.markdown("**Total learning hours by difficulty level**")
--         st.bar_chart(
--             diff_df,
--             x="DIFFICULTY_CODE",
--             y="TOTAL_TIME_HOURS"
--         )

-- ]
-- CODE BLOCK ENDS HERE


------------------------------------------------------------------------------------------------------
-- [VI](b) ANS_GOLD_STREAMLIT_SETUP
------------------------------------------------------------------------------------------------------


------------------------------------------------------------------
-- 0. SET CONTEXT
------------------------------------------------------------------
USE ROLE ROLE_TEAM_ANS;
USE WAREHOUSE ANIMAL_TASK_WH;
USE DATABASE DB_TEAM_ANS;
USE SCHEMA DB_TEAM_ANS.GOLD;

------------------------------------------------------------------
-- 1. CREATE INTERNAL NAMED STAGE IN GOLD
------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS ANS_STREAMLIT_STAGE;

-- Optional sanity checks
DESCRIBE STAGE ANS_STREAMLIT_STAGE;

-- From local machine, upload the python file, 'ans_gold_streamlit_app.py'

-- PYTHON CODE FOR STREAMLIT APP: `ans_gold_streamlit_app.py`
-- CODE BLOCK BEGINS BELOW
-- [

-- ##################################################################
-- ## ans_gold_streamlit_app.py
-- ##################################################################

-- import streamlit as st
-- from snowflake.snowpark.context import get_active_session

-- # Get active Snowflake session
-- session = get_active_session()

-- # -------------------------------
-- # 1. Load data from GOLD tables
-- # -------------------------------

-- lang_df = session.sql("""
--     SELECT
--         LANGUAGE_NAME,
--         COURSE_COUNT,
--         TOTAL_TIME_HOURS,
--         TOTAL_CHAPTERS,
--         TOTAL_EXERCISES,
--         TOTAL_VIDEOS
--     FROM GOLD.G_LANGUAGE_INSTRUCTIONAL_EFFORT
--     ORDER BY TOTAL_TIME_HOURS DESC
-- """).to_pandas()

-- track_df = session.sql("""
--     SELECT
--         TRACK_TITLE,
--         IS_CAREER_FLAG,
--         COURSE_COUNT,
--         TOTAL_TIME_HOURS,
--         TOTAL_CHAPTERS,
--         TOTAL_EXERCISES,
--         TOTAL_VIDEOS
--     FROM GOLD.G_TRACK_CONTENT_SUMMARY
--     ORDER BY TOTAL_TIME_HOURS DESC
-- """).to_pandas()

-- diff_df = session.sql("""
--     SELECT
--         DIFFICULTY_CODE,
--         COURSE_COUNT,
--         TOTAL_TIME_HOURS,
--         TOTAL_CHAPTERS,
--         TOTAL_EXERCISES,
--         TOTAL_VIDEOS
--     FROM GOLD.G_DIFFICULTY_CONTENT_SUMMARY
--     ORDER BY DIFFICULTY_CODE
-- """).to_pandas()

-- # -------------------------------
-- # 2. Streamlit layout
-- # -------------------------------

-- st.title("ANS – DataCamp Gold Layer Dashboard")

-- st.caption(
--     "Visualizations built from GOLD tables: "
--     "`G_LANGUAGE_INSTRUCTIONAL_EFFORT`, "
--     "`G_TRACK_CONTENT_SUMMARY`, "
--     "`G_DIFFICULTY_CONTENT_SUMMARY`."
-- )

-- tab_lang, tab_track, tab_diff = st.tabs(
--     ["1. By Programming Language", "2. By Track", "3. By Difficulty"]
-- )

-- # --- Tab 1: Language ---
-- with tab_lang:
--     st.subheader("Instructional Effort by Programming Language")

--     st.dataframe(lang_df)

--     st.markdown("**Total learning hours per programming language**")
--     st.bar_chart(lang_df, x="LANGUAGE_NAME", y="TOTAL_TIME_HOURS")

--     st.markdown("**Total chapters per programming language**")
--     st.bar_chart(lang_df, x="LANGUAGE_NAME", y="TOTAL_CHAPTERS")

-- # --- Tab 2: Track ---
-- with tab_track:
--     st.subheader("Content Summary by Track")

--     track_type = st.radio(
--         "Filter tracks by type:",
--         options=["All", "Career Tracks", "Skill Tracks"],
--         horizontal=True,
--     )

--     track_filtered = track_df.copy()
--     if track_type == "Career Tracks":
--         track_filtered = track_filtered[track_filtered["IS_CAREER_FLAG"] == 1]
--     elif track_type == "Skill Tracks":
--         track_filtered = track_filtered[track_filtered["IS_CAREER_FLAG"] == 0]

--     st.dataframe(track_filtered)

--     st.markdown("**Total learning hours per track**")
--     st.bar_chart(track_filtered, x="TRACK_TITLE", y="TOTAL_TIME_HOURS")

--     st.markdown("**Number of courses per track**")
--     st.bar_chart(track_filtered, x="TRACK_TITLE", y="COURSE_COUNT")

-- # --- Tab 3: Difficulty ---
-- with tab_diff:
--     st.subheader("Course Distribution and Content by Difficulty")

--     st.dataframe(diff_df)

--     col1, col2 = st.columns(2)

--     with col1:
--         st.markdown("**Course count by difficulty level**")
--         st.bar_chart(diff_df, x="DIFFICULTY_CODE", y="COURSE_COUNT")

--     with col2:
--         st.markdown("**Total learning hours by difficulty level**")
--         st.bar_chart(diff_df, x="DIFFICULTY_CODE", y="TOTAL_TIME_HOURS")

-- ]
-- CODE BLOCK ENDS HERE

-- After upload
LIST @ANS_STREAMLIT_STAGE;

------------------------------------------------------------------
-- 2. CREATE STREAMLIT APP OBJECT
------------------------------------------------------------------
CREATE STREAMLIT ANS_DATACAMP_GOLD_DASHBOARD
  ROOT_LOCATION = '@DB_TEAM_ANS.GOLD.ANS_STREAMLIT_STAGE'
  MAIN_FILE     = 'ans_gold_streamlit_app.py'
  QUERY_WAREHOUSE = ANIMAL_TASK_WH;

-- Optional sanity checks
SHOW STREAMLITS IN SCHEMA DB_TEAM_ANS.GOLD;

------------------------------------------------------------------
-- 3 OPTIONAL: GRANTS
------------------------------------------------------------------
GRANT READ ON STAGE DB_TEAM_ANS.GOLD.ANS_STREAMLIT_STAGE TO ROLE ROLE_TEAM_ANS;
GRANT USAGE ON STREAMLIT DB_TEAM_ANS.GOLD.ANS_DATACAMP_GOLD_DASHBOARD TO ROLE ROLE_TEAM_ANS;




------------------------------------------------------------------------------------------------------
-- [VII] ANS_BRONZE_INCREMENTAL_DATA_INGESTION
------------------------------------------------------------------------------------------------------


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




------------------------------------------------------------------------------------------------------
-- [VIII] ANS_SILVER_INCREMENRAL_DATA_UPDATE
------------------------------------------------------------------------------------------------------


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


    -- DIM_DIFFICULTY  (static small table – safe to recreate)
    TRUNCATE TABLE DIM_DIFFICULTY;

    INSERT INTO DIM_DIFFICULTY (
        difficulty_code, 
        difficulty_order
    )
    VALUES
        ('Beginner', 1),
        ('Intermediate', 2),
        ('Advanced', 3);


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




--------------------------------------------------------------------------------------------------------
-- [IX] ANS_AUDIT_LOG_DATA_INCREMENTAL
--------------------------------------------------------------------------------------------------------


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
    silver_fact_track_rows
FROM DB_TEAM_ANS.GOLD.GOLD_LOAD_AUDIT
ORDER BY audit_id;




-------------------------------------------------------------------------------------------------------
-- [X] ANS_AI_SQL
-------------------------------------------------------------------------------------------------------


------------------------------------------------------------------
-- 0. SET CONTEXT
------------------------------------------------------------------
USE ROLE ROLE_TEAM_ANS;
USE WAREHOUSE ANIMAL_TASK_WH;
USE DATABASE DB_TEAM_ANS;
USE SCHEMA DB_TEAM_ANS.BRONZE;

------------------------------------------------------------------
-- 0. AI_COMPLETE – Your Generative Workhorse
------------------------------------------------------------------
-- Function: AI_COMPLETE(prompt_text)
-- What it does: Takes a text prompt as input and uses an LLM to generate a text completion. This is the foundation for most generative tasks.
-- Business Use Cases:
--      Auto-generating personalized email campaigns.
--      Creating product descriptions from a list of features.
--      Drafting responses to customer feedback.
-- Example: Generating Marketing Taglines
------------------------------------------------------------------
ALTER TABLE DCAMP_COURSES_BRONZE
DROP COLUMN IF EXISTS ai_predicted_title;

-- Add a new column 'ai_predicted_title' to DCAMP_COURSES_BRONZE
ALTER TABLE DCAMP_COURSES_BRONZE
ADD COLUMN ai_predicted_title VARCHAR;

-- Optional check
SELECT ai_predicted_title FROM DCAMP_COURSES_BRONZE;

-- Update values of 'ai_predicted_title' using AI_COMPLETE
UPDATE DCAMP_COURSES_BRONZE t
SET ai_predicted_title = AI_COMPLETE(
    'claude-3-7-sonnet', 
    prompt('Get the most likely course on the official Datacamp website from the given description. Just print the name in the output, no other text: {0}', description)
);

-- Optional check after insertion of values in column
SELECT
    title,
    ai_predicted_title,
    description,
    short_description,
    instructors_names
FROM DCAMP_COURSES_BRONZE;

------------------------------------------------------------------
-- 1. AI_SIMILARITY – Finding Meaningful Connections
------------------------------------------------------------------
-- Function: AI_SIMILARITY(text1, text2)
-- What it does: Calculates a score representing the semantic similarity between two text inputs. A higher score means the texts are closer in meaning.
-- Business Use Cases:
--      De-duplicating records, such as finding duplicate support tickets.
--      Powering simple recommendation engines ("find products with similar descriptions").
--      Grouping similar survey responses.
-- Example: Finding Duplicate Issue Emails
------------------------------------------------------------------
ALTER TABLE DCAMP_COURSES_BRONZE
DROP COLUMN IF EXISTS ai_title_similarity;

-- Add a new column 'ai_title_similarity' to DCAMP_COURSES_BRONZE
ALTER TABLE DCAMP_COURSES_BRONZE
ADD COLUMN ai_title_similarity VARCHAR;

-- Optional check
SELECT ai_title_similarity FROM DCAMP_COURSES_BRONZE;

-- Update values of 'ai_title_similarity' using AI_COMPLETE
UPDATE DCAMP_COURSES_BRONZE t
SET ai_title_similarity = AI_SIMILARITY(title, ai_predicted_title);

-- Optional check after insertion of values in column
SELECT
    title,
    ai_predicted_title,
    ai_title_similarity,
    description,
    short_description,
    instructors_names
FROM DCAMP_COURSES_BRONZE
ORDER BY ai_title_similarity DESC;




-------------------------------------------------------------------------------------------------------
-- [XI] ANS_CORTEX_SEARCH
-------------------------------------------------------------------------------------------------------


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




---------------------------------------------------------------------------------------------------------
-- [XII] ANS_CORTEX_ANALYST
---------------------------------------------------------------------------------------------------------


------------------------------------------------------------------
-- 0. SET CONTEXT
------------------------------------------------------------------
USE ROLE ROLE_TEAM_ANS;
USE WAREHOUSE ANIMAL_TASK_WH;
USE DATABASE DB_TEAM_ANS;
USE SCHEMA DB_TEAM_ANS.SILVER;

------------------------------------------------------------------
-- 1. CREATE AN INTERNAL NAMED STAGE IN SILVER
------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS ANS_SEMANTIC_MODELS;

-- Optional sanity checks
DESCRIBE STAGE ANS_SEMANTIC_MODELS;

-- From local machine, upload the YAML file, 'ans_silver_semantic_model.yaml'

-- YAML CODE FOR SEMANTIC MODEL: `ans_silver_semantic_model.yaml`
-- CODE BLOCK BEGINS BELOW
-- [

-- ##################################################################
-- ## ans_silver_semantic_model.yaml
-- ##################################################################

-- # --------------------------------------------------------------------
-- # Semantic Model for DB_TEAM_ANS.SILVER
-- # --------------------------------------------------------------------
-- name: ans_datacamp_silver_model
-- description: >
--   Semantic model on the DB_TEAM_ANS.SILVER star schema for DataCamp
--   courses & tracks. Exposes Silver fact tables and conformed dimensions
--   so Cortex Analyst can answer natural-language questions.

-- # --------------------------------------------------------------------
-- # LOGICAL TABLES (each maps to a physical Silver table)
-- # --------------------------------------------------------------------
-- tables:

--   # =====================
--   # DIMENSIONS
--   # =====================
--   - name: dim_date
--     description: Date dimension for snapshots and last_updated_on.
--     base_table:
--       database: DB_TEAM_ANS
--       schema: SILVER
--       table: DIM_DATE
--     primary_key:
--       columns:
--         - date_sk

--     # Logical columns on DIM_DATE
--     dimensions:
--       - name: date_sk
--         expr: date_sk
--         data_type: NUMBER
--         unique: true

--       - name: day_of_month
--         expr: day_of_month
--         data_type: NUMBER

--       - name: day_of_week
--         expr: day_of_week
--         data_type: NUMBER

--       - name: day_name
--         expr: day_name
--         data_type: STRING

--       - name: is_weekend
--         expr: is_weekend
--         data_type: BOOLEAN

--     time_dimensions:
--       - name: calendar_date
--         synonyms: ["date", "snapshot date"]
--         expr: date_value
--         data_type: DATE
--         unique: true

--       - name: year
--         expr: year
--         data_type: NUMBER

--       - name: quarter
--         expr: quarter
--         data_type: NUMBER

--       - name: month
--         expr: month
--         data_type: NUMBER

--       - name: month_name
--         expr: month_name
--         data_type: STRING

--       - name: week_of_year
--         expr: week_of_year
--         data_type: NUMBER

--   - name: dim_programming_language
--     description: Lookup dimension for programming languages used in courses and tracks.
--     base_table:
--       database: DB_TEAM_ANS
--       schema: SILVER
--       table: DIM_PROGRAMMING_LANGUAGE
--     primary_key:
--       columns:
--         - language_sk
--     dimensions:
--       - name: language_sk
--         expr: language_sk
--         data_type: NUMBER
--         unique: true
--       - name: language_code
--         expr: language_code
--         data_type: STRING
--       - name: language_name
--         synonyms: ["programming language", "language"]
--         expr: language_name
--         data_type: STRING

--   - name: dim_difficulty
--     description: Normalized difficulty level lookup across courses and tracks.
--     base_table:
--       database: DB_TEAM_ANS
--       schema: SILVER
--       table: DIM_DIFFICULTY
--     primary_key:
--       columns:
--         - difficulty_sk
--     dimensions:
--       - name: difficulty_sk
--         expr: difficulty_sk
--         data_type: NUMBER
--         unique: true
--       - name: difficulty_code
--         synonyms: ["difficulty", "difficulty label"]
--         expr: difficulty_code
--         data_type: STRING
--       - name: difficulty_order
--         expr: difficulty_order
--         data_type: NUMBER

--   - name: dim_content_area
--     description: Content area dimension (e.g. SQL, Python, Machine Learning).
--     base_table:
--       database: DB_TEAM_ANS
--       schema: SILVER
--       table: DIM_CONTENT_AREA
--     primary_key:
--       columns:
--         - content_area_sk
--     dimensions:
--       - name: content_area_sk
--         expr: content_area_sk
--         data_type: NUMBER
--         unique: true
--       - name: content_area_name
--         synonyms: ["content area", "subject area", "topic area"]
--         expr: content_area_name
--         data_type: STRING

--   - name: dim_topic
--     description: Topic lookup dimension (from DCAMP_TOPIC_MAPPING).
--     base_table:
--       database: DB_TEAM_ANS
--       schema: SILVER
--       table: DIM_TOPIC
--     primary_key:
--       columns:
--         - topic_sk
--     dimensions:
--       - name: topic_sk
--         expr: topic_sk
--         data_type: NUMBER
--         unique: true
--       - name: topic_id
--         expr: topic_id
--         data_type: STRING
--       - name: topic_name
--         expr: topic_name
--         data_type: STRING

--   - name: dim_technology
--     description: Technology lookup dimension (from DCAMP_TECHNOLOGY_MAPPING).
--     base_table:
--       database: DB_TEAM_ANS
--       schema: SILVER
--       table: DIM_TECHNOLOGY
--     primary_key:
--       columns:
--         - technology_sk
--     dimensions:
--       - name: technology_sk
--         expr: technology_sk
--         data_type: NUMBER
--         unique: true
--       - name: technology_id
--         expr: technology_id
--         data_type: STRING
--       - name: technology_name
--         expr: technology_name
--         data_type: STRING

--   - name: dim_instructor
--     description: One row per unique instructor across courses and tracks.
--     base_table:
--       database: DB_TEAM_ANS
--       schema: SILVER
--       table: DIM_INSTRUCTOR
--     primary_key:
--       columns:
--         - instructor_sk
--     dimensions:
--       - name: instructor_sk
--         expr: instructor_sk
--         data_type: NUMBER
--         unique: true
--       - name: instructor_name
--         synonyms: ["instructor"]
--         expr: instructor_name
--         data_type: STRING

--   - name: dim_collaborator
--     description: One row per unique collaborator from course-level data.
--     base_table:
--       database: DB_TEAM_ANS
--       schema: SILVER
--       table: DIM_COLLABORATOR
--     primary_key:
--       columns:
--         - collaborator_sk
--     dimensions:
--       - name: collaborator_sk
--         expr: collaborator_sk
--         data_type: NUMBER
--         unique: true
--       - name: collaborator_name
--         synonyms: ["collaborator"]
--         expr: collaborator_name
--         data_type: STRING

--   - name: dim_track
--     description: Track-level descriptive attributes, separate from track facts.
--     base_table:
--       database: DB_TEAM_ANS
--       schema: SILVER
--       table: DIM_TRACK
--     primary_key:
--       columns:
--         - track_sk
--     dimensions:
--       - name: track_sk
--         expr: track_sk
--         data_type: NUMBER
--         unique: true
--       - name: track_id
--         expr: track_id
--         data_type: STRING
--       - name: track_title
--         synonyms: ["track name"]
--         expr: track_title
--         data_type: STRING
--       - name: is_career_flag
--         synonyms: ["is career track", "career track flag"]
--         expr: is_career_flag
--         data_type: BOOLEAN
--       - name: programming_language_sk
--         expr: programming_language_sk
--         data_type: NUMBER
--       - name: predominant_difficulty_sk
--         expr: predominant_difficulty_sk
--         data_type: NUMBER
--       # raw lineage-style attributes
--       - name: raw_course_difficulty_levels
--         expr: raw_course_difficulty_levels
--         data_type: STRING
--       - name: raw_course_titles
--         expr: raw_course_titles
--         data_type: STRING
--       - name: raw_instructors
--         expr: raw_instructors
--         data_type: STRING
--       - name: raw_programming_languages
--         expr: raw_programming_languages
--         data_type: STRING

--   - name: dim_course
--     description: Central course dimension with descriptive attributes and FKs.
--     base_table:
--       database: DB_TEAM_ANS
--       schema: SILVER
--       table: DIM_COURSE
--     primary_key:
--       columns:
--         - course_sk
--     dimensions:
--       - name: course_sk
--         expr: course_sk
--         data_type: NUMBER
--         unique: true
--       - name: course_id
--         expr: course_id
--         data_type: STRING

--       # core descriptive attributes
--       - name: title
--         synonyms: ["course title", "course name"]
--         expr: title
--         data_type: STRING
--       - name: short_description
--         expr: short_description
--         data_type: STRING
--       - name: description
--         expr: description
--         data_type: STRING
--       - name: xp
--         expr: xp
--         data_type: NUMBER
--       - name: time_needed_hours
--         synonyms: ["time needed hours", "duration hours"]
--         expr: time_needed_hours
--         data_type: NUMBER

--       # foreign key attributes to other dims
--       - name: programming_language_sk
--         expr: programming_language_sk
--         data_type: NUMBER
--       - name: difficulty_sk
--         expr: difficulty_sk
--         data_type: NUMBER
--       - name: topic_sk
--         expr: topic_sk
--         data_type: NUMBER
--       - name: technology_sk
--         expr: technology_sk
--         data_type: NUMBER
--       - name: content_area_sk
--         expr: content_area_sk
--         data_type: NUMBER
--       - name: last_updated_date_sk
--         expr: last_updated_date_sk
--         data_type: NUMBER

--       # URLs and media
--       - name: course_url
--         expr: course_url
--         data_type: STRING
--       - name: image_url
--         expr: image_url
--         data_type: STRING

--       # raw list-style lineage attributes
--       - name: raw_instructors_names
--         expr: raw_instructors_names
--         data_type: STRING
--       - name: raw_collaborators_names
--         expr: raw_collaborators_names
--         data_type: STRING
--       - name: raw_tracks_titles
--         expr: raw_tracks_titles
--         data_type: STRING
--       - name: raw_prerequisites_titles
--         expr: raw_prerequisites_titles
--         data_type: STRING

--   # =====================
--   # FACT TABLES
--   # =====================
--   - name: fact_course_snapshots
--     description: Course-level snapshot fact table (one row per course, date).
--     base_table:
--       database: DB_TEAM_ANS
--       schema: SILVER
--       table: FACT_COURSE_SNAPSHOT_SILVER
--     primary_key:
--       columns:
--         - course_snapshot_sk
--     dimensions:
--       - name: course_snapshot_sk
--         expr: course_snapshot_sk
--         data_type: NUMBER
--         unique: true
--       - name: course_sk
--         expr: course_sk
--         data_type: NUMBER
--       - name: snapshot_date_sk
--         expr: snapshot_date_sk
--         data_type: NUMBER
--       - name: load_ts
--         expr: load_ts
--         data_type: TIMESTAMP_NTZ
--     facts:
--       - name: nb_of_subscriptions
--         synonyms: ["enrollments", "subscriptions"]
--         expr: nb_of_subscriptions
--         data_type: NUMBER
--       - name: num_chapters
--         expr: num_chapters
--         data_type: NUMBER
--       - name: num_exercises
--         expr: num_exercises
--         data_type: NUMBER
--       - name: num_videos
--         expr: num_videos
--         data_type: NUMBER
--       - name: datasets_count
--         expr: datasets_count
--         data_type: NUMBER
--     metrics:
--       - name: total_subscriptions
--         expr: SUM(nb_of_subscriptions)
--       - name: total_course_chapters
--         expr: SUM(num_chapters)
--       - name: total_course_exercises
--         expr: SUM(num_exercises)
--       - name: total_course_videos
--         expr: SUM(num_videos)
--       - name: total_course_datasets
--         expr: SUM(datasets_count)

--   - name: fact_track_snapshots
--     description: Track-level fact table with aggregated metrics per track, date.
--     base_table:
--       database: DB_TEAM_ANS
--       schema: SILVER
--       table: FACT_TRACK_SUMMARY_SILVER
--     primary_key:
--       columns:
--         - track_summary_sk
--     dimensions:
--       - name: track_summary_sk
--         expr: track_summary_sk
--         data_type: NUMBER
--         unique: true
--       - name: track_sk
--         expr: track_sk
--         data_type: NUMBER
--       - name: snapshot_date_sk
--         expr: snapshot_date_sk
--         data_type: NUMBER
--       - name: is_career_flag
--         expr: is_career_flag
--         data_type: BOOLEAN
--       - name: predominant_difficulty_sk
--         expr: predominant_difficulty_sk
--         data_type: NUMBER
--       - name: load_ts
--         expr: load_ts
--         data_type: TIMESTAMP_NTZ
--     facts:
--       - name: course_count
--         expr: course_count
--         data_type: NUMBER
--       - name: total_chapters
--         expr: total_chapters
--         data_type: NUMBER
--       - name: total_exercises
--         expr: total_exercises
--         data_type: NUMBER
--       - name: total_videos
--         expr: total_videos
--         data_type: NUMBER
--       - name: total_xp
--         expr: total_xp
--         data_type: NUMBER
--       - name: avg_xp_per_course
--         expr: avg_xp_per_course
--         data_type: NUMBER
--       - name: avg_time_hours
--         expr: avg_time_hours
--         data_type: NUMBER
--       - name: total_duration_hours
--         expr: total_duration_hours
--         data_type: NUMBER
--       - name: datasets_count
--         expr: datasets_count
--         data_type: NUMBER
--       - name: participant_count
--         expr: participant_count
--         data_type: NUMBER
--     metrics:
--       - name: total_track_courses
--         expr: SUM(course_count)
--       - name: total_track_participants
--         expr: SUM(participant_count)
--       - name: total_track_xp
--         expr: SUM(total_xp)
--       - name: avg_track_xp_per_course
--         expr: AVG(avg_xp_per_course)

--   # =====================
--   # BRIDGE TABLES
--   # =====================
--   - name: bridge_course_instructor
--     description: Bridge table linking courses to instructors (and roles).
--     base_table:
--       database: DB_TEAM_ANS
--       schema: SILVER
--       table: BRIDGE_COURSE_INSTRUCTOR
--     primary_key:
--       columns:
--         - course_sk
--         - instructor_sk
--         - role
--     dimensions:
--       - name: course_sk
--         expr: course_sk
--         data_type: NUMBER
--       - name: instructor_sk
--         expr: instructor_sk
--         data_type: NUMBER
--       - name: role
--         expr: role
--         data_type: STRING
--       - name: instructor_order
--         expr: instructor_order
--         data_type: NUMBER

--   - name: bridge_course_track
--     description: Bridge table linking courses to tracks.
--     base_table:
--       database: DB_TEAM_ANS
--       schema: SILVER
--       table: BRIDGE_COURSE_TRACK
--     primary_key:
--       columns:
--         - course_sk
--         - track_sk
--     dimensions:
--       - name: course_sk
--         expr: course_sk
--         data_type: NUMBER
--       - name: track_sk
--         expr: track_sk
--         data_type: NUMBER

--   - name: bridge_course_prerequisite
--     description: Self-referencing bridge between dependent and prerequisite courses.
--     base_table:
--       database: DB_TEAM_ANS
--       schema: SILVER
--       table: BRIDGE_COURSE_PREREQUISITE
--     primary_key:
--       columns:
--         - course_sk
--         - prerequisite_course_sk
--     dimensions:
--       - name: course_sk
--         expr: course_sk
--         data_type: NUMBER
--       - name: prerequisite_course_sk
--         expr: prerequisite_course_sk
--         data_type: NUMBER

--   - name: bridge_course_collaborator
--     description: Bridge table linking courses to collaborators.
--     base_table:
--       database: DB_TEAM_ANS
--       schema: SILVER
--       table: BRIDGE_COURSE_COLLABORATOR
--     primary_key:
--       columns:
--         - course_sk
--         - collaborator_sk
--     dimensions:
--       - name: course_sk
--         expr: course_sk
--         data_type: NUMBER
--       - name: collaborator_sk
--         expr: collaborator_sk
--         data_type: NUMBER

-- # --------------------------------------------------------------------
-- # MODEL-LEVEL RELATIONSHIPS (joins between logical tables)
-- # --------------------------------------------------------------------
-- relationships:

--   # =========================
--   # FACTS → CORE DIMENSIONS
--   # =========================
--   - name: fact_course_to_dim_course
--     left_table: fact_course_snapshots        # FACT_COURSE_SNAPSHOT_SILVER
--     right_table: dim_course                  # DIM_COURSE
--     relationship_columns:
--       - left_column: course_sk
--         right_column: course_sk
--     join_type: left_outer
--     relationship_type: many_to_one

--   - name: fact_course_to_dim_date
--     left_table: fact_course_snapshots        # FACT_COURSE_SNAPSHOT_SILVER
--     right_table: dim_date                    # DIM_DATE
--     relationship_columns:
--       - left_column: snapshot_date_sk
--         right_column: date_sk
--     join_type: left_outer
--     relationship_type: many_to_one

--   - name: fact_track_to_dim_track
--     left_table: fact_track_snapshots         # FACT_TRACK_SUMMARY_SILVER
--     right_table: dim_track                   # DIM_TRACK
--     relationship_columns:
--       - left_column: track_sk
--         right_column: track_sk
--     join_type: left_outer
--     relationship_type: many_to_one

--   - name: fact_track_to_dim_date
--     left_table: fact_track_snapshots
--     right_table: dim_date
--     relationship_columns:
--       - left_column: snapshot_date_sk
--         right_column: date_sk
--     join_type: left_outer
--     relationship_type: many_to_one

--   - name: fact_track_to_dim_difficulty
--     left_table: fact_track_snapshots
--     right_table: dim_difficulty
--     relationship_columns:
--       - left_column: predominant_difficulty_sk
--         right_column: difficulty_sk
--     join_type: left_outer
--     relationship_type: many_to_one

--   # ==================================
--   # DIM_TRACK / DIM_COURSE → LOOKUPS
--   # ==================================
--   - name: dim_track_to_dim_programming_language
--     left_table: dim_track
--     right_table: dim_programming_language
--     relationship_columns:
--       - left_column: programming_language_sk
--         right_column: language_sk
--     join_type: left_outer
--     relationship_type: many_to_one

--   - name: dim_track_to_dim_difficulty
--     left_table: dim_track
--     right_table: dim_difficulty
--     relationship_columns:
--       - left_column: predominant_difficulty_sk
--         right_column: difficulty_sk
--     join_type: left_outer
--     relationship_type: many_to_one

--   - name: dim_course_to_dim_programming_language
--     left_table: dim_course
--     right_table: dim_programming_language
--     relationship_columns:
--       - left_column: programming_language_sk
--         right_column: language_sk
--     join_type: left_outer
--     relationship_type: many_to_one

--   - name: dim_course_to_dim_difficulty
--     left_table: dim_course
--     right_table: dim_difficulty
--     relationship_columns:
--       - left_column: difficulty_sk
--         right_column: difficulty_sk
--     join_type: left_outer
--     relationship_type: many_to_one

--   - name: dim_course_to_dim_topic
--     left_table: dim_course
--     right_table: dim_topic
--     relationship_columns:
--       - left_column: topic_sk
--         right_column: topic_sk
--     join_type: left_outer
--     relationship_type: many_to_one

--   - name: dim_course_to_dim_technology
--     left_table: dim_course
--     right_table: dim_technology
--     relationship_columns:
--       - left_column: technology_sk
--         right_column: technology_sk
--     join_type: left_outer
--     relationship_type: many_to_one

--   - name: dim_course_to_dim_content_area
--     left_table: dim_course
--     right_table: dim_content_area
--     relationship_columns:
--       - left_column: content_area_sk
--         right_column: content_area_sk
--     join_type: left_outer
--     relationship_type: many_to_one

--   - name: dim_course_to_dim_date_last_updated
--     left_table: dim_course
--     right_table: dim_date
--     relationship_columns:
--       - left_column: last_updated_date_sk
--         right_column: date_sk
--     join_type: left_outer
--     relationship_type: many_to_one

--   # ==========================
--   # BRIDGES → DIMENSIONS
--   # ==========================
--   - name: bridge_course_instructor_to_dim_course
--     left_table: bridge_course_instructor
--     right_table: dim_course
--     relationship_columns:
--       - left_column: course_sk
--         right_column: course_sk
--     join_type: inner
--     relationship_type: many_to_one

--   - name: bridge_course_instructor_to_dim_instructor
--     left_table: bridge_course_instructor
--     right_table: dim_instructor
--     relationship_columns:
--       - left_column: instructor_sk
--         right_column: instructor_sk
--     join_type: inner
--     relationship_type: many_to_one

--   - name: bridge_course_track_to_dim_course
--     left_table: bridge_course_track
--     right_table: dim_course
--     relationship_columns:
--       - left_column: course_sk
--         right_column: course_sk
--     join_type: inner
--     relationship_type: many_to_one

--   - name: bridge_course_track_to_dim_track
--     left_table: bridge_course_track
--     right_table: dim_track
--     relationship_columns:
--       - left_column: track_sk
--         right_column: track_sk
--     join_type: inner
--     relationship_type: many_to_one

--   - name: bridge_course_prerequisite_to_dim_course
--     left_table: bridge_course_prerequisite
--     right_table: dim_course
--     relationship_columns:
--       - left_column: course_sk
--         right_column: course_sk
--     join_type: inner
--     relationship_type: many_to_one

--   - name: bridge_prerequisite_course_to_dim_course
--     left_table: bridge_course_prerequisite
--     right_table: dim_course
--     relationship_columns:
--       - left_column: prerequisite_course_sk
--         right_column: course_sk
--     join_type: inner
--     relationship_type: many_to_one

--   - name: bridge_course_collaborator_to_dim_course
--     left_table: bridge_course_collaborator
--     right_table: dim_course
--     relationship_columns:
--       - left_column: course_sk
--         right_column: course_sk
--     join_type: inner
--     relationship_type: many_to_one

--   - name: bridge_course_collaborator_to_dim_collaborator
--     left_table: bridge_course_collaborator
--     right_table: dim_collaborator
--     relationship_columns:
--       - left_column: collaborator_sk
--         right_column: collaborator_sk
--     join_type: inner
--     relationship_type: many_to_one

-- # --------------------------------------------------------------------
-- # END OF SCRIPT
-- # --------------------------------------------------------------------

-- ]
-- CODE BLOCK ENDS HERE

-- After upload
LIST @ANS_SEMANTIC_MODELS;;




---------------------------------------------------------------------------------------------------------
-- [XIII] ANS_ALL_TABLES_QUERYING
---------------------------------------------------------------------------------------------------------


------------------------------------------------------------------
-- 0. BRONZE SCHEMA
------------------------------------------------------------------
-- View DCAMP_COURSES_BRONZE table
SELECT * FROM DB_TEAM_ANS.BRONZE.DCAMP_COURSES_BRONZE;

-- View DCAMP_ALL_TRACKS_BRONZE table
SELECT * FROM DB_TEAM_ANS.BRONZE.DCAMP_ALL_TRACKS_BRONZE;

-- View DCAMP_TOPIC_MAPPING_BRONZE table
SELECT * FROM DB_TEAM_ANS.BRONZE.DCAMP_TOPIC_MAPPING_BRONZE;

-- View DCAMP_TECHNOLOGY_MAPPING_BRONZE table
SELECT * FROM DB_TEAM_ANS.BRONZE.DCAMP_TECHNOLOGY_MAPPING_BRONZE;

------------------------------------------------------------------
-- 1. SILVER SCHEMA
------------------------------------------------------------------

------------------------------------------------------------------
-- 1.1 DIMENSION TABLES
------------------------------------------------------------------
-- View DIM_DATE table
SELECT * FROM DB_TEAM_ANS.SILVER.DIM_DATE;

-- View DIM_PROGRAMMING_LANGUAGE table
SELECT * FROM DB_TEAM_ANS.SILVER.DIM_PROGRAMMING_LANGUAGE;

-- View DIM_DIFFICULTY table
SELECT * FROM DB_TEAM_ANS.SILVER.DIM_DIFFICULTY;

-- View DIM_CONTENT_AREA table
SELECT * FROM DB_TEAM_ANS.SILVER.DIM_CONTENT_AREA;

-- View DIM_TOPIC table
SELECT * FROM DB_TEAM_ANS.SILVER.DIM_TOPIC;

-- View DIM_TECHNOLOGY table
SELECT * FROM DB_TEAM_ANS.SILVER.DIM_TECHNOLOGY;

-- View DIM_INSTRUCTOR table
SELECT * FROM DB_TEAM_ANS.SILVER.DIM_INSTRUCTOR;

-- View DIM_COLLABORATOR table
SELECT * FROM DB_TEAM_ANS.SILVER.DIM_COLLABORATOR;

-- View DIM_TRACK table
SELECT * FROM DB_TEAM_ANS.SILVER.DIM_TRACK;

-- View DIM_COURSE table
SELECT * FROM DB_TEAM_ANS.SILVER.DIM_COURSE;

------------------------------------------------------------------
-- 1.1 BRIDGE (SUB-DIMENSION) TABLES
------------------------------------------------------------------
-- View BRIDGE_COURSE_INSTRUCTOR table
SELECT * FROM DB_TEAM_ANS.SILVER.BRIDGE_COURSE_INSTRUCTOR;

-- View BRIDGE_COURSE_TRACK table
SELECT * FROM DB_TEAM_ANS.SILVER.BRIDGE_COURSE_TRACK;

-- View BRIDGE_COURSE_PREREQUISITE table
SELECT * FROM DB_TEAM_ANS.SILVER.BRIDGE_COURSE_PREREQUISITE;

-- View BRIDGE_COURSE_COLLABORATOR table
SELECT * FROM DB_TEAM_ANS.SILVER.BRIDGE_COURSE_COLLABORATOR;

------------------------------------------------------------------
-- 1.1 FACT TABLES
------------------------------------------------------------------
-- View FACT_COURSE_SNAPSHOT_SILVER table
SELECT * FROM DB_TEAM_ANS.SILVER.FACT_COURSE_SNAPSHOT_SILVER;

-- View FACT_TRACK_SUMMARY_SILVER table
SELECT * FROM DB_TEAM_ANS.SILVER.FACT_TRACK_SUMMARY_SILVER;

------------------------------------------------------------------
-- 2. GOLD SCHEMA
------------------------------------------------------------------
-- View G_LANGUAGE_INSTRUCTIONAL_EFFORT table
SELECT * FROM DB_TEAM_ANS.GOLD.G_LANGUAGE_INSTRUCTIONAL_EFFORT;

-- View G_TRACK_CONTENT_SUMMARY table
SELECT * FROM DB_TEAM_ANS.GOLD.G_TRACK_CONTENT_SUMMARY;

-- View G_DIFFICULTY_CONTENT_SUMMARY table
SELECT * FROM DB_TEAM_ANS.GOLD.G_DIFFICULTY_CONTENT_SUMMARY;

------------------------------------------------------------------
-- 3. AUDIT LOG TABLES
------------------------------------------------------------------
-- View SILVER_LOAD_AUDIT table
SELECT * FROM DB_TEAM_ANS.SILVER.SILVER_LOAD_AUDIT;

-- View GOLD_LOAD_AUDIT table
SELECT * FROM DB_TEAM_ANS.GOLD.GOLD_LOAD_AUDIT;




---------------------------------------------------------------------------------------------------------
-- END OF SCRIPT
---------------------------------------------------------------------------------------------------------
