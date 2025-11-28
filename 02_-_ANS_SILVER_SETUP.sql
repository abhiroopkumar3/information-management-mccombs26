-- TERM PROJECT
-- TEAM NAME: ANS
-- MEMBER NAMES: Abhiroop Kumar (ak56448), Nikhil Kumar (nk25627), Simoni K Dalal (skd939)
-- DATABASE: DB_TEAM_ANS
-- ROLE: ROLE_TEAM_ANS
-- DUE: Dec 8 at 11:59pm

-- FILE NAME: ANS_SILVER_SETUP.sql

-- SOLUTION

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

------------------------------------------------------------------
-- END OF SCRIPT
------------------------------------------------------------------
