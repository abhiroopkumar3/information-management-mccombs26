-- TERM PROJECT
-- TEAM NAME: ANS
-- MEMBER NAMES: Abhiroop Kumar (ak56448), Nikhil Kumar (nk25627), Simoni K Dalal (skd939)
-- DATABASE: DB_TEAM_ANS
-- ROLE: ROLE_TEAM_ANS
-- DUE: Dec 8 at 11:59pm

-- FILE NAME: ALL_TABLES_QUERYING.sql

-- SOLUTION

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

------------------------------------------------------------------
-- END OF SCRIPT
------------------------------------------------------------------
