-- TERM PROJECT
-- TEAM NAME: ANS
-- MEMBER NAMES: Abhiroop Kumar (ak56448), Nikhil Kumar (nk25627), Simoni K Dalal (skd939)
-- DATABASE: DB_TEAM_ANS
-- ROLE: ROLE_TEAM_ANS
-- DUE: Dec 8 at 11:59pm

-- FILE NAME: ANS_CORTEX_ANALYST.sql

-- SOLUTION

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

-- After upload
LIST @ANS_SEMANTIC_MODELS;;

------------------------------------------------------------------
-- END OF SCRIPT
------------------------------------------------------------------