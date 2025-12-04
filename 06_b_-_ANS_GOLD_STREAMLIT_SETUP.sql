-- TERM PROJECT
-- TEAM NAME: ANS
-- MEMBER NAMES: Abhiroop Kumar (ak56448), Nikhil Kumar (nk25627), Simoni K Dalal (skd939)
-- DATABASE: DB_TEAM_ANS
-- ROLE: ROLE_TEAM_ANS
-- DUE: Dec 8 at 11:59pm

-- FILE NAME: ANS_GOLD_STREAMLIT_SETUP.sql

-- SOLUTION

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

------------------------------------------------------------------
-- END OF SCRIPT
------------------------------------------------------------------