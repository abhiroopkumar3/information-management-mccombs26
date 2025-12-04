-- TERM PROJECT
-- TEAM NAME: ANS
-- MEMBER NAMES: Abhiroop Kumar (ak56448), Nikhil Kumar (nk25627), Simoni K Dalal (skd939)
-- DATABASE: DB_TEAM_ANS
-- ROLE: ROLE_TEAM_ANS
-- DUE: Dec 8 at 11:59pm

-- FILE NAME: ANS_AI_SQL.sql

-- SOLUTION

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
FROM DB_TEAM_ANS.BRONZE.DCAMP_COURSES_BRONZE;

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
FROM DB_TEAM_ANS.BRONZE.DCAMP_COURSES_BRONZE
ORDER BY ai_title_similarity DESC;

------------------------------------------------------------------
-- END OF SCRIPT
------------------------------------------------------------------