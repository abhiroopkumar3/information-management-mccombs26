# **ANS Data Engineering Pipeline – README**

### Snowflake Medallion Architecture (Bronze → Silver → Gold)

Team **ANS**  
*Abhiroop Kumar (ak56448), Nikhil Kumar (nk25627), Simoni K Dalal (skd939)*  
Database: **DB_TEAM_ANS**  
Role: **ROLE_TEAM_ANS**  
Due: **Dec 8 at 11:59 PM**  

---

# 1. Project Overview

This repository implements a complete **Snowflake Medallion Architecture** for **DataCamp course & track metadata**, including:

* Raw CSV ingestion into **Bronze**
* Fully normalized, time-variant **star/snowflake** modeling in **Silver**
* Business-ready **Gold** data marts using **Dynamic Tables**
* **Incremental** updates with time-based snapshots
* **Audit logging** for before/after row-count validation
* **AI SQL (Snowflake Cortex)** on long text fields
* **Cortex Search** on Bronze text columns
* **Cortex Analyst** on a Silver semantic model
* A native **Snowflake Streamlit dashboard** powered by Gold tables

Business questions addressed include:

* Which **programming languages** receive the most instructional effort?
* How much content exists per **career/skill track**?
* How is content distributed across **difficulty** levels?
* How does incremental data change these metrics over time?

---

# 2. Source Data & Kaggle Dataset

Source: **Kaggle – DataCamp Courses Metadata** (single primary CSV + structured derivatives).

## 2.1 Raw Kaggle Files (Initial Snapshot)

We ingest four base CSVs:

* `courses.csv` – 116 courses, 23 columns
* `all_tracks.csv` – 115 tracks, 18 columns
* `topic_mapping.csv` – 17 topics (id → name mapping)
* `technology_mapping.csv` – 37 technologies (id → name mapping)

These files have:

* Rich **text** columns (descriptions, titles)
* **Categorical** columns (programming_language, difficulty_level, content_area)
* **Numeric** metrics (xp, time_needed_in_hours, counts)
* **Multi-valued** text (instructors_names, collaborators_names, tracks_titles, prerequisites_titles)

## 2.2 Incremental Files

To demonstrate incremental ingestion, we created four additional CSVs:

* `courses_incremental.csv` – 4 new AI/MLOps courses (new course IDs)
* `all_tracks_incremental.csv` – 2 new career tracks using those courses
* `topic_mapping_incremental.csv` – 3 new topics (MLOps, Generative AI, LLM Applications)
* `technology_mapping_incremental.csv` – 3 new technologies (Databricks, Vertex AI, dbt)

These are **insert-only** (no overlapping IDs), which cleanly exercises our incremental logic.

---

# 3. Medallion Architecture – High-Level

We follow a **Bronze → Silver → Gold** approach:

* **Bronze** – Raw, schema-on-read, minimal transformation
* **Silver** – Cleaned, conformed, normalized dimensional model
* **Gold** – Curated, denormalized, **business-grain** dynamic tables for BI & dashboards

Scripts are numbered to reflect the pipeline order (01–13).

---

# 4. Bronze Layer – Raw Ingestion

**Scripts:**

* `01_-_ANS_BRONZE_IMPORT.sql`
* `07_-_ANS_BRONZE_INCREMENTAL_DATA_INGESTION.sql`

## 4.1 Structures

Bronze contains four raw tables (all columns as `VARCHAR`):

* `BRONZE.DCAMP_COURSES_BRONZE`
* `BRONZE.DCAMP_ALL_TRACKS_BRONZE`
* `BRONZE.DCAMP_TOPIC_MAPPING_BRONZE`
* `BRONZE.DCAMP_TECHNOLOGY_MAPPING_BRONZE`

Key components:

* Stages:

  * `@DCAMP_BRONZE_STAGE` – initial full-load stage
  * `@DCAMP_BRONZE_STAGE_INCREMENT` – incremental stage
* File format:

  * `DCAMP_BRONZE_CSV_FF` with
    `FIELD_DELIMITER = ','`
    `FIELD_OPTIONALLY_ENCLOSED_BY = '"'`
    `TRIM_SPACE = TRUE`
    `ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE`

Initial row counts (after 01):

* Courses: **116**
* Tracks: **115**
* Topics: **17**
* Technologies: **37**

## 4.2 Incremental Ingestion (Snowpipe)

`07_-_ANS_BRONZE_INCREMENTAL_DATA_INGESTION.sql` defines 4 **AUTO_INGEST** Snowpipes:

* `COURSES_INGEST_PIPE` → `DCAMP_COURSES_BRONZE`
* `ALL_TRACKS_INGEST_PIPE` → `DCAMP_ALL_TRACKS_BRONZE`
* `TOPIC_MAPPING_INGEST_PIPE` → `DCAMP_TOPIC_MAPPING_BRONZE`
* `TECHNOLOGY_MAPPING_INGEST_PIPE` → `DCAMP_TECHNOLOGY_MAPPING_BRONZE`

Each Snowpipe uses filename patterns (e.g., `courses_.*[.]csv`) to load new incremental files as they arrive.

After loading the incremental CSVs:

* Courses: **120** (116 + 4)
* Tracks: **117** (115 + 2)
* Topics: 20 (17 + 3)
* Technologies: 40 (37 + 3)

---

# 5. Silver Layer – Dimensional Warehouse

**Scripts:**

* `02_-_ANS_SILVER_SETUP.sql`
* `03_-_ANS_SILVER_STATIC_DATA.sql`
* `08_-_ANS_SILVER_INCREMENRAL_DATA_UPDATE.sql`

Silver implements a **Kimball-style star/snowflake** model with **time-variant** facts and bridge tables for multi-valued attributes.

## 5.1 Dimensions

Created in `02_-_ANS_SILVER_SETUP.sql` and initially populated in `03_-_ANS_SILVER_STATIC_DATA.sql`:

* `DIM_DATE` – calendar attributes (year, month, weekday, weekend flag)
* `DIM_PROGRAMMING_LANGUAGE` – language code/name (R, Python, SQL, etc.)
* `DIM_DIFFICULTY` – fixed 3-level scale: Beginner, Intermediate, Advanced
* `DIM_CONTENT_AREA` – coarse business areas (Data Science, Data Engineering, AI, etc.)
* `DIM_TOPIC` – topic_id → topic_name mapping
* `DIM_TECHNOLOGY` – technology_id → technology_name mapping
* `DIM_INSTRUCTOR` – instructor names
* `DIM_COLLABORATOR` – collaborator organizations
* `DIM_TRACK` – track_id, title, `is_career_flag`, predominant difficulty, raw track-level strings
* `DIM_COURSE` – central course dimension with:

  * Surrogate `COURSE_SK`
  * `COURSE_ID`, `TITLE`, `SHORT_DESCRIPTION`, `DESCRIPTION`
  * `XP`, `TIME_NEEDED_HOURS`
  * FKs: `PROGRAMMING_LANGUAGE_SK`, `DIFFICULTY_SK`, `TOPIC_SK`, `TECHNOLOGY_SK`, `CONTENT_AREA_SK`, `LAST_UPDATED_DATE_SK`
  * Raw multi-valued text preserved (`RAW_INSTRUCTORS_NAMES`, `RAW_COLLABORATORS_NAMES`, `RAW_TRACKS_TITLES`, `RAW_PREREQUISITES_TITLES`)

## 5.2 Bridge Tables (Multi-Valued Attributes)

* `BRIDGE_COURSE_INSTRUCTOR`

  * Columns: `COURSE_SK`, `INSTRUCTOR_SK`, `ROLE`, `INSTRUCTOR_ORDER`
* `BRIDGE_COURSE_COLLABORATOR`

  * `COURSE_SK`, `COLLABORATOR_SK`
* `BRIDGE_COURSE_TRACK`

  * `COURSE_SK`, `TRACK_SK`
* `BRIDGE_COURSE_PREREQUISITE`

  * `COURSE_SK`, `PREREQUISITE_COURSE_SK`

These are populated from multi-valued strings in Bronze using `SPLIT` / `FLATTEN`, eliminating repetition and enabling clean joins.

## 5.3 Fact Tables & Grain

### FACT 1 – `FACT_COURSE_SNAPSHOT_SILVER`

* Grain: **one row per course per snapshot date**
* Keys:

  * `COURSE_SK`
  * `SNAPSHOT_DATE_SK` (FK → `DIM_DATE`)
* Measures:

  * `NB_OF_SUBSCRIPTIONS`
  * `NUM_CHAPTERS`, `NUM_EXERCISES`, `NUM_VIDEOS`
  * `DATASETS_COUNT`
  * `LOAD_TS`

### FACT 2 – `FACT_TRACK_SUMMARY_SILVER`

* Grain: **one row per track per snapshot date**
* Keys:

  * `TRACK_SK`
  * `SNAPSHOT_DATE_SK`
* Measures:

  * `COURSE_COUNT`
  * `TOTAL_CHAPTERS`, `TOTAL_EXERCISES`, `TOTAL_VIDEOS`
  * `TOTAL_XP`
  * `AVG_XP_PER_COURSE`
  * `AVG_TIME_HOURS`
  * `TOTAL_DURATION_HOURS`
  * `DATASETS_COUNT`
  * `PARTICIPANT_COUNT`
  * `IS_CAREER_FLAG`
  * `PREDOMINANT_DIFFICULTY_SK`
  * `LOAD_TS`

Initial static load (03 script) produces:

* 1 snapshot date (single `SNAPSHOT_DATE_SK`)
* `FACT_COURSE_SNAPSHOT_SILVER`: 116 rows
* `FACT_TRACK_SUMMARY_SILVER`: 115 rows

## 5.4 Incremental Refresh (Snapshots)

`08_-_ANS_SILVER_INCREMENRAL_DATA_UPDATE.sql` defines:

* `LOAD_SILVER_DIM_TABLES()`

  * Inserts **only new** dimension members (topic, technology, track, course, etc.)
  * Rebuilds small static `DIM_DIFFICULTY`
* `LOAD_SILVER_BRIDGE_TABLES()`

  * Inserts **only missing** course–instructor / course–collaborator / course–track / course–prerequisite rows
* `LOAD_SILVER_FACT_TABLES()`

  * Inserts **new fact rows** for the **latest snapshot date** only where that `(COURSE_SK, SNAPSHOT_DATE_SK)` or `(TRACK_SK, SNAPSHOT_DATE_SK)` does not already exist
* `REFRESH_SILVER_TABLES()`

  * Calls the above three in order and returns a summary message

After loading the incremental CSVs and running `CALL REFRESH_SILVER_TABLES();`:

* `DIM_COURSE` grows from 116 → 120 rows
* `DIM_TRACK` grows from 115 → 117 rows
* `FACT_COURSE_SNAPSHOT_SILVER`: 116 → **236** rows (2 snapshots)
* `FACT_TRACK_SUMMARY_SILVER`: 115 → **232** rows (2 snapshots)

Silver facts now maintain a **time-series snapshot history**.

---

# 6. Gold Layer – Business Aggregations (Dynamic Tables)

**Script:** `04_-_ANS_GOLD_SETUP.sql`

Gold uses **Dynamic Tables** (with `TARGET_LAG = '1 MINUTE'`) to stay synchronized with Silver. Each Gold table implements one key business question.

## 6.1 G_LANGUAGE_INSTRUCTIONAL_EFFORT

**Business Question:**

> Which programming languages require the most instructional effort?

* Grain: **1 row per programming language**
* Sources:

  * `FACT_COURSE_SNAPSHOT_SILVER`
  * `DIM_COURSE`
  * `DIM_PROGRAMMING_LANGUAGE`
* Measures:

  * `COURSE_COUNT`
  * `TOTAL_TIME_HOURS`
  * `TOTAL_CHAPTERS`
  * `TOTAL_EXERCISES`
  * `TOTAL_VIDEOS`
  * `TOTAL_NB_OF_SUBSCRIPTIONS`
  * Per-course averages:

    * `AVG_TIME_HOURS_PER_COURSE`
    * `AVG_CHAPTERS_PER_COURSE`
    * `AVG_EXERCISES_PER_COURSE`
    * `AVG_VIDEOS_PER_COURSE`
* Filters to **latest snapshot date** via a `latest_snapshot` CTE.

## 6.2 G_TRACK_CONTENT_SUMMARY

**Business Question:**

> How much learning content does each **career/skill track** provide?

* Grain: **1 row per track**
* Sources:

  * `FACT_COURSE_SNAPSHOT_SILVER`
  * `BRIDGE_COURSE_TRACK`
  * `DIM_TRACK`
  * `DIM_COURSE`
* Measures:

  * `COURSE_COUNT`
  * `TOTAL_TIME_HOURS`
  * `TOTAL_CHAPTERS`
  * `TOTAL_EXERCISES`
  * `TOTAL_VIDEOS`
  * `TOTAL_XP`
  * `AVG_TIME_HOURS_PER_COURSE`
  * `AVG_CHAPTERS_PER_COURSE`
  * `AVG_EXERCISES_PER_COURSE`
  * `AVG_VIDEOS_PER_COURSE`
  * `TOTAL_NB_OF_SUBSCRIPTIONS`
  * `TOTAL_DATASETS_COUNT`
* Attributes:

  * `IS_CAREER_FLAG` (Career vs Skill)
  * `SNAPSHOT_DATE_SK`

## 6.3 G_DIFFICULTY_CONTENT_SUMMARY

**Business Question:**

> How is course content distributed across difficulty levels?

* Grain: **1 row per difficulty level**
* Sources:

  * `FACT_COURSE_SNAPSHOT_SILVER`
  * `DIM_COURSE`
  * `DIM_DIFFICULTY`
* Measures (mirroring language table):

  * `COURSE_COUNT`
  * `TOTAL_TIME_HOURS`, chapters, exercises, videos
  * `TOTAL_NB_OF_SUBSCRIPTIONS`
  * `TOTAL_DATASETS_COUNT`
  * Per-course averages
* Attributes:

  * `DIFFICULTY_CODE`
  * `DIFFICULTY_ORDER` for sorting

Gold tables remain **small & stable in row count** (e.g., 3 languages, ~54 tracks, 3 difficulties) but their metric values update whenever new snapshots are loaded.

---

# 7. AI & Cortex Features

## 7.1 AI SQL – Snowflake Cortex on Bronze

**Script:** `10_-_ANS_AI_SQL.sql`

On `BRONZE.DCAMP_COURSES_BRONZE` we add:

* `AI_PREDICTED_TITLE` – generated via:

  ```sql
  AI_COMPLETE(
    'claude-3-7-sonnet',
    prompt('Get the most likely course on the official Datacamp website from the given description. Just print the name in the output, no other text: {0}', description)
  )
  ```

* `AI_TITLE_SIMILARITY` – similarity between `TITLE` and `AI_PREDICTED_TITLE` using `AI_SIMILARITY(title, ai_predicted_title)`.

Export: `DCAMP_COURSES_BRONZE__AI_COLUMNS.csv` captures:

* `TITLE`, `AI_PREDICTED_TITLE`, `AI_TITLE_SIMILARITY`
* `DESCRIPTION`, `SHORT_DESCRIPTION`, `INSTRUCTORS_NAMES`

Use cases:

* Detect potential **duplicate or misaligned titles**
* Suggest **cleaned or canonical** titles
* Provide a basis for **course similarity** and recommendations

## 7.2 Cortex Search – Semantic Search on Bronze

**Script:** `11_-_ANS_CORTEX_SEARCH.sql`

Defines two **Cortex Search Services**:

1. `ANS_DATACAMP_COURSES_CORTEX_SEARCH`

   * Source: `BRONZE.DCAMP_COURSES_BRONZE`
   * Search column: `TITLE`
   * Attributes: `ID`, `DESCRIPTION`, `SHORT_DESCRIPTION`, `PROGRAMMING_LANGUAGE`, `DIFFICULTY_LEVEL`, `CONTENT_AREA`, `LINK`, `IMAGE_URL`
   * Model: `'snowflake-arctic-embed-m-v1.5'`

2. `ANS_DATACAMP_TRACKS_CORTEX_SEARCH`

   * Source: `BRONZE.DCAMP_ALL_TRACKS_BRONZE`
   * Search column: `TRACK_TITLE`
   * Attributes: `TRACK_ID`, `IS_CAREER`, `PROGRAMMING_LANGUAGE`, `COURSE_COUNT`, `COURSE_TITLES`, `PREDOMINANT_DIFFICULTY`, `TOTAL_DURATION_HOURS`, etc.

Both services:

* Use `TARGET_LAG = '1 MINUTE'`
* Are initialized `ON_CREATE`
* Provide sample `SEARCH_PREVIEW` queries for phrases like `"python beginner"` or `"machine learning"`

## 7.3 Cortex Analyst – Natural Language over Silver

**Script:** `12_-_ANS_CORTEX_ANALYST.sql`
**Semantic Model:** `ans_silver_semantic_model.yaml` (uploaded to `@ANS_SEMANTIC_MODELS` stage)

* Semantic model defines:

  * All Silver dims, facts, and bridges under `tables:`
  * Relationships (`PK`/`FK`) connecting `FACT_COURSE_SNAPSHOT_SILVER`, `FACT_TRACK_SUMMARY_SILVER`, and their dimensions

Cortex Analyst uses this semantic model to:

* Inspect the Silver schema
* Translate **natural language questions** into SQL (joins, group-bys)
* Execute queries and return results, enabling **self-service BI** without writing SQL

---

# 8. Gold Visualization – Snowflake Streamlit Dashboard

**Files:**

* Dev notebook: `06_a_-_ANS_GOLD_VISUALIZATION.ipynb.txt`
* Deployed app script: `ans_gold_streamlit_app.py`
* App setup: `06_b_-_ANS_GOLD_STREAMLIT_SETUP.sql`

## 8.1 App Setup

`06_b_-_ANS_GOLD_STREAMLIT_SETUP.sql`:

* Creates stage: `GOLD.ANS_STREAMLIT_STAGE`
* Uploads `ans_gold_streamlit_app.py` into the stage
* Creates Snowflake Streamlit app: `ANS_DATACAMP_GOLD_DASHBOARD`
* Grants `USAGE` on the app and `READ` on the stage to `ROLE_TEAM_ANS`

## 8.2 Dashboard Layout

`ans_gold_streamlit_app.py`:

* Gets an active Snowpark session

* Queries:

  * `G_LANGUAGE_INSTRUCTIONAL_EFFORT` → `lang_df`
  * `G_TRACK_CONTENT_SUMMARY` → `track_df`
  * `G_DIFFICULTY_CONTENT_SUMMARY` → `diff_df`

* UI:

  * Title: **“ANS – DataCamp Gold Layer Dashboard”**
  * Three tabs:

    1. **By Programming Language**

       * Table of `lang_df`
       * Bar charts: instructional hours & chapters by language

    2. **By Track**

       * Radio filter: `All`, `Career Tracks`, `Skill Tracks` using `IS_CAREER_FLAG`
       * Table + bar charts: total hours & course counts by track

    3. **By Difficulty**

       * Table of `diff_df`
       * Side-by-side charts: course count & total hours by difficulty level

This dashboard is the main **presentation artifact** for the Gold layer.

---

# 9. Incremental Pipeline – End-to-End Runbook

1. **Set context**

   ```sql
   USE ROLE ROLE_TEAM_ANS;
   USE WAREHOUSE ANIMAL_TASK_WH;
   USE DATABASE DB_TEAM_ANS;
   ```

2. **Initial setup (one-time)**

   * Run `01_-_ANS_BRONZE_IMPORT.sql`
   * Run `02_-_ANS_SILVER_SETUP.sql`
   * Run `03_-_ANS_SILVER_STATIC_DATA.sql`
   * Run `04_-_ANS_GOLD_SETUP.sql`
   * Run `05_-_ANS_AUDIT_LOG_DATA.sql`
   * Run `06_b_-_ANS_GOLD_STREAMLIT_SETUP.sql`

3. **Initial audit (optional)**

   * `CALL LOG_SILVER_LOAD_METRICS();`
   * `CALL LOG_GOLD_LOAD_METRICS();`

4. **Configure incremental ingestion**

   * Run `07_-_ANS_BRONZE_INCREMENTAL_DATA_INGESTION.sql` (Snowpipes)

5. **Drop incremental CSVs into the incremental stage**

   * `courses_incremental.csv`, `all_tracks_incremental.csv`, `topic_mapping_incremental.csv`, `technology_mapping_incremental.csv`

6. **Refresh Silver after new data lands**

   ```sql
   CALL REFRESH_SILVER_TABLES();
   ```

7. **Gold dynamic tables auto-refresh**

   * `TARGET_LAG = '1 MINUTE'` keeps Gold aligned with current Silver facts

8. **Post-refresh audit**

   ```sql
   CALL LOG_SILVER_LOAD_METRICS();
   CALL LOG_GOLD_LOAD_METRICS();

   SELECT * FROM SILVER_LOAD_AUDIT ORDER BY AUDIT_ID DESC;
   SELECT * FROM GOLD_LOAD_AUDIT   ORDER BY AUDIT_ID DESC;
   ```

9. **Run Streamlit app**

   * Open `ANS_DATACAMP_GOLD_DASHBOARD` in Snowflake UI

---

# 10. Validation & Audit Evidence

**Audit tables:**

* `SILVER_LOAD_AUDIT`

  * Logs:

    * Bronze row counts (courses, tracks)
    * Silver row counts (`DIM_COURSE`, `DIM_TRACK`)
    * Fact row counts for both Silver fact tables
* `GOLD_LOAD_AUDIT`

  * Logs:

    * Row counts for each Gold table
    * Silver fact row counts at the time of refresh

Example evolution:

* **Initial load**:

  * Bronze courses/tracks: 116 / 115
  * Silver dims: 116 / 115
  * Silver facts: 116 / 115
  * Gold: 3 language rows, 54 track rows, 3 difficulty rows

* **After incremental**:

  * Bronze courses/tracks: 120 / 117
  * Silver dims: 120 / 117
  * Silver facts: 236 / 232 (two snapshots)
  * Gold: still 3 / 54 / 3 rows, but **aggregates updated**

Exports in `Exported Tables/` capture snapshots of:

* Bronze tables (`01 - BRONZE`)
* Silver dims, bridges, facts (`02 - SILVER`)
* Gold tables (`03 - GOLD`)
* Audit logs (`04 - AUDIT LOG`)
* AI-enriched columns (`05 - AI SQL`)

These CSVs provide **verifiable evidence** for each layer and each load step.

---

# 11. Repository / File Structure

```bash
project/
├── 00_-_ANS_TERM_PROJECT_MERGED_SCRIPT.sql
├── 01_-_ANS_BRONZE_IMPORT.sql
├── 02_-_ANS_SILVER_SETUP.sql
├── 03_-_ANS_SILVER_STATIC_DATA.sql
├── 04_-_ANS_GOLD_SETUP.sql
├── 05_-_ANS_AUDIT_LOG_DATA.sql
├── 06_a_-_ANS_GOLD_VISUALIZATION.ipynb.txt
├── 06_b_-_ANS_GOLD_STREAMLIT_SETUP.sql
├── 07_-_ANS_BRONZE_INCREMENTAL_DATA_INGESTION.sql
├── 08_-_ANS_SILVER_INCREMENRAL_DATA_UPDATE.sql
├── 09_-_ANS_AUDIT_LOG_DATA_INCREMENTAL.sql
├── 10_-_ANS_AI_SQL.sql
├── 11_-_ANS_CORTEX_SEARCH.sql
├── 12_-_ANS_CORTEX_ANALYST.sql
├── 13_-_ALL_TABLES_QUERYING.sql
├── ans_gold_streamlit_app.py
├── ans_silver_semantic_model.yaml
 Datasets/
│   ├── courses.csv
│   ├── all_tracks.csv
│   ├── topic_mapping.csv
│   ├── technology_mapping.csv
 Datasets (Incremental)/
│   ├── courses_incremental.csv
│   ├── all_tracks_incremental.csv
│   ├── topic_mapping_incremental.csv
│   └── technology_mapping_incremental.csv
├── Exported Tables/
│   ├── 01 - BRONZE/*.csv
│   ├── 02 - SILVER/*.csv
│   ├── 03 - GOLD/*.csv
│   ├── 04 - AUDIT LOG/*.csv
│   └── 05 - AI SQL/DCAMP_COURSES_BRONZE__AI_COLUMNS.csv
└── README.md
```

---

# 12. Optional Future Enhancements

* Additional Gold marts (e.g., **By Topic**, **By Technology**, **By Instructor**)
* Materialized views for specific heavy queries
* Data quality checks & alerts (e.g., minimum chapters/time thresholds)
* More advanced AI/Cortex usage:

  * Vector-based recommendations for “next best course”
  * Explainable difficulty classification
* Extended semantic model (more business metrics layered on Cortex Analyst)
* Automated scheduling (outside class environment, in real production)

---

# 13. License & Ownership

Project created for:  
**The University of Texas at Austin – MSBA**  
Course: **MIS 381N – Information Management** (Term Project)  

Authors: **Team ANS**

* Abhiroop Kumar (ak56448)
* Nikhil Kumar (nk25627)
* Simoni K Dalal (skd939)

--- 

# END OF FILE
