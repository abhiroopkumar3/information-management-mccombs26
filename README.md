# **ANS Data Engineering Pipeline – README**

### Snowflake Medallion Architecture (Bronze → Silver → Gold)

Team **ANS**
*Abhiroop Kumar (ak56448), Nikhil Kumar (nk25627), Simoni K Dalal (skd939)*
Database: **DB_TEAM_ANS**
Role: **ROLE_TEAM_ANS**
Due: **Dec 8 at 11:59 PM**

---

# **1. Project Overview**

This repository implements a complete **Snowflake Medallion Architecture** supporting:

* Raw ingestion (Bronze)
* Cleansed & normalized warehouse modeling (Silver)
* Business-ready aggregated analytics (Gold)
* Automated incremental updates
* Audit logging for pipeline validation
* AI-augmented SQL using Snowflake Cortex (AI_COMPLETE, AI_SIMILARITY)
* A fully interactive **Streamlit dashboard** powered by Gold dynamic tables

The project builds an analytics-ready data warehouse for DataCamp courses and tracks, enabling:

* content depth analysis
* learning pathway evaluation
* difficulty segmentation
* instructor & collaborator analytics
* time-series snapshots
* self-service BI

---

# **2. Architecture Summary**

## **2.1 Bronze Layer – Raw Ingestion**

Bronze stores raw CSVs from DataCamp with no transformations.

Implemented in:

* **01_-_ANS_BRONZE_IMPORT.sql** 
* **07_-_ANS_BRONZE_INCREMENTAL_DATA_INGESTION.sql** (Snowpipe) 

Key components:

* Named stages (`DCAMP_BRONZE_STAGE`, `DCAMP_BRONZE_STAGE_INCREMENT`)
* Standard CSV file format
* Four Bronze tables:

  * `DCAMP_COURSES_BRONZE`
  * `DCAMP_ALL_TRACKS_BRONZE`
  * `DCAMP_TOPIC_MAPPING_BRONZE`
  * `DCAMP_TECHNOLOGY_MAPPING_BRONZE`
* Snowpipes for incremental ingestion of new CSV files

---

## **2.2 Silver Layer – Standardized & Normalized**

Silver contains:

### **Dimension Tables**

Defined in **02_-_ANS_SILVER_SETUP.sql** 
Populated in **03_-_ANS_SILVER_STATIC_DATA.sql** 

Dimensions include:

* `DIM_DATE`
* `DIM_PROGRAMMING_LANGUAGE`
* `DIM_DIFFICULTY`
* `DIM_CONTENT_AREA`
* `DIM_TOPIC`
* `DIM_TECHNOLOGY`
* `DIM_INSTRUCTOR`
* `DIM_COLLABORATOR`
* `DIM_TRACK`
* `DIM_COURSE`

### **Bridge Tables**

* `BRIDGE_COURSE_INSTRUCTOR`
* `BRIDGE_COURSE_TRACK`
* `BRIDGE_COURSE_PREREQUISITE`
* `BRIDGE_COURSE_COLLABORATOR`

### **Fact Tables**

* `FACT_COURSE_SNAPSHOT_SILVER`
* `FACT_TRACK_SUMMARY_SILVER`

### **Incremental Refresh Procedures (Full Medallion Refresh)**

Implemented in **08_-_ANS_SILVER_INCREMENRAL_DATA_UPDATE.sql** 

Stored procedures:

* `LOAD_SILVER_DIM_TABLES()`
* `LOAD_SILVER_BRIDGE_TABLES()`
* `LOAD_SILVER_FACT_TABLES()`
* `REFRESH_SILVER_TABLES()` → runs all three in order

Purpose:

* Load new dimension members
* Add new bridge relationships
* Append new daily snapshot facts
* Avoid duplicates via LEFT JOIN logic

---

## **2.3 Gold Layer – Business Aggregations**

Gold is built using **Dynamic Tables**, ensuring the Gold layer is always up-to-date with Silver incremental refreshes.

Defined in:
**04_-_ANS_GOLD_SETUP.sql** 

Gold dynamic tables:

1. **G_LANGUAGE_INSTRUCTIONAL_EFFORT**
   *Answers*: Which programming languages require the most instructional effort?

2. **G_TRACK_CONTENT_SUMMARY**
   *Answers*: How much learning content does each career/skill track provide?

3. **G_DIFFICULTY_CONTENT_SUMMARY**
   *Answers*: How does content distribute across Beginner/Intermediate/Advanced levels?

Gold dynamic tables refresh every **1 minute** via `TARGET_LAG = '1 MINUTE'`.

---

# **3. Business Questions Answered**

## **BQ1 – Programming Language Instructional Effort**

*Which programming languages require the most instructional effort (hours, chapters, exercises, videos)?*

Source tables:

* `FACT_COURSE_SNAPSHOT_SILVER`
* `DIM_COURSE`
* `DIM_PROGRAMMING_LANGUAGE`

Result: `G_LANGUAGE_INSTRUCTIONAL_EFFORT`

---

## **BQ2 – Track-Level Summary**

*What total learning content (hours, chapters, exercises, videos) does each career/skill track contain?*

Source tables:

* `BRIDGE_COURSE_TRACK`
* `DIM_TRACK`
* `FACT_COURSE_SNAPSHOT_SILVER`

Result: `G_TRACK_CONTENT_SUMMARY`

---

## **BQ3 – Difficulty Distribution**

*How is course content distributed across difficulty levels?*

Source tables:

* `FACT_COURSE_SNAPSHOT_SILVER`
* `DIM_COURSE`
* `DIM_DIFFICULTY`

Result: `G_DIFFICULTY_CONTENT_SUMMARY`

---

# **4. Audit Logging (Silver & Gold)**

Implemented across two files:

* **05_-_ANS_AUDIT_LOG_DATA.sql** 
* **09_-_ANS_AUDIT_LOG_DATA_INCREMENTAL.sql** 

### Stored Procedures:

* `LOG_SILVER_LOAD_METRICS()` → Inserts row counts for bronze vs silver
* `LOG_GOLD_LOAD_METRICS()` → Inserts row counts for gold dynamic tables

Audit tables:

* `SILVER_LOAD_AUDIT`
* `GOLD_LOAD_AUDIT`

Purpose:

* Verify correctness of incremental loads
* Validate Gold matches Silver facts
* Provide row-count traceability

---

# **5. AI SQL Layer – Snowflake Cortex**

Implemented in **10_-_ANS_AI_SQL.sql** 

### Added Columns:

* `ai_predicted_title` → LLM-generated course title guess
* `ai_title_similarity` → Similarity score between actual and AI-generated title

### Functions Used:

* `AI_COMPLETE(model, prompt)`
* `AI_SIMILARITY(text1, text2)`

Applications:

* Metadata enrichment
* Duplicate detection
* Title cleaning
* Course similarity and recommendation prep

---

# **6. Gold Visualization SQL + Streamlit Dashboard**

All SQL + Streamlit code is in:
**06_-_ANS_GOLD_VISUALIZATION.txt** 

### Streams of Data

SQL cells:

* `lang_effort_sql`
* `track_content_sql`
* `difficulty_content_sql`

### Streamlit Notebook Components

* Tabs for three visualizations
* Filtering for Skill vs Career tracks
* Bar charts for:

  * total learning hours
  * course counts
  * chapters/exercises/videos breakdown

Dashboard includes:

1. **Programming language effort**
2. **Track summary (Career vs Skill toggle)**
3. **Difficulty-level distribution**

---

# **7. Incremental Pipeline (End-to-End)**

### Full Pipeline Order:

#### **A. Bronze**

1. Upload CSVs to internal stage
2. Run COPY INTO or let Snowpipe auto-ingest

#### **B. Silver**

3. `CALL REFRESH_SILVER_TABLES();`

   * Loads new dims
   * Updates bridge tables
   * Appends new fact snapshot rows

#### **C. Gold**

4. Dynamic tables auto-refresh every minute
5. Run Streamlit dashboard

#### **D. Audit**

6. `CALL LOG_SILVER_LOAD_METRICS();`
7. `CALL LOG_GOLD_LOAD_METRICS();`

---

# **8. Repository/File Structure**

```
project/
│
├── 01_-_ANS_BRONZE_IMPORT.sql
├── 02_-_ANS_SILVER_SETUP.sql
├── 03_-_ANS_SILVER_STATIC_DATA.sql
├── 04_-_ANS_GOLD_SETUP.sql
├── 05_-_ANS_AUDIT_LOG_DATA.sql
├── 06_-_ANS_GOLD_VISUALIZATION.txt
├── 07_-_ANS_BRONZE_INCREMENTAL_DATA_INGESTION.sql
├── 08_-_ANS_SILVER_INCREMENRAL_DATA_UPDATE.sql
├── 09_-_ANS_AUDIT_LOG_DATA_INCREMENTAL.sql
├── 10_-_ANS_AI_SQL.sql
└── README.md
```

---

# **9. Future Enhancements**

* Materialized views for real-time performance
* Semantic layer with metrics
* Instructor-level insights
* Course recommendation system using Cortex vector search
* Data quality rules & alerts
* More Gold tables (e.g., by Topic, Technology, Instructors)

---

# **10. License & Ownership**

Project created for:
**University of Texas – MSBA**
Course: Information Management Term Project
Authors: Team **ANS**