# **ANS Data Engineering Pipeline – README**

### *Snowflake Medallion Architecture (Bronze → Silver → Gold)*

Team: **ANS**
Members: *Abhiroop Kumar (ak56448), Nikhil Kumar (nk25627), Simoni K Dalal (skd939)*

---

# **1. Project Overview**

This repository implements a full **Bronze → Silver → Gold** Medallion Architecture for DataCamp course & track metadata.
The Gold layer answers three explicitly defined **business questions**, with dashboards built in **Snowflake Native Streamlit**.

The goal is to create a fully scalable, analytics-ready environment supporting:

* dimensional modeling
* time-series snapshots
* course/track lineage analysis
* difficulty segmentation
* instructor/collaborator mapping
* BI dashboards & KPIs

This README describes ingestion, transformation, business questions, Gold schema tables, and dashboard visualization logic.

---

# **2. Architecture Summary**

## **2.1 Bronze Layer – Raw Ingestion**

Bronze stores raw CSV data exactly as ingested:

* `DCAMP_COURSES_BRONZE`
* `DCAMP_ALL_TRACKS_BRONZE`
* `DCAMP_TOPIC_MAPPING_BRONZE`
* `DCAMP_TECHNOLOGY_MAPPING_BRONZE`

(DDL + COPY INTO scripts in `01_-_ANS_BRONZE_IMPORT.sql`)

---

## **2.2 Silver Layer – Standardized, Cleaned, Normalized**

Silver contains:

### **A. Dimension Tables**

* `DIM_COURSE`
* `DIM_TRACK`
* `DIM_INSTRUCTOR`
* `DIM_COLLABORATOR`
* `DIM_TOPIC`
* `DIM_TECHNOLOGY`
* `DIM_CONTENT_AREA`
* `DIM_PROGRAMMING_LANGUAGE`
* `DIM_DIFFICULTY`
* `DIM_DATE`

### **B. Bridge Tables** (for many-to-many relationships)

* `BRIDGE_COURSE_INSTRUCTOR`
* `BRIDGE_COURSE_COLLABORATOR`
* `BRIDGE_COURSE_TRACK`
* `BRIDGE_COURSE_PREREQUISITE`

### **C. Fact Tables**

* `FACT_COURSE_SNAPSHOT_SILVER`
* `FACT_TRACK_SUMMARY_SILVER`

These capture course-level and track-level KPIs for every snapshot date.

All Silver-layer logic is implemented in:
📄 `02_-_ANS_SILVER_SETUP.sql`, `03_-_ANS_SILVER_STATIC_DATA.sql`

---

# **3. Business Questions Addressed by the Gold Layer**

The **Group Project Guidelines** require a Gold layer that answers high-level business questions and a Streamlit dashboard with three visualizations.

Below are the **three business questions** selected and implemented.

---

## **3.1 Business Question 1 – Programming Language Instructional Effort**

> *Across all DataCamp courses, which programming languages require the most instructional effort?*
> Metrics include: instructional hours, chapters, exercises, and videos.

Gold Table:
✔ `G_LANGUAGE_INSTRUCTIONAL_EFFORT`
Grain: one row per programming language.

---

## **3.2 Business Question 2 – Track-Level Content Summary**

> *For each career or skill track, what is the total learning content (time, chapters, exercises, videos), and how many courses does the track include?*

Gold Table:
✔ `G_TRACK_CONTENT_SUMMARY`
Grain: one row per track.

---

## **3.3 Business Question 3 – Difficulty Distribution & Curriculum Depth**

> *How is the course catalog distributed across difficulty levels (Beginner / Intermediate / Advanced), and which difficulty level contributes the most total learning content?*

Gold Table:
✔ `G_DIFFICULTY_CONTENT_SUMMARY`
Grain: one row per difficulty level.

---

# **4. Gold Layer – Business Aggregation Tables**

The Gold layer aggregates information from **FACT_COURSE_SNAPSHOT_SILVER** and **Silver dimensions** into business-ready tables.

These tables are created inside `04_-_ANS_GOLD_VISUALIZATION.sql`.

---

## **4.1 G_LANGUAGE_INSTRUCTIONAL_EFFORT**

Purpose: Answer Business Question 1.

```sql
CREATE OR REPLACE TABLE GOLD.G_LANGUAGE_INSTRUCTIONAL_EFFORT AS
WITH latest AS (
    SELECT MAX(snapshot_date_sk) AS snapshot_date_sk
    FROM SILVER.FACT_COURSE_SNAPSHOT_SILVER
)
SELECT
    pl.language_sk,
    pl.language_name,
    COUNT(DISTINCT c.course_sk)         AS course_count,
    SUM(c.time_needed_hours)            AS total_time_hours,
    SUM(f.num_chapters)                 AS total_chapters,
    SUM(f.num_exercises)                AS total_exercises,
    SUM(f.num_videos)                   AS total_videos,
    AVG(c.time_needed_hours)            AS avg_time_hours_per_course,
    AVG(f.num_chapters)                 AS avg_chapters_per_course,
    AVG(f.num_exercises)                AS avg_exercises_per_course,
    AVG(f.num_videos)                   AS avg_videos_per_course
FROM SILVER.FACT_COURSE_SNAPSHOT_SILVER f
JOIN SILVER.DIM_COURSE c
    ON f.course_sk = c.course_sk
JOIN SILVER.DIM_PROGRAMMING_LANGUAGE pl
    ON c.programming_language_sk = pl.language_sk
JOIN latest ls
    ON f.snapshot_date_sk = ls.snapshot_date_sk
GROUP BY pl.language_sk, pl.language_name, ls.snapshot_date_sk;
```

---

## **4.2 G_TRACK_CONTENT_SUMMARY**

Purpose: Answer Business Question 2.

```sql
CREATE OR REPLACE TABLE GOLD.G_TRACK_CONTENT_SUMMARY AS
WITH latest AS (
    SELECT MAX(snapshot_date_sk) AS snapshot_date_sk
    FROM SILVER.FACT_COURSE_SNAPSHOT_SILVER
)
SELECT
    t.track_sk,
    t.track_title,
    t.is_career_flag,
    COUNT(DISTINCT c.course_sk)         AS course_count,
    SUM(c.time_needed_hours)            AS total_time_hours,
    SUM(f.num_chapters)                 AS total_chapters,
    SUM(f.num_exercises)                AS total_exercises,
    SUM(f.num_videos)                   AS total_videos
FROM SILVER.FACT_COURSE_SNAPSHOT_SILVER f
JOIN SILVER.DIM_COURSE c
    ON f.course_sk = c.course_sk
JOIN SILVER.BRIDGE_COURSE_TRACK b
    ON c.course_sk = b.course_sk
JOIN SILVER.DIM_TRACK t
    ON b.track_sk = t.track_sk
JOIN latest ls
    ON f.snapshot_date_sk = ls.snapshot_date_sk
GROUP BY t.track_sk, t.track_title, t.is_career_flag, ls.snapshot_date_sk;
```

---

## **4.3 G_DIFFICULTY_CONTENT_SUMMARY**

Purpose: Answer Business Question 3.

```sql
CREATE OR REPLACE TABLE GOLD.G_DIFFICULTY_CONTENT_SUMMARY AS
WITH latest AS (
    SELECT MAX(snapshot_date_sk) AS snapshot_date_sk
    FROM SILVER.FACT_COURSE_SNAPSHOT_SILVER
)
SELECT
    d.difficulty_sk,
    d.difficulty_code,
    d.difficulty_order,
    COUNT(DISTINCT c.course_sk)          AS course_count,
    SUM(c.time_needed_hours)             AS total_time_hours,
    SUM(f.num_chapters)                  AS total_chapters,
    SUM(f.num_exercises)                 AS total_exercises,
    SUM(f.num_videos)                    AS total_videos
FROM SILVER.FACT_COURSE_SNAPSHOT_SILVER f
JOIN SILVER.DIM_COURSE c
    ON f.course_sk = c.course_sk
JOIN SILVER.DIM_DIFFICULTY d
    ON c.difficulty_sk = d.difficulty_sk
JOIN latest ls
    ON f.snapshot_date_sk = ls.snapshot_date_sk
GROUP BY d.difficulty_sk, d.difficulty_code, d.difficulty_order, ls.snapshot_date_sk;
```

---

# **5. Streamlit Dashboard – Three Required Visualizations**

The project requires a **Snowflake Streamlit dashboard** with three visualizations derived exclusively from the Gold layer.

Dashboard is implemented inside the Snowflake Notebook using:

* Three SQL cells (one for each Gold table)
* One Streamlit Python cell
* Charts based on `.to_pandas()` outputs

---

## **5.1 SQL Queries Feeding Streamlit**

### **Language-level view**

```sql
SELECT * FROM GOLD.G_LANGUAGE_INSTRUCTIONAL_EFFORT;
```

### **Track-level view**

```sql
SELECT * FROM GOLD.G_TRACK_CONTENT_SUMMARY;
```

### **Difficulty-level view**

```sql
SELECT * FROM GOLD.G_DIFFICULTY_CONTENT_SUMMARY;
```

---

## **5.2 Streamlit Visualization Code**

```python
import streamlit as st

lang_df = lang_effort_sql.to_pandas()
track_df = track_content_sql.to_pandas()
diff_df = difficulty_content_sql.to_pandas()

st.title("ANS – DataCamp Gold Layer Dashboard")

tab_lang, tab_track, tab_diff = st.tabs([
    "1. By Programming Language",
    "2. By Track",
    "3. By Difficulty Level"
])

with tab_lang:
    st.subheader("Instructional Effort by Programming Language")
    st.bar_chart(lang_df, x="LANGUAGE_NAME", y="TOTAL_TIME_HOURS")

with tab_track:
    st.subheader("Total Learning Hours by Track")
    st.bar_chart(track_df, x="TRACK_TITLE", y="TOTAL_TIME_HOURS")

with tab_diff:
    st.subheader("Course Distribution by Difficulty")
    st.bar_chart(diff_df, x="DIFFICULTY_CODE", y="COURSE_COUNT")
```

These three visualizations complete the project’s dashboard requirement.

---

# **6. Repository Structure**

```
project/
│
├── 01_-_ANS_BRONZE_IMPORT.sql
├── 02_-_ANS_SILVER_SETUP.sql
├── 03_-_ANS_SILVER_STATIC_DATA.sql
├── 04_-_ANS_GOLD_VISUALIZATION.sql     # Gold tables + SQL for dashboard
├── Lab 9 - Data Visualization Queries for Streamlit.sql
├── sample_streamlit_app.py
│
└── README.md   (this file)
```

---

# **7. How to Run the Full Pipeline**

1. Run Bronze import
2. Run Silver DDL
3. Run Silver DML
4. Validate
5. Create Gold tables
6. Run Streamlit cells inside Snowflake Notebook

---

# **8. Final Notes**

Future improvements:

* Materialized views for performance
* KPI dashboard
* Instructor-level insights
* Track difficulty progression analysis

---

# **9. License & Ownership**

This project is built for **University of Texas MSBA – Information Management Term Project**.
Authors: Team **ANS**.