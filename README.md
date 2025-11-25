# **ANS Data Engineering Pipeline – README**

### *Snowflake Medallion Architecture (Bronze → Silver → Gold)*

Team: **ANS**
Members: *Abhiroop Kumar (ak56448), Nikhil Kumar (nk25627), Simoni K Dalal (skd939)*

---

# **1. Project Overview**

This repository implements a complete **Medallion Architecture (Bronze → Silver → Gold)** for DataCamp course & track metadata.
The goal is to build a scalable, normalized, query-efficient Snowflake analytics model suitable for:

* dimensional modeling
* BI dashboards
* time-series snapshots
* instructor/collaborator relationships
* prerequisite graph analysis
* track-course linkage exploration

The project includes full DDL, DML, and validation scripts.

---

# **2. Architecture Summary**

## **2.1 Medallion Layers**

### **Bronze Layer – Raw Ingestion**

Source CSVs are copied into 4 bronze tables:

* `DCAMP_COURSES_BRONZE`
* `DCAMP_ALL_TRACKS_BRONZE`
* `DCAMP_TOPIC_MAPPING_BRONZE`
* `DCAMP_TECHNOLOGY_MAPPING_BRONZE`
  *(Created via `01_-_ANS_BRONZE_IMPORT.sql` )*

These tables preserve the raw schema, multi-valued fields, and text formats.

---

### **Silver Layer – Standardized & Normalized**

Silver is split into:

#### **A. Dimension Tables**

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
  *(Created via `02_-_ANS_SILVER_SETUP.sql` )*

Each dimension uses a **surrogate key**, enforces proper data types, and ensures 1NF/2NF normalization.

#### **B. Bridge Tables (Many-to-Many Normalization)**

* `BRIDGE_COURSE_INSTRUCTOR`
* `BRIDGE_COURSE_COLLABORATOR`
* `BRIDGE_COURSE_TRACK`
* `BRIDGE_COURSE_PREREQUISITE`

Designed to correctly split:

* multi-instructor inputs
* collaborator lists
* course-to-track memberships
* prerequisite chains

#### **C. Fact Tables**

* `FACT_COURSE_SNAPSHOT_SILVER`
* `FACT_TRACK_SUMMARY_SILVER`

Each fact stores metrics + snapshot date surrogate key.

Data population is handled by
`03_-_ANS_SILVER_STATIC_DATA.sql`  
including:

* multi-value LATERAL FLATTEN
* type casting
* date spine population
* foreign key lookups
* deduplication logic

---

### **Gold Layer – To Be Added**

Gold will contain:

* curated business views
* BI star schema views
* dashboards and KPIs
* aggregated semantic layer
* ready-to-consume reporting tables

Template stub exists in:
`04_-_ANS_GOLD_VISUALIZATION.sql` 

---

# **3. Key Features of This ETL**

### ✔ Fully normalized Snowflake Dimensional Model

* All multi-valued fields (instructors, collaborators, tracks, prerequisites) are split into bridge tables.
* Surrogate keys for all DIMs ensure stable foreign key relationships.
* Bronze string columns are converted to NUMBER, BOOLEAN, DATE.

### ✔ Accurate Course/Track Lineage

* `BRIDGE_COURSE_TRACK` maps many-to-many relationships between courses & tracks.
* `BRIDGE_COURSE_INSTRUCTOR` captures course → instructor role relationships.
* `BRIDGE_COURSE_COLLABORATOR` isolates collaborators separately.
* `BRIDGE_COURSE_PREREQUISITE` resolves title-based references to SK-based mapping.

### ✔ Snapshot Facts

* Both fact tables use a **snapshot_date_sk** tied to `DIM_DATE`.
* Snapshot date = most recent last_updated date found in Bronze.

### ✔ End-to-End Validation Included

Using the queries in `03_-_ANS_SILVER_STATIC_DATA.sql`:

* Course counts match between bronze and fact
* Track counts match
* `0 orphan courses` in facts
* `0 orphan tracks` in facts

---

# **4. Repository Structure**

```
project/
│
├── 01_-_ANS_BRONZE_IMPORT.sql        # Bronze DDL + COPY INTO        :contentReference[oaicite:4]{index=4}
├── 02_-_ANS_SILVER_SETUP.sql         # Silver DDL layer              :contentReference[oaicite:5]{index=5}
├── 03_-_ANS_SILVER_STATIC_DATA.sql   # Silver DML/ETL                :contentReference[oaicite:6]{index=6}
├── 04_-_ANS_GOLD_VISUALIZATION.sql   # Gold placeholders             :contentReference[oaicite:7]{index=7}
├── 05_-_BASIC_TABLE_QUERYING.sql     # Query samples + checks        :contentReference[oaicite:8]{index=8}
│
└── README.md (this file)
```

---

# **5. Data Flow Diagram (High-Level)**

```
 BRONZE (Raw CSVs)
      │
      ▼
 SILVER DIMENSIONS ──────────────┐
 SILVER BRIDGES (M:N tables)     │──► STAR MODEL
 SILVER FACTS (Snapshot)         │
      │                          │
      ▼                          │
 GOLD (Business Views, KPIs, Dashboards)   ← (To Be Added)
```

---

# **6. How to Run the Pipeline (Quick Start)**

1. **Run Bronze Import**

   ```sql
   !execute 01_-_ANS_BRONZE_IMPORT.sql
   ```

2. **Run Silver Setup (DDL)**

   ```sql
   !execute 02_-_ANS_SILVER_SETUP.sql
   ```

3. **Run Silver ETL (DML)**

   ```sql
   !execute 03_-_ANS_SILVER_STATIC_DATA.sql
   ```

4. **Run validation queries**

   ```sql
   !execute 05_-_BASIC_TABLE_QUERYING.sql
   ```

5. Add Gold logic later in
   `04_-_ANS_GOLD_VISUALIZATION.sql`.

---

# **7. Next Steps (to fill in later)**

You can expand the README with:

### **Gold Layer Enhancements**

* KPI materialized views
* Course popularity trend view
* Track difficulty segmentation view
* Instructor activity dashboards

### **Performance Optimization**

* Clustering by snapshot_date_sk
* Result scan caching
* Micro-partition pruning examples

### **Analytics Examples**

* “What skills are most common across top tracks?”
* “Which courses have the highest time-to-XP ratio?”
* “Most frequent prerequisite chains.”

### **ERDs**

* Add Mermaid or dbdiagram.io diagrams for DIM/FACT/BRIDGE layers

---

# **8. Versioning & Change Log**

*(Leave placeholders for future updates)*

---

# **9. License & Ownership**

This project is built for **University of Texas MSBA – Data Engineering Term Project**.
Authors: Team **ANS**.