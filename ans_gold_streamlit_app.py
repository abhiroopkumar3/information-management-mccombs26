# TERM PROJECT
# TEAM NAME: ANS
# MEMBER NAMES: Abhiroop Kumar (ak56448), Nikhil Kumar (nk25627), Simoni K Dalal (skd939)
# DATABASE: DB_TEAM_ANS
# ROLE: ROLE_TEAM_ANS
# DUE: Dec 8 at 11:59pm

# FILE NAME: ans_gold_streamlit_app.py

# SOLUTION

import streamlit as st
from snowflake.snowpark.context import get_active_session

# Get active Snowflake session
session = get_active_session()

# -------------------------------
# 1. Load data from GOLD tables
# -------------------------------

lang_df = session.sql("""
    SELECT
        LANGUAGE_NAME,
        COURSE_COUNT,
        TOTAL_TIME_HOURS,
        TOTAL_CHAPTERS,
        TOTAL_EXERCISES,
        TOTAL_VIDEOS
    FROM GOLD.G_LANGUAGE_INSTRUCTIONAL_EFFORT
    ORDER BY TOTAL_TIME_HOURS DESC
""").to_pandas()

track_df = session.sql("""
    SELECT
        TRACK_TITLE,
        IS_CAREER_FLAG,
        COURSE_COUNT,
        TOTAL_TIME_HOURS,
        TOTAL_CHAPTERS,
        TOTAL_EXERCISES,
        TOTAL_VIDEOS
    FROM GOLD.G_TRACK_CONTENT_SUMMARY
    ORDER BY TOTAL_TIME_HOURS DESC
""").to_pandas()

diff_df = session.sql("""
    SELECT
        DIFFICULTY_CODE,
        COURSE_COUNT,
        TOTAL_TIME_HOURS,
        TOTAL_CHAPTERS,
        TOTAL_EXERCISES,
        TOTAL_VIDEOS
    FROM GOLD.G_DIFFICULTY_CONTENT_SUMMARY
    ORDER BY DIFFICULTY_CODE
""").to_pandas()

# -------------------------------
# 2. Streamlit layout
# -------------------------------

st.title("ANS – DataCamp Gold Layer Dashboard")

st.caption(
    "Visualizations built from GOLD tables: "
    "`G_LANGUAGE_INSTRUCTIONAL_EFFORT`, "
    "`G_TRACK_CONTENT_SUMMARY`, "
    "`G_DIFFICULTY_CONTENT_SUMMARY`."
)

tab_lang, tab_track, tab_diff = st.tabs(
    ["1. By Programming Language", "2. By Track", "3. By Difficulty"]
)

# --- Tab 1: Language ---
with tab_lang:
    st.subheader("Instructional Effort by Programming Language")

    st.dataframe(lang_df)

    st.markdown("**Total learning hours per programming language**")
    st.bar_chart(lang_df, x="LANGUAGE_NAME", y="TOTAL_TIME_HOURS")

    st.markdown("**Total chapters per programming language**")
    st.bar_chart(lang_df, x="LANGUAGE_NAME", y="TOTAL_CHAPTERS")

# --- Tab 2: Track ---
with tab_track:
    st.subheader("Content Summary by Track")

    track_type = st.radio(
        "Filter tracks by type:",
        options=["All", "Career Tracks", "Skill Tracks"],
        horizontal=True,
    )

    track_filtered = track_df.copy()
    if track_type == "Career Tracks":
        track_filtered = track_filtered[track_filtered["IS_CAREER_FLAG"] == 1]
    elif track_type == "Skill Tracks":
        track_filtered = track_filtered[track_filtered["IS_CAREER_FLAG"] == 0]

    st.dataframe(track_filtered)

    st.markdown("**Total learning hours per track**")
    st.bar_chart(track_filtered, x="TRACK_TITLE", y="TOTAL_TIME_HOURS")

    st.markdown("**Number of courses per track**")
    st.bar_chart(track_filtered, x="TRACK_TITLE", y="COURSE_COUNT")

# --- Tab 3: Difficulty ---
with tab_diff:
    st.subheader("Course Distribution and Content by Difficulty")

    st.dataframe(diff_df)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Course count by difficulty level**")
        st.bar_chart(diff_df, x="DIFFICULTY_CODE", y="COURSE_COUNT")

    with col2:
        st.markdown("**Total learning hours by difficulty level**")
        st.bar_chart(diff_df, x="DIFFICULTY_CODE", y="TOTAL_TIME_HOURS")
