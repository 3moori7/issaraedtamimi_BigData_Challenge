import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession
import os
from openai import OpenAI
from dotenv import load_dotenv

# Load environment variables from .env file (explicitly from current directory)
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

spark = (
    SparkSession.builder
    .appName("NewsPulse")
    .master("local[*]")
    .getOrCreate()
)

# Initialize OpenAI client from environment variable
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    st.error("❌ OPENAI_API_KEY not found. Please set it in .env file or environment.")
    st.stop()

client = OpenAI(api_key=api_key)

def generate_llm_summary(top_keywords):
    """
    Call OpenAI API to generate a thematic summary.
    Constraints: max 80 words, must mention at least 3 named storylines.
    Falls back to keyword summary if API fails.
    """
    try:
        # Get top 15 keywords for LLM context
        keywords_str = ", ".join(top_keywords[:15])
        
        prompt = f"""Analyze these trending keywords and generate a ONE-paragraph summary (max 80 words) 
of the main news storylines. MUST mention at least 3 distinct named storylines or themes.

Keywords: {keywords_str}

Format: A single paragraph. No bullets. No preamble."""

        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a news analyst. Generate concise, factual summaries."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=150,
            temperature=0.7
        )
        
        summary = response.choices[0].message.content.strip()
        
        # Enforce word count (roughly)
        words = summary.split()
        if len(words) > 100:
            summary = " ".join(words[:100]) + "..."
        
        return summary
        
    except Exception as e:
        # Fallback: keyword-only summary
        return f"Top themes from trending keywords: {', '.join(top_keywords[:10])}. Unable to generate full LLM summary at this moment."

st.set_page_config(page_title="News Pulse", layout="wide")

st.title("News Pulse - Live Dashboard")

st.write("This dashboard shows live RSS headline trends using PySpark Structured Streaming.")

# Show available tables for debugging
with st.expander("🔍 Debug Info"):
    try:
        tables = spark.sql("SHOW TABLES").collect()
        table_names = [row.tableName for row in tables]
        st.write(f"Available tables: {table_names if table_names else 'None yet'}")
        st.write(f"Total memory tables: {len(table_names)}")
    except Exception as e:
        st.write(f"Debug error: {e}")

st.markdown("---")

# Auto-refresh capability
col1, col2 = st.columns([1, 5])
with col1:
    if st.button("🔄 Refresh"):
        st.rerun()
with col2:
    st.caption(f"Last updated: {pd.Timestamp.now().strftime('%H:%M:%S')}")
try:
    source_df = spark.sql("SELECT * FROM by_source").toPandas()
except Exception as e:
    st.warning(f"Source data loading: {str(e)[:100]}")
    source_df = pd.DataFrame(columns=["source", "count"])

try:
    window_df = spark.sql("""
        SELECT 
            CAST(window.start AS STRING) AS hour,
            count
        FROM by_window
        ORDER BY hour
    """).toPandas()
except Exception as e:
    st.warning(f"Window data loading: {str(e)[:100]}")
    window_df = pd.DataFrame(columns=["hour", "count"])

try:
    words_df = spark.sql("""
        SELECT word, count
        FROM top_words
        ORDER BY count DESC
        LIMIT 10
    """).toPandas()
except Exception as e:
    st.warning(f"Keyword data loading: {str(e)[:100]}")
    words_df = pd.DataFrame(columns=["word", "count"])

col1, col2 = st.columns(2)

with col1:
    st.subheader("Headline Count by Source")
    if not source_df.empty:
        st.bar_chart(source_df.set_index("source"))
    else:
        st.info("Waiting for source data...")

with col2:
    st.subheader("Headline Count by Hour")
    if not window_df.empty:
        st.line_chart(window_df.set_index("hour"))
    else:
        st.info("Waiting for window data...")

st.subheader("Top 10 Keywords")
if not words_df.empty:
    st.table(words_df)
else:
    st.info("Waiting for keyword data...")

st.subheader("LLM Summary")

if not words_df.empty:
    keywords_list = words_df["word"].tolist()
    summary = generate_llm_summary(keywords_list)
else:
    summary = "Waiting for enough keywords to generate a summary."

st.write(summary)