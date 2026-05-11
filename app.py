#Raed's Task
import streamlit as st
import pandas as pd
import os
from openai import OpenAI
from dotenv import load_dotenv

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(_file_), '.env'))

# Initialize OpenAI client
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    st.error("❌ OPENAI_API_KEY not found. Please set it in .env file.")
    st.stop()

client = OpenAI(api_key=api_key)

def generate_llm_summary(top_keywords):
    try:
        keywords_str = ", ".join(top_keywords[:15])
        prompt = f"""Analyze these trending keywords and generate a ONE-paragraph summary (max 80 words).
MUST mention at least 3 distinct named storylines or themes.
Keywords: {keywords_str}
Format: A single paragraph. No bullets."""

        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a news analyst."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=150,
            temperature=0.7
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"Top themes: {', '.join(top_keywords[:10])}. (LLM summary unavailable)"

def load_data(filename):
    path = os.path.join("data/processed", filename)
    if os.path.exists(path):
        try:
            return pd.read_json(path)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()

st.set_page_config(page_title="News Pulse", layout="wide")
st.title("News Pulse - Live Dashboard")

# Auto-refresh helper
if st.button("🔄 Refresh"):
    st.rerun()

st.caption(f"Last updated: {pd.Timestamp.now().strftime('%H:%M:%S')}")

source_df = load_data("by_source.json")
window_df = load_data("by_window.json")
words_df = load_data("top_words.json")

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
        def get_window_start(x):
            if isinstance(x, list):
                # Spark often outputs window as [start, end] list of timestamps
                return pd.to_datetime(x[0], unit='ms')
            elif isinstance(x, dict):
                return pd.to_datetime(x.get('start'))
            return pd.to_datetime(x)

        window_df['hour'] = window_df['window'].apply(lambda x: get_window_start(x).strftime('%H:%M'))
        st.line_chart(window_df.set_index("hour")['count'])
    else:
        st.info("Waiting for window data...")

st.subheader("Top 10 Keywords")
if not words_df.empty:
    words_df = words_df.sort_values("count", ascending=False).head(10)
    st.table(words_df)
else:
    st.info("Waiting for keyword data...")

st.subheader("LLM Summary")
if not words_df.empty:
    summary = generate_llm_summary(words_df["word"].tolist())
    st.write(summary)
else:
    st.write("Waiting for data to generate summary...")
