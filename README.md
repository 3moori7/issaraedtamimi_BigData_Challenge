# News Pulse - Real-Time Big Data Dashboard

News Pulse is a live news monitoring system built for the SE446 Big Data Engineering Challenge. It ingest headlines from multiple RSS feeds, processes them using Spark Structured Streaming, and generates AI-powered thematic summaries using OpenAI GPT-3.5.

## 🚀 Features
- **Live Ingestion**: Polls BBC, CNN, Al Jazeera, and NYT every 30-60 seconds.
- **Spark Processing**: Uses Structured Streaming for real-time windowed aggregations and keyword extraction.
- **LLM Insights**: Generates a one-paragraph thematic summary of the top 10-15 trending keywords.
- **Interactive Dashboard**: A Streamlit web interface with live charts and keyword tables.

## 🏗️ Architecture
The pipeline consists of three separate components running concurrently:
1. **Ingester (Python)**: Fetches RSS feeds and writes raw JSON-lines to `data/incoming/`.
2. **Spark Job (PySpark)**: Watches the folder, transforms the data, and writes the state to `data/processed/`.
3. **Dashboard (Streamlit)**: Reads the processed state and visualizes it in the browser.

## 🛠️ Setup

### 1. Prerequisites
- Java 11 or 17 (Required for PySpark)
- Python 3.9+

### 2. Install Dependencies
```bash
pip install pyspark==3.5.0 feedparser pandas streamlit openai python-dotenv
```

### 3. Configuration
Create a `.env` file in the root directory:
```env
OPENAI_API_KEY=your_sk_key_here
```

## 🚦 How to Run

Open three separate terminals and run the following:

**Terminal 1: Ingester**
```bash
python ingester.py
```

**Terminal 2: Spark Job**
```bash
python streaming_job.py
```

**Terminal 3: Dashboard**
```bash
streamlit run app.py
```

## 📝 Project Structure
- `ingester.py`: RSS polling logic.
- `streaming_job.py`: Spark Structured Streaming logic with `foreachBatch` sink.
- `app.py`: Streamlit frontend with LLM integration.
- `REFLECTION.md`: Analysis of system scalability.
- `data/`: Local storage for raw and processed data.

<img width="1649" height="920" alt="image" src="https://github.com/user-attachments/assets/f8156345-6154-4c6f-8c1d-23bbd08a8c4a" />

