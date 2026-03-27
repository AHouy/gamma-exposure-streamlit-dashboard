FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Override streamlit-highcharts index.html with our custom version (enabling heatmap, etc.)
COPY override/streamlit_highcharts/index.html /usr/local/lib/python3.11/site-packages/streamlit_highcharts/frontend/index.html

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "app.py"]