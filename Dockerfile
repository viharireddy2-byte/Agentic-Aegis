# Agentic Aegis — container image
# ===================================
# Runs the Streamlit dashboard by default. Run the pipeline instead with:
#   docker run --rm -v $(pwd)/data:/app/data agentic-aegis \
#     python -m src.orchestration.foundry_flows

FROM python:3.11-slim

WORKDIR /app

# System deps needed by psycopg2-binary/pymysql wheels at import time on slim images.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements-extra.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir -r requirements-extra.txt

COPY . .

RUN python scripts/setup_initial.py

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/_stcore/health')" || exit 1

CMD ["streamlit", "run", "dashboards/foundry_dashboard.py", \
     "--server.address=0.0.0.0", "--server.port=8080"]
