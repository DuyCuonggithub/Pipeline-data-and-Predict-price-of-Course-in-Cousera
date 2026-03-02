# Dockerfile (Fixed for Postgres & dbt)

# Bắt đầu từ ảnh Airflow chính thức
FROM apache/airflow:2.8.1-python3.11

# --- BƯỚC 1: Cài đặt thư viện hệ thống (Quyền ROOT) ---
USER root

# Cài đặt Xvfb, Playwright deps và ĐẶC BIỆT là libpq-dev cho Postgres
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    xvfb \
    build-essential \
    libpq-dev \
    libglib2.0-0 \
    libnspr4 \
    libnss3 \
    libdbus-1-3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libxkbcommon0 \
    libatspi2.0-0 \
    libxcomposite1 \
    libxdamage1 \
    libgbm1 \
    libcairo2 \
    libpango-1.0-0 \
    libasound2 \
    && rm -rf /var/lib/apt/lists/*

# --- BƯỚC 2: Cài đặt gói Python & Trình duyệt (Quyền AIRFLOW) ---
USER airflow

# Copy file requirements
COPY requirements.txt /requirements.txt

# Cài đặt thư viện Python và trình duyệt Playwright
RUN pip install --no-cache-dir -r /requirements.txt \
    && playwright install chromium