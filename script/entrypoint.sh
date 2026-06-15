#!/bin/bash
set -e

# 1. Install requirements secara aman ke level user
if [ -e "/opt/airflow/requirements.txt" ]; then
    echo "Installing requirements.txt..."
    pip3 install --user --no-cache-dir -r /opt/airflow/requirements.txt
fi

# 2. Inisialisasi database Postgres yang masih kosong
echo "Initializing Airflow database schema..."
airflow db init

# 3. Buat user admin (ditambahkan proteksi '|| echo' agar tidak crash jika sudah ada)
echo "Creating admin user..."
airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin || echo "Admin user already exists or creation skipped."

echo "Starting Airflow Webserver..."
exec airflow webserver