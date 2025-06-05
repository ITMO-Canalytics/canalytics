FROM apache/airflow:2.7.1

USER root

# Install required system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create necessary directories and set permissions
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins /opt/airflow/data && \
    chown -R airflow:root /opt/airflow

# Copy single requirements file
COPY requirements.txt /requirements.txt
RUN chown airflow:root /requirements.txt

# Switch to airflow user for pip installs
USER airflow

# Install Python dependencies using Airflow's recommended approach
RUN pip install --no-cache-dir -r /requirements.txt

# Set work directory
WORKDIR /opt/airflow

# Initialize Airflow database when the container starts and create admin user
ENTRYPOINT ["/bin/bash", "-c", "(airflow db migrate || airflow db init) && airflow users create --username \"$AIRFLOW__WEBSERVER__ADMIN_USER\" --password \"$AIRFLOW__WEBSERVER__ADMIN_PASSWORD\" --firstname Admin --lastname User --role Admin --email \"$AIRFLOW__WEBSERVER__ADMIN_EMAIL\" || true && /entrypoint airflow $0 $@"]
