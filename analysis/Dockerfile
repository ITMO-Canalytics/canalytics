FROM jupyter/pyspark-notebook:latest

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create analysis and data directories for jovyan user (already exists in jupyter image)
RUN mkdir -p /home/jovyan/analysis /home/jovyan/data && \
    chown -R jovyan:users /home/jovyan

# Copy single requirements file
COPY requirements.txt /tmp/requirements.txt

# Install Python dependencies as root into system Python
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Switch to jovyan
USER jovyan
WORKDIR /home/jovyan

# Expose the port Jupyter will run on
EXPOSE 8888 