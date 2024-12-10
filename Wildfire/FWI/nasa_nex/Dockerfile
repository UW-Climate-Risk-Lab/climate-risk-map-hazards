# Use Python 3.12+ base image
FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY run.py .
COPY pipeline.py .

# Expose ports for Dask dashboard (8787 is default)
EXPOSE 8787

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Set the entrypoint to run the Python script
ENTRYPOINT ["python", "pipeline.py"]