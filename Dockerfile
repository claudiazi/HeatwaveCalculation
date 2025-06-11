# Use Python slim image as base
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    default-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install pyspark==3.4.0 pandas

# Copy requirements and install dependencies
COPY requirements.txt /app/
RUN pip install -r requirements.txt

# Copy the application code
COPY src/ /app/src/
COPY main.py /app/

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Set the entrypoint
ENTRYPOINT ["python", "main.py"]

# Default command (can be overridden)
CMD ["--mode", "both"]