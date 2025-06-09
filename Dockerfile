# Use the official PySpark image as base
FROM apache/spark-py:v3.4.0

# Set working directory
WORKDIR /app

# Install additional dependencies
RUN pip install pandas

# Copy the application code
COPY main.py /app/

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Create a directory for data
RUN mkdir -p /app/data

# Set the entrypoint
ENTRYPOINT ["spark-submit", "--master", "local[*]", "main.py"]

# Default command (can be overridden)
CMD ["--mode", "both"]