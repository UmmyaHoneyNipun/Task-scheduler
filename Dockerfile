FROM python:3.11-slim

# Sets the working directory inside the container
WORKDIR /app

# Install standard system tools needed for PostgreSQL connections
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copying the requirements file and installing python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copying the application source code into the container
COPY scheduler/ ./scheduler/

# Python must prints logs immediately instead of buffering
ENV PYTHONUNBUFFERED=1

# Expose the API port
EXPOSE 8000

# Start the web server
CMD ["uvicorn", "scheduler.api:app", "--host", "0.0.0.0", "--port", "8000"]
