# Use official lightweight Python image
FROM python:3.12-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DJANGO_SETTINGS_MODULE=trading_platform.settings

# Set work directory
WORKDIR /app

# Install system dependencies (git is required for neo-api-client in requirements.txt)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . /app/

# Make sure entrypoint script is executable
RUN chmod +x /app/dockerentrypoint.sh

# Expose Daphne's default port
EXPOSE 8000

# Set entrypoint
ENTRYPOINT ["/app/dockerentrypoint.sh"]
