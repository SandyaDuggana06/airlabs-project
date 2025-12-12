# Use slim Python base
FROM python:3.12-slim

# Install cron, bash, and procps (for ps, top)
RUN apt-get update && apt-get install -y cron bash procps && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the cron script
COPY refreshtask.py /app/refreshtask.py

# Copy the crontab file
COPY crontab.txt /etc/cron.d/refresh-cron

# Set permissions for cron file
RUN chmod 0644 /etc/cron.d/refresh-cron

# Apply cron job
RUN crontab /etc/cron.d/refresh-cron

# Create log file
RUN touch /var/log/cron.log

# Run cron in the foreground
CMD ["cron", "-f"]
