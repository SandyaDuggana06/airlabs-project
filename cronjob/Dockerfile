# Dockerfile.cron
FROM python:3.12-slim

# Install cron and bash
RUN apt-get update && apt-get install -y cron bash && rm -rf /var/lib/apt/lists/*

# Copy shared requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy a script that triggers refresh
COPY refreshtask.py .

# Copy a crontab file
COPY crontab.txt /etc/cron.d/refresh-cron

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/refresh-cron

# Apply cron job
RUN crontab /etc/cron.d/refresh-cron

# Create the log file to be able to run tail
RUN touch /var/log/cron.log

# Run the cron service
CMD ["cron", "-f"]
