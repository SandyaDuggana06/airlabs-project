FROM python:3.12-slim

WORKDIR /app

# Copy shared requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN apt-get update && apt-get install -y wget

# Copy API code
COPY myproject.py .

# Expose API port
EXPOSE 8000

# Use uvicorn to run FastAPI
CMD ["uvicorn", "myproject:app", "--host", "0.0.0.0", "--port", "8000"]
