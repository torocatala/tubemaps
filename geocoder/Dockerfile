FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY extract_locations.py .

CMD ["python", "extract_locations.py"]
