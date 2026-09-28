FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY service.py .

# Port and identity come from the compose file (command + environment).
CMD ["python", "service.py", "8001"]
