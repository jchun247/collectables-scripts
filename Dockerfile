FROM python:3.13-slim

WORKDIR /scripts

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Python scripts
COPY . .

# Use ENTRYPOINT to accept script name as parameter
ENTRYPOINT ["python"]