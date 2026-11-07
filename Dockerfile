# Use an official lightweight Python image
FROM python:3.13-slim

# Install git so pip can clone the 0.6 master branch code
RUN apt-get update && apt-get install -y --no-install-recommends git ffmpeg && rm -rf /var/lib/apt/lists/*

# Copy the requirements file first to leverage Docker's build cache
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

COPY src .

# Run the Python script when the container starts
CMD ["python", "adc_relay.py"]