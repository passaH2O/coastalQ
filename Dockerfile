# Create from Python3.13 image
FROM python:3.13-slim

# Install system dependencies
RUN apt update \
    && DEBIAN_FRONTEND=noninteractive apt install -y curl zip python3-dev build-essential libhdf5-serial-dev netcdf-bin libnetcdf-dev

# Create virtual environment and install dependencies
COPY requirements.txt /app/requirements.txt
RUN /usr/local/bin/python3 -m venv /app/env \
    && /app/env/bin/pip install -r /app/requirements.txt

# Copy and execute module
COPY ./coastalQ /app/coastalQ/
COPY run_coastalQ.py /app/run_coastalQ.py

# Metadata
LABEL version="0.0" \
	description="Containerized coastalQ algorithm." \
	"confluence.contact"="kwright@ethz.ch" \
	"algorithm.contact"="kwright@ethz.ch"

# Set the execution command
ENTRYPOINT ["/app/env/bin/python3", "/app/run_coastalQ.py"]