FROM python:3.8-slim

# Install system dependencies including Java
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    openssh-client \
    procps \
    default-jre \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Install wheel and build packages
RUN pip install --upgrade pip wheel setuptools watchfiles

# Install specific versions to fix compatibility issues
RUN pip install aiohttp_cors==0.7.0
RUN pip install typing_extensions==4.5.0

RUN pip install ray

# Copy the entire hydra directory
COPY . /hydra/
WORKDIR /hydra

# Install Hydra core and development dependencies
RUN pip install -r requirements/dev.txt && \
    pip install -e .

# Now use setup.py develop for your plugin but with Ray already installed
RUN cd plugins/hydra_ray_jobs_launcher && \
    python setup.py develop --no-deps

# Set environment variable so Python can find your modules
ENV PYTHONPATH="/hydra:${PYTHONPATH}"

# Create SSH directory
RUN mkdir -p /root/.ssh && chmod 700 /root/.ssh