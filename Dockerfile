FROM quay.io/astronomer/astro-runtime:12.1.0

# Instalar cmake y otras dependencias necesarias
RUN apt-get update && apt-get install -y \
    cmake \
    build-essential \
    libboost-all-dev \
    && rm -rf /var/lib/apt/lists/*

# Instalar dependencias de Python (incluyendo pyarrow)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
