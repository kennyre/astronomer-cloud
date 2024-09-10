FROM quay.io/astronomer/astro-runtime:12.1.0

RUN apt-get update && apt-get install -y \
    cmake \
    build-essential \
    libboost-all-dev \
    libarrow-dev \
    libparquet-dev \
    libssl-dev \
    libcurl4-openssl-dev \
    zlib1g-dev \
    bison \
    && rm -rf /var/lib/apt/lists/*

# Instalar dependencias de Python (incluyendo pyarrow)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
