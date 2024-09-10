FROM quay.io/astronomer/astro-runtime:12.1.0

# Instalar cmake y otras dependencias necesarias
RUN apt-get update && apt-get install -y \
    cmake \
    build-essential \
    libboost-all-dev \
    libarrow-dev \
    libparquet-dev \
    && rm -rf /var/lib/apt/lists/*

# Desactivar opciones innecesarias de pyarrow
ENV PYARROW_BUILD_CUDA=OFF \
    PYARROW_BUILD_FLIGHT=OFF \
    PYARROW_BUILD_GANDIVA=OFF \
    PYARROW_BUILD_DATASET=OFF \
    PYARROW_BUILD_ORC=OFF \
    PYARROW_BUILD_PARQUET=OFF \
    PYARROW_BUILD_PARQUET_ENCRYPTION=OFF \
    PYARROW_BUILD_PLASMA=OFF \
    PYARROW_BUILD_S3=OFF \
    PYARROW_BUILD_HDFS=OFF \
    PYARROW_USE_TENSORFLOW=OFF \
    PYARROW_BUNDLE_ARROW_CPP=OFF \
    PYARROW_BUNDLE_BOOST=OFF \
    PYARROW_BOOST_USE_SHARED=ON \
    PYARROW_PARQUET_USE_SHARED=ON

# Instalar dependencias de Python (incluyendo pyarrow)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
