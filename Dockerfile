FROM quay.io/astronomer/astro-runtime:12.1.0


COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt