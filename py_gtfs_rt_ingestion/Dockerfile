FROM python:3.9-slim

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1
# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED 1

# Install dev dependencies
RUN apt-get update
RUN apt-get install -y libpq-dev gcc curl

# Install poetry
RUN pip install -U pip
RUN pip install "poetry==1.1.14"

COPY . /py_gtfs_rt_ingestion/
WORKDIR /py_gtfs_rt_ingestion/
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev --no-interaction --no-ansi

# Fetch Amazon RDS certificate chain
RUN curl https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem -o /usr/local/share/amazon-certs.pem
RUN echo "6f99a8eac41b1b76bdc4f402c4a1f3d12d24fcae75ce08818ee3349bda075e93 /usr/local/share/amazon-certs.pem" | sha256sum -c -
RUN chmod a=r /usr/local/share/amazon-certs.pem

CMD ["python", "ingest.py"]
