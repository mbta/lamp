FROM python:3.9-slim

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1
# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED 1

# Install non python dependencies
RUN apt-get update
RUN apt-get install -y libpq-dev gcc curl

# Fetch Amazon RDS certificate chain
RUN curl https://s3.amazonaws.com/rds-downloads/rds-ca-2019-root.pem -o /usr/local/share/amazon-certs.pem
RUN echo "d464378fbb8b981d2b28a1deafffd0113554e6adfb34535134f411bf3c689e73 /usr/local/share/amazon-certs.pem" | sha256sum -c -
RUN chmod a=r /usr/local/share/amazon-certs.pem

# Install poetry
RUN pip install -U pip
RUN pip install "poetry==1.1.14"

# copy poetry and pyproject files and install dependencies
WORKDIR /lamp/
COPY poetry.lock poetry.lock
COPY pyproject.toml pyproject.toml
RUN poetry install --no-dev --no-interaction --no-ansi -v

# Copy src directory to run against and build lamp py
COPY src src
COPY alembic.ini alembic.ini
RUN poetry install --no-dev --no-interaction --no-ansi -v
