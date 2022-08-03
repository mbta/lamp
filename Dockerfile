FROM python:3.9-slim

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1
# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED 1

# Install dev dependencies
RUN apt-get update
RUN apt-get install -y libpq-dev gcc

# Install poetry
RUN pip install -U pip
RUN pip install "poetry==1.1.14"

COPY ./performance_manager /performance_manager/
WORKDIR /performance_manager/
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev --no-interaction --no-ansi
