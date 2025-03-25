FROM python:3.10-slim-bookworm

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1
# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED 1

# Install non python dependencies
RUN apt-get update
RUN apt-get install -y libpq-dev gcc curl gpg

# Fetch Amazon RDS certificate chain
RUN curl https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem -o /usr/local/share/amazon-certs.pem
RUN chmod a=r /usr/local/share/amazon-certs.pem

# Install MSSQL ODBC 18 Driver
# for TransitMaster DB connection and ingestion
RUN mkdir -m 0755 -p /etc/apt/keyrings/ \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /etc/apt/keyrings/microsoft.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" | tee /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18

# modify openssl config to allow TLSv1 connection
# moves [openssl_init] section and creates [ssl_default_sect] section to allow TLSv1
# for TransitMaster DB connection and ingestion
RUN sed -i 's/\[openssl_init\]/# [openssl_init]/' /etc/ssl/openssl.cnf \
    && echo '\n\n[openssl_init]\nssl_conf = ssl_sect\n\n[ssl_sect]\nsystem_default = ssl_default_sect\n\n[ssl_default_sect]\nMinProtocol = TLSv1\nCipherString = DEFAULT@SECLEVEL=0\n' >> /etc/ssl/openssl.cnf

# Create tmp directory that will mount to the ephemeral storage on ECS
# Implemented to solve this problem: https://github.com/aws/amazon-ecs-agent/issues/3594
# Where the reported memory usage reported up to ECS far exceeds the actual memory usage
# when many reads/writes occur on a temp directory. 
# Related terraform changes are here: https://github.com/mbta/devops/pull/2719 
RUN mkdir -m 1777 -p /tmp
VOLUME ["/tmp"]

# Install poetry
RUN pip install -U pip
RUN pip install "poetry==1.7.1"

# copy poetry and pyproject files and install dependencies
WORKDIR /lamp/
COPY poetry.lock poetry.lock
COPY pyproject.toml pyproject.toml

# Tableau dependencies for arm64 cannot be resolved (since salesforce doesn't
# support them yet). For that buildplatform build without those dependencies
ARG TARGETARCH BUILDPLATFORM TARGETPLATFORM
RUN echo "Installing python dependencies for build: ${BUILDPLATFORM} target: ${TARGETPLATFORM}"
RUN if [ "$TARGETARCH" = "arm64" ]; then \
    poetry install --without tableau --no-interaction --no-ansi -v ;\
    else poetry install --no-interaction --no-ansi -v ;\
    fi

# Copy src directory to run against and build lamp py
COPY src src
COPY alembic.ini alembic.ini

# Add Version information as an argument, it is provided by GHA and left to the
# default for local development.
ARG VERSION="v0.0.0-unknown"
RUN echo "VERSION = '${VERSION}'" > src/lamp_py/__version__.py

RUN poetry install --only main --no-interaction --no-ansi -v
