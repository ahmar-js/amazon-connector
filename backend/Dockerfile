# syntax=docker/dockerfile:1.4
FROM python:3.10-slim-bookworm

# metadata
LABEL maintainer="ahmaraamir33@gmail.com"
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    POETRY_VIRTUALENVS_CREATE=false

# create non-root user
ARG APP_USER=app
ARG APP_HOME=/app

RUN set -eux; \
    apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      gcc \
      g++ \
      curl \
      gnupg \
      ca-certificates \
      unixodbc-dev \
      apt-transport-https \
      lsb-release \
      libssl-dev \
      default-libmysqlclient-dev \
      netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC Driver 17 for SQL Server (Debian-based)
RUN set -eux; \
    mkdir -p /usr/share/keyrings; \
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg; \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list; \
    apt-get update && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql17

# Create app directory & user, copy code
RUN useradd --create-home --shell /bin/bash $APP_USER
WORKDIR $APP_HOME
COPY --chown=$APP_USER:$APP_USER requirements.txt $APP_HOME/

# Install pip dependencies (use --no-cache-dir)
RUN pip install --upgrade pip setuptools wheel \
 && pip wheel --no-deps --wheel-dir /wheels -r /app/requirements.txt \
 && pip install --no-cache-dir --no-index --find-links=/wheels -r /app/requirements.txt

# Copy project files
COPY --chown=$APP_USER:$APP_USER . $APP_HOME/

# Make entrypoint executable
COPY --chown=$APP_USER:$APP_USER ./docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ENV PATH="/home/${APP_USER}/.local/bin:${PATH}"

USER $APP_USER

# Expose port (gunicorn will bind to this)
EXPOSE 8000

# Default command - use entrypoint to allow running migrations, etc.
ENTRYPOINT ["entrypoint.sh"]
CMD ["gunicorn", "amazon_connector.wsgi:application", "--bind", "0.0.0.0:8000", "--workers", "3", "--threads", "2"]
