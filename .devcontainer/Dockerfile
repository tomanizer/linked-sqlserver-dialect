FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive

# Install Microsoft ODBC driver for SQL Server (msodbcsql18) for pyodbc usage.
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gnupg \
    unixodbc \
    unixodbc-dev \
  && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft.gpg \
  && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
     > /etc/apt/sources.list.d/microsoft-prod.list \
  && apt-get update \
  && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
  && rm -rf /var/lib/apt/lists/*


