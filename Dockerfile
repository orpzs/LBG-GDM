# Use the official public Python image from Docker Hub instead of a private mirror
FROM python:3.10.12-slim as builder

# These environment variables are standard and can be kept
ENV HOME=/root \
    POETRY_VIRTUALENVS_CREATE=0 \
    PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.8.2 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

# Install poetry
RUN pip install "poetry==$POETRY_VERSION"

# Set the working directory
WORKDIR /app


# Copy requirements file
COPY pyproject.toml /app/

# REMOVED: The custom poetry source is gone. Poetry will now use the public PyPI repository by default.

# Lock dependencies based on the public repository
RUN poetry lock

# Install dependencies and clean up cache
RUN poetry install --without dev --no-interaction --no-ansi && rm -rf $POETRY_CACHE_DIR

# Copy the application code
COPY agents/ /app/agents/
COPY config/ /app/config/
COPY config.yaml /app/
COPY .env /app/

# Expose the application port
EXPOSE 8080

# Specify the default command to run the application
CMD ["poetry", "run", "uvicorn", "--host", "0.0.0.0", "agents.main:app", "--port", "8080"]