FROM python:3.10.12-slim-bullseye as builder

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


RUN pip install "poetry==$POETRY_VERSION"

# Set the working directory
WORKDIR /app

COPY pyproject.toml /app/

RUN poetry lock

RUN poetry install --without dev --no-interaction --no-ansi && rm -rf $POETRY_CACHE_DIR

COPY agents/ /app/agents/
# COPY frontend/__init__.py /app/frontend/__init__.py
COPY frontend/ /app/frontend/
COPY config/ /app/config/
COPY frontend.yaml /app/

# Expose the port the Cloud Run app runs on
EXPOSE 8000

# Specify the default command
# CMD sh -c "poetry run adk web agents"

# CMD ["poetry", "run", "uvicorn", "--host", "0.0.0.0", "agents.main:app", "--port", "8000"]

CMD ["poetry", "run", "streamlit", "run", "frontend/streamlit_app.py", "--server.port=8000", "--server.address", "0.0.0.0"]
