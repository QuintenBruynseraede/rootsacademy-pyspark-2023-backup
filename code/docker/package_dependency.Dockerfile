FROM python:3.8
RUN apt-get update && apt-get install -y zip python-dev libffi-dev build-essential
WORKDIR /app
COPY .. /app
RUN curl -sSL https://install.python-poetry.org | python3 -
RUN /root/.local/bin/poetry install --only main

ENTRYPOINT (cd /root/.cache/pypoetry/virtualenvs/*/lib/python3.8/site-packages; zip -r /app/package.zip ./*)
