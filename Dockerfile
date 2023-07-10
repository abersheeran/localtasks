FROM python:3.11-slim as requirements

WORKDIR /src

RUN python -m pip install -U pdm

RUN pdm config python.use_venv False

COPY pyproject.toml pyproject.toml
COPY pdm.lock pdm.lock

RUN pdm export --production -f requirements -o requirements.txt --without-hashes

FROM python:3.11-slim

WORKDIR /src

COPY --from=requirements /src/requirements.txt requirements.txt

RUN python -m pip install -r requirements.txt

COPY . .

CMD ["runweb", "-a", "localtasks.api:app", "-b", "0.0.0.0:80"]
