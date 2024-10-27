###########
## BUILD ##
###########
FROM python:3.10-slim-bullseye as build

ENV POETRY_VERSION=1.1.11

RUN apt-get update \
    && apt-get install -y git build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && pip install poetry==${POETRY_VERSION}

WORKDIR /opt/datalineup

ADD pyproject.toml poetry.lock README.md ./
ADD src ./src

RUN poetry build

################
## MAIN IMAGE ##
################
FROM python:3.10-slim-bullseye

COPY --from=build /opt/datalineup/dist/*.whl /opt/datalineup/

RUN apt-get update \
    && apt-get install -y git build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && pip install /opt/datalineup/*.whl \
    && rm -rf /opt/datalineup

ENV DATALINEUP_FLASK_HOST=0.0.0.0
ENV DATALINEUP_FLASK_PORT=80
ENV DATALINEUP_DATABASE_URL=sqlite:///tmp/datalineup.sqlite

EXPOSE 80

CMD python -m datalineup_engine.worker_manager.server
