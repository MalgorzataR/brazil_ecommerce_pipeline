FROM apache/airflow:3.1.3

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        unzip \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow