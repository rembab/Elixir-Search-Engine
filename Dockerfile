FROM ghcr.io/livebook-dev/livebook:0.12.1

USER root

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-numpy \
    python3-scipy \
    python3-sklearn \
    erlang-tools \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m livebook \
    && mkdir -p /data \
    && chown -R livebook:livebook /data


ENV LIVEBOOK_APPS_PATH="/apps"
ENV LIVEBOOK_APPS_PATH_WARMUP="manual"
ENV LIVEBOOK_PASSWORD="search_engine_admin" 

COPY --chown=livebook:livebook . /apps/

USER livebook

WORKDIR /apps

EXPOSE 8080
EXPOSE 8081
