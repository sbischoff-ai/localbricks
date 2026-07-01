FROM python:3.11-slim-bookworm

ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG NO_PROXY
ARG http_proxy
ARG https_proxy
ARG no_proxy

ENV DEBIAN_FRONTEND=noninteractive \
    JUPYTER_CONFIG_DIR=/workspace/.jupyter \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    VIRTUAL_ENV=/workspace/.venv \
    PYSPARK_PYTHON=/workspace/.venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/workspace/.venv/bin/python \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
        openjdk-17-jre-headless \
        procps \
        tini \
    && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/workspace/.venv/bin:/root/.local/bin:${PATH}"

WORKDIR /workspace

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-dev --no-cache
COPY .jupyter /workspace/.jupyter
COPY localbricks /workspace/localbricks
COPY pipelines /workspace/pipelines

RUN useradd --create-home --shell /bin/bash notebook \
    && mkdir -p /workspace/notebooks /workspace/data /workspace/warehouse /home/notebook/.ivy2.5.2 /home/notebook/.ipython/profile_default \
    && cp /workspace/.jupyter/ipython_config.py /home/notebook/.ipython/profile_default/ipython_config.py \
    && chmod +x /workspace/.jupyter/start-jupyter.sh \
    && chown -R notebook:notebook /workspace /home/notebook

USER notebook

EXPOSE 8888

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/workspace/.jupyter/start-jupyter.sh"]
