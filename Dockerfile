FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
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

RUN useradd --create-home --shell /bin/bash notebook \
    && mkdir -p /workspace/notebooks /workspace/data /workspace/warehouse /home/notebook/.ivy2 \
    && chown -R notebook:notebook /workspace /home/notebook

USER notebook

EXPOSE 8888

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["sh", "-lc", "/workspace/.venv/bin/python -m ipykernel install --user --name localbricks --display-name 'Python (localbricks)' >/dev/null 2>&1 || true; /workspace/.venv/bin/jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --ServerApp.token=${JUPYTER_TOKEN:-localbricks} --ServerApp.allow_origin='*' --ServerApp.root_dir=/workspace"]
