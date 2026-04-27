FROM python:3.11-slim-bookworm

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

RUN useradd --create-home --shell /bin/bash notebook \
    && mkdir -p /workspace/notebooks /workspace/data /workspace/warehouse /home/notebook/.ivy2 /home/notebook/.ipython/profile_default \
    && cp /workspace/.jupyter/ipython_config.py /home/notebook/.ipython/profile_default/ipython_config.py \
    && chown -R notebook:notebook /workspace /home/notebook

USER notebook

EXPOSE 8888

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["sh", "-lc", "/workspace/.venv/bin/python -m ipykernel install --user --name localbricks --display-name 'Python (localbricks)' >/dev/null 2>&1 || true; /workspace/.venv/bin/jupyter lab --config=/workspace/.jupyter/jupyter_ai_config.py --ip=0.0.0.0 --port=8888 --no-browser --ServerApp.token=${JUPYTER_TOKEN:-localbricks} --ServerApp.allow_origin='*' --ServerApp.root_dir=/workspace"]
