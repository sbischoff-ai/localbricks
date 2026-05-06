#!/bin/sh
set -eu

spark_proxy_opts="$(/workspace/.venv/bin/python /workspace/.jupyter/spark_proxy_opts.py)"
if [ -n "$spark_proxy_opts" ]; then
    if [ -n "${SPARK_SUBMIT_OPTS:-}" ]; then
        export SPARK_SUBMIT_OPTS="${SPARK_SUBMIT_OPTS} $spark_proxy_opts"
    else
        export SPARK_SUBMIT_OPTS="$spark_proxy_opts"
    fi
fi

/workspace/.venv/bin/python -m ipykernel install \
    --user \
    --name localbricks \
    --display-name "Python (localbricks)" >/dev/null 2>&1 || true

set -- \
    --ip=0.0.0.0 \
    --port=8888 \
    --no-browser \
    "--ServerApp.token=${JUPYTER_TOKEN:-localbricks}" \
    "--ServerApp.allow_origin=*" \
    --ServerApp.root_dir=/workspace

case "${JUPYTER_AI_ENABLED:-false}" in
    true|TRUE|True|1|yes|YES|Yes|on|ON|On)
        /workspace/.venv/bin/jupyter labextension enable @jupyter-ai/core --level=user --no-build >/dev/null 2>&1 || true
        set -- --config=/workspace/.jupyter/jupyter_ai_config.py "$@"
        ;;
    *)
        /workspace/.venv/bin/jupyter labextension disable @jupyter-ai/core --level=user --no-build >/dev/null 2>&1 || true
        set -- --config=/workspace/.jupyter/jupyter_ai_disabled_config.py "$@"
        ;;
esac

exec /workspace/.venv/bin/jupyter lab "$@"
