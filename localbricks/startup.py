from __future__ import annotations

import builtins
import os

from localbricks.spark import create_spark


def _auto_spark_enabled() -> bool:
    value = os.getenv("LOCALBRICKS_AUTO_SPARK", "true")
    return value not in {"0", "false", "FALSE", "False", "no", "NO", "No", "off", "OFF", "Off"}


def ensure_notebook_spark() -> None:
    if not _auto_spark_enabled():
        return

    spark = create_spark("localbricks-notebook")
    builtins.spark = spark

    try:
        ipython = get_ipython()  # type: ignore[name-defined]
    except NameError:
        return

    ipython.user_ns.setdefault("spark", spark)

