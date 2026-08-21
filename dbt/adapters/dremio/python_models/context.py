# Copyright (C) 2022 Dremio Corporation

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Runtime context objects passed into dbt Python models executed by the
Dremio adapter.

A Python model receives two arguments:
    def model(dbt, session):
        ...

- ``dbt``     is a :class:`DbtDremioContext` instance
- ``session`` is a :class:`DremioSession` instance (a thin pandas session wrapper)

The user calls ``dbt.ref("model_name")`` / ``dbt.source("src", "tbl")`` to get
pandas DataFrames, and returns the transformed DataFrame.  The adapter then
persists that DataFrame back to Dremio via INSERT or CTAS.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("dremio")


# ---------------------------------------------------------------------------
# Config proxy
# ---------------------------------------------------------------------------

class DbtDremioConfig:
    """Mirrors the dbt.config() / dbt.config.get() API inside a Python model."""

    def __init__(self, config_dict: Dict[str, Any]):
        self._config: Dict[str, Any] = dict(config_dict)

    # Called as dbt.config(key=value) inside the model function
    def __call__(self, **kwargs: Any) -> None:
        self._config.update(kwargs)

    def get(self, key: str, default: Any = None) -> Any:
        return self._config.get(key, default)


# ---------------------------------------------------------------------------
# Session (the second argument to model())
# ---------------------------------------------------------------------------

class DremioSession:
    """
    Thin session wrapper that satisfies the dbt Python model contract.

    The session does not carry an active connection; it is only used to
    expose a pandas-compatible ``sql()`` helper so models can run ad-hoc
    SQL against Dremio and receive the result as a DataFrame.
    """

    def __init__(self, fetch_fn):
        """
        Parameters
        ----------
        fetch_fn : callable
            A function ``(sql: str) -> pd.DataFrame`` that executes a SQL
            query on Dremio and returns the result as a pandas DataFrame.
        """
        self._fetch = fetch_fn

    def sql(self, query: str) -> "pd.DataFrame":
        """Execute a SQL query and return the result as a pandas DataFrame."""
        return self._fetch(query)


# ---------------------------------------------------------------------------
# Main dbt context
# ---------------------------------------------------------------------------

class DbtDremioContext:
    """
    The ``dbt`` argument injected into every Python model function.

    Implements the dbt-standard interface:
      - dbt.ref("model_name")
      - dbt.source("source_name", "table_name")
      - dbt.this          → the target relation string
      - dbt.config        → DbtDremioConfig instance
      - dbt.is_incremental
    """

    def __init__(
        self,
        model_config: Dict[str, Any],
        refs: Dict[str, str],
        sources: Dict[tuple, str],
        this: str,
        is_incremental: bool,
        fetch_fn,
    ):
        """
        Parameters
        ----------
        model_config : dict
            The resolved model configuration (materialized, tags, meta, etc.)
        refs : dict
            Mapping of ref-name → fully qualified SQL relation name.
            E.g. ``{"my_model": '"my_space"."folder"."my_model"'}``
        sources : dict
            Mapping of (source_name, table_name) → fully qualified SQL relation name.
        this : str
            The rendered target relation for the current model.
        is_incremental : bool
            True when the model is running in incremental mode.
        fetch_fn : callable
            A function ``(sql: str) -> pd.DataFrame`` used by dbt.ref(),
            dbt.source(), and session.sql().
        """
        self._refs = refs
        self._sources = sources
        self._this_str = this
        self._is_incremental = is_incremental
        self._fetch = fetch_fn
        self.config = DbtDremioConfig(model_config)

    # ------------------------------------------------------------------
    # dbt.ref()
    # ------------------------------------------------------------------

    def ref(self, *args: str) -> "pd.DataFrame":
        """
        Return the contents of an upstream model as a pandas DataFrame.

        Accepts one argument (model name) or two (package name, model name),
        mirroring the SQL {{ ref() }} macro signature.
        """
        if len(args) == 1:
            key = args[0]
        elif len(args) == 2:
            # package-qualified ref: only use the model name part as key
            key = args[1]
        else:
            raise TypeError(f"dbt.ref() takes 1 or 2 arguments ({len(args)} given)")

        if key not in self._refs:
            raise KeyError(
                f"dbt.ref('{key}') not found in compiled refs. "
                f"Available: {list(self._refs.keys())}"
            )

        relation = self._refs[key]
        logger.debug(f"dbt.ref('{key}') → SELECT * FROM {relation}")
        return self._fetch(f"SELECT * FROM {relation}")

    # ------------------------------------------------------------------
    # dbt.source()
    # ------------------------------------------------------------------

    def source(self, source_name: str, table_name: str) -> "pd.DataFrame":
        """Return the contents of a source table as a pandas DataFrame."""
        key = (source_name, table_name)
        if key not in self._sources:
            raise KeyError(
                f"dbt.source('{source_name}', '{table_name}') not found in compiled sources. "
                f"Available: {list(self._sources.keys())}"
            )

        relation = self._sources[key]
        logger.debug(f"dbt.source('{source_name}', '{table_name}') → SELECT * FROM {relation}")
        return self._fetch(f"SELECT * FROM {relation}")

    # ------------------------------------------------------------------
    # dbt.this
    # ------------------------------------------------------------------

    @property
    def this(self) -> "_DremioThis":
        return _DremioThis(self._this_str)

    # ------------------------------------------------------------------
    # dbt.is_incremental
    # ------------------------------------------------------------------

    @property
    def is_incremental(self) -> bool:
        return self._is_incremental


# ---------------------------------------------------------------------------
# dbt.this helper
# ---------------------------------------------------------------------------

class _DremioThis:
    """Mimics the ``dbt.this`` object so models can call ``str(dbt.this)``."""

    def __init__(self, rendered: str):
        self._rendered = rendered
        # Best-effort parse into components
        parts = rendered.replace('"', "").split(".")
        self.database = parts[0] if len(parts) > 0 else ""
        self.schema = parts[1] if len(parts) > 1 else ""
        self.identifier = parts[-1] if len(parts) > 0 else ""

    def __str__(self) -> str:
        return self._rendered

    def __repr__(self) -> str:  # pragma: no cover
        return f"_DremioThis({self._rendered!r})"
