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
Iceberg REST Catalog writer for dbt Python model materializations.

Writes a pandas DataFrame to an Iceberg table using ``pandas.DataFrame.to_iceberg()``,
which internally uses PyIceberg. Supports any Iceberg REST-compatible catalog:
Nessie, Polaris, Apache Gravitino, AWS Glue (REST mode), Unity Catalog, etc.

Required profile configuration::

    my_profile:
      outputs:
        dev:
          type: dremio
          software_host: my-dremio-host
          port: 9047
          ...
          # Iceberg REST Catalog — required for Python models
          iceberg_catalog_uri: "http://nessie:19120/iceberg/main"
          iceberg_catalog_name: rest                # optional, default "dremio"
          iceberg_catalog_namespace: staging        # optional, overrides schema-derived namespace
          iceberg_catalog_warehouse: my_warehouse   # optional, catalog-dependent
          iceberg_catalog_credential: "client_id:client_secret"  # optional OAuth2
          iceberg_catalog_token: "my_bearer_token"  # optional, alternative to credential
          iceberg_catalog_properties:               # optional extra PyIceberg properties
            region: us-east-1
            s3.endpoint: https://minio:9000
            s3.access-key-id: minioadmin
            s3.secret-access-key: minioadmin

Any key-value pair in ``iceberg_catalog_properties`` is passed verbatim to
``pyiceberg.catalog.load_catalog()``, allowing full control over S3/GCS/ADLS
FileIO configuration, signing, and any other PyIceberg catalog option.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Dict, Any, Optional

if TYPE_CHECKING:
    import pandas as pd

from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("dremio")

# Iceberg identifiers must be valid identifiers: letters, digits, underscores.
# Replace any character that is not alphanumeric or underscore with underscore.
_INVALID_IDENTIFIER_CHARS = re.compile(r"[^a-zA-Z0-9_]")


def _sanitize_identifier(name: str) -> str:
    """Replace characters invalid in Iceberg identifiers with underscores.

    Iceberg table and namespace identifiers must not contain hyphens, spaces,
    or other special characters. dbt model names like ``my-model`` become
    ``my_model``.
    """
    return _INVALID_IDENTIFIER_CHARS.sub("_", name)


class IcebergWriter:
    """Writes a pandas DataFrame to an Iceberg table via a REST catalog.

    Parameters
    ----------
    catalog_properties : dict
        Properties passed directly to ``pyiceberg.catalog.load_catalog()``.
        Must include at minimum ``uri``. ``type`` defaults to ``"rest"``.
    catalog_name : str
        Logical name for the catalog instance (default: ``"dremio"``).
    """

    def __init__(self, catalog_properties: Dict[str, Any], catalog_name: str = "dremio"):
        self._catalog_properties = catalog_properties
        self._catalog_name = catalog_name
        self._catalog = None

    def connect(self) -> "IcebergWriter":
        """Load the PyIceberg catalog. Called once before any writes."""
        try:
            from pyiceberg.catalog import load_catalog
        except ImportError as exc:
            raise ImportError(
                "Python model Iceberg writes require pyiceberg to be installed. "
                "Run: pip install dbt-dremio[iceberg]"
            ) from exc

        logger.debug(
            f"Loading Iceberg catalog '{self._catalog_name}' "
            f"at {self._catalog_properties.get('uri')}"
        )
        self._catalog = load_catalog(self._catalog_name, **self._catalog_properties)
        return self

    def write(
        self,
        df: "pd.DataFrame",
        namespace: tuple,
        table_name: str,
        append: bool = False,
    ) -> int:
        """Write a pandas DataFrame to an Iceberg table via PyIceberg directly.

        Follows the proven pattern:
        1. Ensure namespace exists (create_namespace_if_not_exists).
        2. Drop the table if it exists and this is not an append (full overwrite).
        3. Create the table using ``pa.Schema.from_pandas(df)`` as schema.
        4. Write via ``table.overwrite(arrow_table)`` or ``table.append(arrow_table)``.

        Parameters
        ----------
        df : pd.DataFrame
        namespace : tuple
            e.g. ``("staging",)`` or ``()`` for root.
        table_name : str
        append : bool
            If True, append to existing table. If False (default), drop+recreate.

        Returns
        -------
        int  — rows written
        """
        if self._catalog is None:
            raise RuntimeError("IcebergWriter.write() called before connect().")

        if df.empty:
            logger.debug(f"IcebergWriter: empty DataFrame, skipping write to {table_name}")
            return 0

        rows = len(df)

        try:
            import pyarrow as pa
        except ImportError as exc:
            raise ImportError(
                "Python model Iceberg writes require pyarrow. "
                "Run: pip install dbt-dremio[iceberg]"
            ) from exc

        # 1. Resolve namespace — use "default" when none is specified.
        #    Most Iceberg REST catalogs (Nessie, Polaris, etc.) require at least
        #    one namespace level; a bare table identifier without namespace is
        #    not valid for REST catalog implementations.
        resolved_namespace = namespace if namespace else ("default",)
        full_name = ".".join(resolved_namespace + (table_name,))
        # Dotted string for catalog API calls
        table_id = ".".join(resolved_namespace + (table_name,))

        logger.debug(
            f"IcebergWriter: writing {rows} rows to '{full_name}' "
            f"(append={append}) via catalog '{self._catalog_name}'"
        )

        # 2. Ensure namespace exists
        try:
            self._catalog.create_namespace_if_not_exists(resolved_namespace)
            logger.debug(f"IcebergWriter: namespace {resolved_namespace} ensured.")
        except Exception as exc:
            logger.debug(f"IcebergWriter: namespace check/create: {exc}")

        # 2. Convert to Arrow using pa.Schema.from_pandas — same approach
        #    that works in production with Nessie + MinIO
        arrow_schema = pa.Schema.from_pandas(df, preserve_index=False)
        arrow_table = pa.Table.from_pandas(df, schema=arrow_schema, preserve_index=False)

        if append and self._catalog.table_exists(table_id):
            # Incremental append to existing table
            iceberg_table = self._catalog.load_table(table_id)
            iceberg_table.append(arrow_table)
            logger.debug(f"IcebergWriter: appended {rows} rows to '{full_name}'.")
        else:
            # Full overwrite: drop if exists, then recreate
            if self._catalog.table_exists(table_id):
                logger.debug(f"IcebergWriter: dropping '{full_name}' for overwrite.")
                self._catalog.drop_table(table_id)

            iceberg_table = self._catalog.create_table(table_id, schema=arrow_schema)
            iceberg_table.overwrite(arrow_table)
            logger.debug(f"IcebergWriter: created and wrote '{full_name}' ({rows} rows).")

        return rows


def build_iceberg_writer(credentials) -> IcebergWriter:
    """Build an :class:`IcebergWriter` from ``DremioCredentials``.

    Assembles the PyIceberg catalog properties dict from the profile fields:

    - ``iceberg_catalog_uri`` (required)
    - ``iceberg_catalog_warehouse`` (optional)
    - ``iceberg_catalog_credential`` (optional, OAuth2 ``client_id:secret``)
    - ``iceberg_catalog_token`` (optional, bearer token)
    - ``iceberg_catalog_properties`` (optional, dict of extra PyIceberg props)

    Any key in ``iceberg_catalog_properties`` overrides the values derived
    from the dedicated credential fields above, giving the user full control.

    Raises
    ------
    dbt_common.exceptions.DbtRuntimeError
        When ``iceberg_catalog_uri`` is not set in the profile.
    """
    import dbt_common.exceptions

    if not credentials.iceberg_catalog_uri:
        raise dbt_common.exceptions.DbtRuntimeError(
            "Python models require an Iceberg REST Catalog to be configured in the profile.\n"
            "Add 'iceberg_catalog_uri' to your profiles.yml.\n\n"
            "Minimal example (Nessie, no auth):\n"
            "  iceberg_catalog_uri: http://nessie:19120/iceberg/main\n\n"
            "Example with S3/MinIO storage:\n"
            "  iceberg_catalog_uri: http://nessie:19120/iceberg/main\n"
            "  iceberg_catalog_properties:\n"
            "    region: us-east-1\n"
            "    s3.endpoint: https://minio:9000\n"
            "    s3.access-key-id: minioadmin\n"
            "    s3.secret-access-key: minioadmin\n\n"
            "Example with OAuth2 auth (Polaris):\n"
            "  iceberg_catalog_uri: https://polaris/api/catalog\n"
            "  iceberg_catalog_credential: client_id:client_secret\n"
            "  iceberg_catalog_warehouse: my_catalog\n"
        )

    # Base properties — type defaults to rest
    props: Dict[str, Any] = {
        "type": "rest",
        "uri": credentials.iceberg_catalog_uri,
    }

    if credentials.iceberg_catalog_warehouse:
        props["warehouse"] = credentials.iceberg_catalog_warehouse

    # Authentication: prefer credential over token, but both are optional
    # (some catalogs like Nessie with no-auth need neither)
    if credentials.iceberg_catalog_credential:
        props["credential"] = credentials.iceberg_catalog_credential
    elif credentials.iceberg_catalog_token:
        props["token"] = credentials.iceberg_catalog_token

    # Merge extra properties last so they can override anything above
    if credentials.iceberg_catalog_properties:
        props.update(credentials.iceberg_catalog_properties)

    catalog_name = credentials.iceberg_catalog_name or "dremio"

    return IcebergWriter(catalog_properties=props, catalog_name=catalog_name)


def table_identifier_from_relation(
    database: str,
    schema: str,
    alias: str,
    namespace_override: Optional[str] = None,
) -> tuple:
    """Build the Iceberg (namespace_tuple, table_name) from Dremio relation components.

    All path components are sanitized: characters invalid in Iceberg identifiers
    (hyphens, spaces, etc.) are replaced with underscores.

    Parameters
    ----------
    database : str
        Dremio database / space name (not used for Iceberg namespace).
    schema : str
        Dremio schema / folder path, possibly dot-separated.
        ``"no_schema"`` is Dremio's sentinel for root-level — produces empty namespace.
    alias : str
        The model alias / table name.
    namespace_override : str, optional
        When set (from ``iceberg_catalog_namespace`` in the profile), replaces
        the schema-derived namespace entirely. Use an empty string ``""`` to
        explicitly write at the catalog root.

    Returns
    -------
    tuple[tuple[str, ...], str]
        ``(namespace_tuple, table_name)`` e.g. ``(("staging",), "my_model")``
        or ``((), "my_model")`` for root-level tables.

    Examples
    --------
    >>> table_identifier_from_relation("nessie", "staging", "my-model")
    (('staging',), 'my_model')
    >>> table_identifier_from_relation("nessie", "no_schema", "my-model")
    ((), 'my_model')
    >>> table_identifier_from_relation("n", "a.b", "m", namespace_override="ns")
    (('ns',), 'm')
    >>> table_identifier_from_relation("n", "a", "m", namespace_override="")
    ((), 'm')
    """
    table_name = _sanitize_identifier(alias)

    if namespace_override is not None:
        # Empty string → root level (no namespace)
        if namespace_override == "":
            return ((), table_name)
        ns_parts = tuple(
            _sanitize_identifier(p) for p in namespace_override.split(".") if p
        )
        return (ns_parts, table_name)

    if schema and schema != "no_schema":
        ns_parts = tuple(
            _sanitize_identifier(p) for p in schema.split(".") if p
        )
        return (ns_parts, table_name)

    # Root level — no namespace
    return ((), table_name)
