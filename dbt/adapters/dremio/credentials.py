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

from dbt.adapters.contracts.connection import Credentials
from dataclasses import dataclass, field
from typing import Optional, Dict
from dbt.adapters.dremio.relation import DremioRelation
from dbt_common.exceptions import DbtValidationError


@dataclass
class DremioCredentials(Credentials):
    database: Optional[str] = None
    schema: Optional[str] = None
    environment: Optional[str] = None
    UID: Optional[str] = None
    PWD: Optional[str] = None
    pat: Optional[str] = None
    datalake: Optional[str] = None
    root_path: Optional[str] = None
    cloud_project_id: Optional[str] = None
    cloud_host: Optional[str] = None
    software_host: Optional[str] = None
    port: Optional[int] = 9047       # REST API endpoint port
    flight_port: Optional[int] = None  # Arrow Flight endpoint port (default 32010 on Dremio)
    flight_host: Optional[str] = None  # Arrow Flight host; defaults to software_host/cloud_host if not set
    use_ssl: Optional[bool] = True
    verify_ssl: Optional[bool] = True
    # Iceberg REST Catalog — required for Python model materializations.
    # Supports any Iceberg REST-compatible catalog: Nessie, Polaris, Unity, AWS Glue, etc.
    iceberg_catalog_uri: Optional[str] = None       # e.g. "http://nessie:19120/iceberg"
    iceberg_catalog_credential: Optional[str] = None  # "client_id:client_secret" or OAuth2 token
    iceberg_catalog_token: Optional[str] = None      # bearer token (alternative to credential)
    iceberg_catalog_warehouse: Optional[str] = None  # warehouse / project name
    iceberg_catalog_name: Optional[str] = None       # logical catalog name passed to load_catalog (default: "dremio")
    iceberg_catalog_namespace: Optional[str] = None  # explicit Iceberg namespace (default: derived from schema)
    iceberg_catalog_properties: Optional[Dict[str, str]] = field(default=None)  # extra PyIceberg props (e.g. region, s3.endpoint, s3.access-key-id)

    _ALIASES = {
        # Only terms on left-side will be used going forward.
        "username": "UID",  # backwards compatibility with existing profiles
        "user": "UID",
        "password": "PWD",
        "object_storage_source": "datalake",
        "object_storage_path": "root_path",
        "dremio_space": "database",
        "dremio_space_folder": "schema",
    }

    _DEFAULT_OBJECT_STORAGE_SOURCE = "$scratch"
    _SPACE_NAME_PLACEHOLDER = "@user"

    @property
    def type(self):
        return "dremio"

    @property
    def unique_field(self):
        """
        Hashed and included in anonymous telemetry to track adapter adoption.
        Pick a field that can uniquely identify one team/organization building with this adapter
        """
        return self.software_host if self.cloud_host is None else self.cloud_host

    @property
    def aliases(self):
        return self._ALIASES

    def _connection_keys(self):
        # return an iterator of keys to pretty-print in 'dbt debug'
        return (
            "cloud_host",
            "cloud_project_id",
            "software_host",
            "port",
            "flight_port",
            "flight_host",
            "use_ssl",
            "environment",
            "iceberg_catalog_uri",
            "iceberg_catalog_warehouse",
            "iceberg_catalog_name",
            # These are aliased...
            "UID",
            "root_path",
            "datalake",
            "database",
            "schema",
            # ...by these. Output these to ensure they match
            # what they alias.
            "user",  # -> UID
            "username",  # -> UID
            "object_storage_source",  # -> datalake
            "object_storage_path",  # -> root_path
            "dremio_space",  # -> database
            "dremio_space_folder",  # -> schema
        )

    @classmethod
    def __pre_deserialize__(cls, data):
        data = super().__pre_deserialize__(cls._validate_and_restructure_data(data))
        if "cloud_host" not in data:
            data["cloud_host"] = None
        if "software_host" not in data:
            data["software_host"] = None

        if "database" not in data:
            data["database"] = None
        if "schema" not in data:
            data["schema"] = None

        if "datalake" not in data:
            data["datalake"] = None
        if "root_path" not in data:
            data["root_path"] = None

        if "pat" not in data:
            data["pat"] = None

        if "environment" not in data:
            data["environment"] = None

        if "flight_port" not in data:
            data["flight_port"] = None

        if "flight_host" not in data:
            data["flight_host"] = None

        for key in (
            "iceberg_catalog_uri",
            "iceberg_catalog_credential",
            "iceberg_catalog_token",
            "iceberg_catalog_warehouse",
            "iceberg_catalog_name",
            "iceberg_catalog_namespace",
            "iceberg_catalog_properties",
        ):
            if key not in data:
                data[key] = None

        return data

    def __post_init__(self):
        if self.datalake is None:
            self.datalake = self._DEFAULT_OBJECT_STORAGE_SOURCE
        if self.root_path is None:
            self.root_path = DremioRelation.no_schema
        if self.database is None or self.database == self._SPACE_NAME_PLACEHOLDER:
            self.database = f"@{self.UID}"
        if self.schema is None:
            self.schema = DremioRelation.no_schema

    @staticmethod
    def _validate_and_restructure_data(data):
        # data parameter is the project profile configuration already aliased
        space_source_configs = ["datalake", "root_path", "database", "schema"]
        using_space_source = all(key in data for key in space_source_configs)
        enterprise_catalog_configs = ["enterprise_catalog_namespace", "enterprise_catalog_folder"]
        using_enterprise_catalog = all(key in data for key in enterprise_catalog_configs)
        if using_space_source and using_enterprise_catalog:
            raise DbtValidationError(
                "Cannot use both enterprise catalog and individual storage configurations"
            )
        # Using enterprise catalog means using it to store both tables and views
        # So internally we set it as both source and space before aliasing
        if using_enterprise_catalog:
            data["object_storage_source"] = data["enterprise_catalog_namespace"]
            data["object_storage_path"] = data["enterprise_catalog_folder"]
            data["dremio_space"] = data["enterprise_catalog_namespace"]
            data["dremio_space_folder"] = data["enterprise_catalog_folder"]
            del data["enterprise_catalog_namespace"]
            del data["enterprise_catalog_folder"]
        return data
