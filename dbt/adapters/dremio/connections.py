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

import agate
from typing import Any, Dict, Tuple, Optional, List
from contextlib import contextmanager

from dbt.adapters.base.query_headers import MacroQueryStringSetter

from dbt.adapters.dremio.__version__ import version
from dbt.adapters.dremio.api.cursor import DremioCursor
from dbt.adapters.dremio.api.handle import DremioHandle
from dbt.adapters.dremio.api.parameters import ParametersBuilder
from dbt.adapters.dremio.api.rest.entities.reflection import ReflectionEntity
from dbt.adapters.dremio.relation import DremioRelation

from dbt_common.clients import agate_helper

import time
import json

import dbt_common.exceptions
from dbt.adapters.sql import SQLConnectionManager
from dbt.adapters.contracts.connection import AdapterResponse, DEFAULT_QUERY_COMMENT

from dbt.adapters.dremio.api.rest.client import DremioRestClient

from dbt.adapters.dremio.api.rest.error import (
    DremioAlreadyExistsException,
    DremioNotFoundException,
    DremioRequestTimeoutException,
    DremioTooManyRequestsException,
    DremioInternalServerException,
    DremioServiceUnavailableException,
    DremioGatewayTimeoutException,
    DremioBadRequestException,
)

from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("dremio")

DREMIO_QUERY_COMMENT = f"""
{{%- set comment_dict = {{}} -%}}
{{%- do comment_dict.update(
    app='dbt',
    dbt_version=dbt_version,
    dbt_dremio_version='{version}',
    profile_name=target.get('profile_name'),
    target_name=target.get('target_name')
) -%}}
{{%- if node is not none -%}}
  {{%- do comment_dict.update(
    node_id=node.unique_id,
  ) -%}}
{{% else %}}
  {{# in the node context, the connection name is the node_id #}}
  {{%- do comment_dict.update(connection_name=connection_name) -%}}
{{%- endif -%}}
{{{{ return(tojson(comment_dict)) }}}}
"""

class DremioMacroQueryStringSetter(MacroQueryStringSetter):
    # Overriding this method to update the query comment macro
    def _get_comment_macro(self) -> Optional[str]:
        if self.config.query_comment.comment == DEFAULT_QUERY_COMMENT:
            return DREMIO_QUERY_COMMENT
        else:
            return self.config.query_comment.comment

class DremioConnectionManager(SQLConnectionManager):
    TYPE = "dremio"
    DEFAULT_CONNECTION_RETRIES = 5

    retries = DEFAULT_CONNECTION_RETRIES

    def set_query_header(self, query_header_context: Dict[str, Any]) -> None:
        self.query_header = DremioMacroQueryStringSetter(self.profile, query_header_context)

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except Exception as e:
            logger.debug(f"Error running SQL: {sql}")
            self.release()
            if isinstance(e, dbt_common.exceptions.DbtRuntimeError):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise dbt_common.exceptions.DbtRuntimeError(e)

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = connection.credentials
        parameters_builder = ParametersBuilder.build(credentials)
        api_parameters = parameters_builder.get_parameters()

        def connect():
            handle = DremioHandle(api_parameters)
            _ = handle.cursor()
            connection.state = "open"
            connection.handle = handle
            logger.debug(f"Connected to db: {credentials.database}")
            return handle

        retryable_exceptions = [
            # list of retryable_exceptions underlying driver might expose
            DremioRequestTimeoutException,
            DremioTooManyRequestsException,
            DremioInternalServerException,
            DremioServiceUnavailableException,
            DremioGatewayTimeoutException,
        ]

        def exponential_backoff_retry_timeout(retries: int) -> int:
            BASE = 2  # multiplicative factor
            time_delay = pow(BASE, retries)
            return time_delay

        return cls.retry_connection(
            connection,
            connect=connect,
            logger=logger,
            retry_limit=cls.retries,
            retry_timeout=exponential_backoff_retry_timeout,
            retryable_exceptions=retryable_exceptions,
        )

    @classmethod
    def is_cancelable(cls) -> bool:
        return True

    def cancel(self, connection):
        return connection.handle.cursor.job_cancel()

    def commit(self, *args, **kwargs):
        pass

    def rollback(self, *args, **kwargs):
        pass

    def add_begin_query(self):
        pass

    def add_commit_query(self):
        pass

    # Auto_begin may not be relevant with the rest_api
    def add_query(
        self, sql, auto_begin=True, bindings=None, abridge_sql_log=False,
        fetch=False
    ):
        connection = self.get_thread_connection()
        if auto_begin and connection.transaction_open is False:
            self.begin()

        logger.debug(
            f'Using {self.TYPE} connection "{connection.name}". fetch={fetch}')

        with self.exception_handler(sql):
            if abridge_sql_log:
                logger.debug(
                    "On {}: {}....".format(connection.name, sql[0:512]))
            else:
                logger.debug("On {}: {}".format(connection.name, sql))

            pre = time.time()
            cursor = connection.handle.cursor()

            if bindings is None:
                cursor.execute(sql, fetch=fetch)
            else:
                logger.debug(f"Bindings: {bindings}")
                cursor.execute(sql, bindings, fetch=fetch)

            logger.debug(
                "SQL status: {} in {:0.2f} seconds".format(
                    self.get_response(cursor), (time.time() - pre)
                )
            )
            return connection, cursor

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    @classmethod
    def get_response(cls, cursor: DremioCursor) -> AdapterResponse:
        rows = cursor.rowcount
        message = "OK" if rows == -1 else str(rows)
        return AdapterResponse(_message=message, rows_affected=rows)

    @classmethod
    def data_type_code_to_name(cls, type_code) -> str:
        return type_code

    def execute(
            self,
            sql: str,
            auto_begin: bool = False,
            fetch: bool = False,
            limit: Optional[int] = None,
    ) -> Tuple[AdapterResponse, agate.Table]:
        sql = self._add_query_comment(sql)
        _, cursor = self.add_query(sql, auto_begin, fetch=fetch)
        response = self.get_response(cursor)
        if fetch:
            table = cursor.table
        else:
            table = agate_helper.empty_table()

        return response, table

    def drop_catalog(self, database, schema):
        logger.debug('Dropping schema "{}.{}"', database, schema)

        thread_connection = self.get_thread_connection()
        connection = self.open(thread_connection)
        credentials = connection.credentials
        rest_client = connection.handle.get_client()

        path_list = self._create_path_list(database, schema)
        if database != credentials.datalake:
            try:
                catalog_info = rest_client.get_catalog_item(
                    catalog_id=None,
                    catalog_path=path_list,
                )
            except DremioNotFoundException:
                logger.debug("Catalog not found. Returning")
                return

            rest_client.delete_catalog(catalog_info["id"])

    def create_catalog(self, relation):
        thread_connection = self.get_thread_connection()
        connection = self.open(thread_connection)
        credentials = connection.credentials
        rest_client = connection.handle.get_client()
        database = relation.database
        schema = relation.schema

        if database == ("@" + credentials.UID) or self._catalog_exists(
            database):
            logger.debug(
                "Database is default or already exists: creating folders only")
        else:
            logger.debug(f"Creating space: {database}")
            self._create_space(database, rest_client)

        if database != credentials.datalake:
            logger.debug(f"Creating folder(s): {database}.{schema}")
            self._create_folders(database, schema, rest_client)
        return

    def _catalog_exists(self, path: str):
        thread_connection = self.get_thread_connection()
        connection = self.open(thread_connection)
        rest_client = connection.handle.get_client()
        return self._catalog_item_exists([path], rest_client)

    def _catalog_item_exists(self, path_list, rest_client: DremioRestClient):
        try:
            catalog_info = rest_client.get_catalog_item(
                catalog_id=None,
                catalog_path=path_list,
            )
            return catalog_info.get("id") is not None
        except DremioNotFoundException:
            return False

    # dbt docs integration with Dremio wikis and tags
    def process_wikis(self, relation, text: str):
        logger.debug("Integrating wikis")
        thread_connection = self.get_thread_connection()
        connection = self.open(thread_connection)
        rest_client = connection.handle.get_client()
        database = relation.database
        schema = relation.schema

        path = self._create_path_list(database, schema)
        identifier = relation.identifier
        path.append(identifier)
        try:
            catalog_info = rest_client.get_catalog_item(
                catalog_id=None,
                catalog_path=path,
            )
        except DremioNotFoundException:
            logger.debug("Catalog not found. Returning")
            return

        object_id = catalog_info.get("id")
        stored_wiki = rest_client.retrieve_wiki(object_id)
        wiki_content = stored_wiki.get("text")
        wiki_version = stored_wiki.get("version", None)

        if wiki_version is None:
            logger.debug(f"Creating wiki for {'.'.join(path)}")
            result = rest_client.create_wiki(object_id, text)
            logger.debug(result)
            return
        
        if wiki_content != text:
            if text == "": # text is empty, delete wiki
                logger.debug(f"Deleting wiki for {'.'.join(path)}")
                result = rest_client.delete_wiki(object_id, wiki_version)
                logger.debug(result)
                return
            
            logger.debug(f"Updating wiki for {'.'.join(path)}")
            result = rest_client.update_wiki(object_id, text, wiki_version)
            logger.debug(result)

    def process_tags(self, relation, tags: list[str]):
        logger.debug("Integrating tags")
        thread_connection = self.get_thread_connection()
        connection = self.open(thread_connection)
        rest_client = connection.handle.get_client()
        database = relation.database
        schema = relation.schema

        path = self._create_path_list(database,schema)
        identifier = relation.identifier
        path.append(identifier)
        try:
            catalog_info = rest_client.get_catalog_item(
                catalog_id=None,
                catalog_path=path,
            )
        except DremioNotFoundException:
            logger.debug("Catalog not found. Returning")
            return

        object_id = catalog_info.get("id")
        stored_tags = rest_client.retrieve_tags(object_id)
        tags_list = stored_tags.get("tags")
        tags_version = stored_tags.get("version", None)

        if tags_version is None:
            logger.debug(f"Creating tags for {'.'.join(path)}")
            result = rest_client.create_tags(object_id, tags)
            logger.debug(result)
            return

        if tags_list != tags:
            if tags == []:  # tags is empty, delete tags
                logger.debug(f"Deleting tags for {'.'.join(path)}")
                result = rest_client.delete_tags(object_id, tags_version)
                logger.debug(result)
                return

            logger.debug(f"Updating tags for {'.'.join(path)}")
            result = rest_client.update_tags(object_id, tags, tags_version)
            logger.debug(result)


    def create_reflection(self, name: str, reflection_type: str, anchor: DremioRelation, display: List[str],
                          dimensions: List[str],
                          date_dimensions: List[str], measures: List[str],
                          computations: List[str], partition_by: List[str], partition_transform: List[str],
                          partition_method: str, distribute_by: List[str], localsort_by: List[str],
                          arrow_cache: bool) -> None:
        thread_connection = self.get_thread_connection()
        connection = self.open(thread_connection)
        rest_client = connection.handle.get_client()

        database = anchor.database
        schema = anchor.schema
        path = self._create_path_list(database, schema)
        identifier = anchor.identifier

        path.append(identifier)

        catalog_info = rest_client.get_catalog_item(
            catalog_id=None,
            catalog_path=path,
        )

        dataset_id = catalog_info.get("id")

        payload = ReflectionEntity(name, reflection_type, dataset_id, display, dimensions, date_dimensions, measures,
                                   computations, partition_by, partition_transform, partition_method, distribute_by,
                                   localsort_by, arrow_cache).build_payload()

        dataset_info = rest_client.get_reflections(dataset_id)
        reflections_info = dataset_info.get("data")

        updated = False
        for reflection in reflections_info:
            if reflection.get("name") == name:
                logger.debug(f"Reflection {name} already exists. Updating it")
                payload["tag"] = reflection.get("tag")
                rest_client.update_reflection(reflection.get("id"), payload)
                updated = True
                break

        if not updated:
            logger.debug(f"Reflection {name} does not exist. Creating it")
            rest_client.create_reflection(payload)

    def _make_new_space_json(self, name) -> json:
        python_dict = {"entityType": "space", "name": name}
        return json.dumps(python_dict)

    def _make_new_folder_json(self, path) -> json:
        python_dict = {"entityType": "folder", "path": path}
        return json.dumps(python_dict)

    def _create_space(self, database, rest_client: DremioRestClient):
        space_json = self._make_new_space_json(database)
        try:
            rest_client.create_catalog_api(space_json)
        except DremioAlreadyExistsException:
            logger.debug(
                f"Database {database} already exists. Creating folders only.")

    def _create_folders(self, database, schema, rest_client: DremioRestClient):
        temp_path_list = [database]
        for folder in schema.split("."):
            temp_path_list.append(folder)
            folder_path = list(temp_path_list)
            if self._catalog_item_exists(folder_path, rest_client):
                logger.debug(f"Folder {folder} already exists.")
                continue

            folder_json = self._make_new_folder_json(folder_path)
            try:
                rest_client.create_catalog_api(folder_json)
            except DremioAlreadyExistsException:
                logger.debug(f"Folder {folder} already exists.")
            except DremioBadRequestException as e:
                if "Can not create a folder inside a [SOURCE]" in e.message:
                    logger.debug(f"Ignoring {e}")
                else:
                    raise e

    def _create_path_list(self, database, schema):
        path = [database]
        if schema != 'no_schema':
            folders = schema.split(".")
            path.extend(folders)
        return path

    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # Python model execution
    # ------------------------------------------------------------------

    def execute_python_job(
        self,
        parsed_model: dict,
        compiled_code: str,
        resolved_refs: Optional[Dict[str, str]] = None,
        resolved_sources: Optional[Dict[tuple, str]] = None,
    ) -> Tuple[int, str]:
        """Execute a Python model client-side and persist the result via Iceberg.

        Flow
        ----
        1. Validates that ``materialized`` is ``table`` or ``incremental``.
        2. Validates that ``iceberg_catalog_uri`` is set in the profile.
        3. Fetches upstream data via Arrow Flight (if configured) or REST.
        4. Runs the user's ``model(dbt, session)`` function in-process.
        5. Writes the resulting pandas DataFrame to Iceberg via PyIceberg
           using the configured REST catalog.

        Returns
        -------
        (rows_written, table_identifier)
        """
        try:
            import pandas as pd  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "Python models require pandas to be installed. "
                "Run: pip install dbt-dremio[iceberg]"
            ) from exc

        from dbt.adapters.dremio.python_models.context import (
            DbtDremioContext,
            DremioSession,
        )
        from dbt.adapters.dremio.python_models.iceberg_writer import (
            build_iceberg_writer,
            table_identifier_from_relation,
        )

        thread_connection = self.get_thread_connection()
        connection = self.open(thread_connection)
        credentials = connection.credentials

        config_dict = parsed_model.get("config", {})
        materialization = config_dict.get("materialized", "table")
        database = parsed_model.get("database", "")
        schema = parsed_model.get("schema", "")
        alias = parsed_model.get("alias", parsed_model.get("name", ""))

        # Only table and incremental are supported for Python models
        if materialization not in ("table", "incremental"):
            raise dbt_common.exceptions.DbtRuntimeError(
                f"Python model '{alias}' uses materialized='{materialization}', "
                "which is not supported for Python models in Dremio. "
                "Supported materializations: 'table', 'incremental'."
            )

        # Build the Iceberg (namespace, table_name) tuple
        # iceberg_catalog_namespace in the profile overrides the schema-derived namespace
        iceberg_namespace_override = getattr(credentials, "iceberg_catalog_namespace", None)
        iceberg_namespace, iceberg_table_name = table_identifier_from_relation(
            database,
            schema,
            alias,
            namespace_override=iceberg_namespace_override,
        )
        # Human-readable for logging
        full_table_id = ".".join(iceberg_namespace + (iceberg_table_name,)) if iceberg_namespace else iceberg_table_name

        # Build the Dremio-quoted relation string for dbt.this and for ref() resolution.
        # When iceberg_catalog_namespace is set, use it as the schema component so that
        # ref('my_python_model') resolves to the correct Nessie path:
        #   "nessie"."test"."my_transform_python"
        # instead of the bare:
        #   "nessie"."my_transform_python"
        if iceberg_namespace_override:
            target_relation = self._build_relation_str(database, iceberg_namespace_override, alias)
        else:
            target_relation = self._build_relation_str(database, schema, alias)

        # ---- Validate Iceberg catalog config eagerly ----
        # build_iceberg_writer raises DbtRuntimeError if iceberg_catalog_uri is missing
        iceberg_writer = build_iceberg_writer(credentials)
        iceberg_writer.connect()

        # ---- Determine incremental state ----
        # For incremental models, the table exists when it has been materialized
        # at least once. We probe the catalog to check.
        is_incremental = False
        if materialization == "incremental" and not self._should_full_refresh(config_dict):
            is_incremental = self._iceberg_table_exists(
                iceberg_writer, iceberg_namespace, iceberg_table_name
            )

        # ---- Build ref/source mappings ----
        refs_map = resolved_refs if resolved_refs is not None else self._build_refs_map(parsed_model)
        sources_map = resolved_sources if resolved_sources is not None else self._build_sources_map(parsed_model)

        # ---- Choose fetch transport: Arrow Flight or REST ----
        fetch_fn, _ = self._build_fetch_fn(connection, credentials)

        dbt_ctx = DbtDremioContext(
            model_config=config_dict,
            refs=refs_map,
            sources=sources_map,
            this=target_relation,
            is_incremental=is_incremental,
            fetch_fn=fetch_fn,
        )
        session = DremioSession(fetch_fn=fetch_fn)

        # ---- Execute the user function ----
        result_df = self._run_python_model(compiled_code, dbt_ctx, session)

        if result_df is None:
            raise dbt_common.exceptions.DbtRuntimeError(
                f"Python model '{alias}' did not return a DataFrame. "
                "The model() function must return a pandas DataFrame."
            )

        import pandas as pd
        if not isinstance(result_df, pd.DataFrame):
            raise dbt_common.exceptions.DbtRuntimeError(
                f"Python model '{alias}' returned a {type(result_df).__name__} "
                "instead of a pandas DataFrame."
            )

        rows_written = len(result_df)
        logger.debug(
            f"Python model '{alias}' produced {rows_written} rows. "
            f"Writing to Iceberg table '{full_table_id}' "
            f"(append={is_incremental})."
        )

        # ---- Write via Iceberg REST catalog ----
        # table  → always overwrite (append=False)
        # incremental, first run  → is_incremental=False → creates the table
        # incremental, subsequent → is_incremental=True  → appends
        iceberg_writer.write(
            result_df, iceberg_namespace, iceberg_table_name, append=is_incremental
        )

        return rows_written, full_table_id

    # ------------------------------------------------------------------
    # Python model helpers
    # ------------------------------------------------------------------

    def _build_fetch_fn(self, connection, credentials):
        """Return the best fetch function for reading upstream data into pandas.

        Prefers Arrow Flight when ``flight_port`` is configured — single
        streaming ``do_get`` call, no JSON parsing, no REST pagination.
        Falls back to the REST API silently on any connection error.

        Returns
        -------
        tuple[callable, DremioFlightReader | None]
            The fetch function and the open reader (for reuse or None).
        """
        from dbt.adapters.dremio.python_models.flight import (
            build_flight_reader,
            get_flight_token,
        )

        reader = build_flight_reader(credentials)

        if reader is not None:
            try:
                token = get_flight_token(credentials, connection.handle.get_client())
                reader.connect(token)
                logger.debug(
                    f"Arrow Flight enabled (reads + DML writes) — "
                    f"{credentials.flight_host or credentials.software_host or credentials.cloud_host}"
                    f":{credentials.flight_port}"
                )

                def flight_fetch_fn(sql: str) -> "pd.DataFrame":
                    return reader.fetch(sql)

                return flight_fetch_fn, reader

            except Exception as exc:
                logger.warning(
                    f"Arrow Flight connection failed ({exc}). "
                    "Falling back to REST API."
                )
                try:
                    reader.close()
                except Exception:
                    pass

        # REST fallback
        logger.debug("Using REST API for Python model fetch and write.")

        def rest_fetch_fn(sql: str) -> "pd.DataFrame":
            import pandas as pd

            _, cursor = self.add_query(sql, fetch=True)
            job_results = cursor.job_results()
            rows = job_results.get("rows", [])
            schema_info = job_results.get("schema", [])
            columns = [col["name"] for col in schema_info]
            return pd.DataFrame(rows, columns=columns) if rows else pd.DataFrame(columns=columns)

        return rest_fetch_fn, None

    @staticmethod
    def _run_python_model(compiled_code: str, dbt_ctx, session) -> "pd.DataFrame":
        """Compile and execute the user's Python model code, returning the DataFrame."""
        module_globals: Dict[str, Any] = {}
        exec(compiled_code, module_globals)  # nosec B102

        model_fn = module_globals.get("model")
        if model_fn is None or not callable(model_fn):
            raise dbt_common.exceptions.DbtRuntimeError(
                "Python model code must define a callable named 'model(dbt, session)'."
            )
        return model_fn(dbt_ctx, session)

    @staticmethod
    def _should_full_refresh(config_dict: dict) -> bool:
        """Return True when the model config requests a full refresh.

        Mirrors dbt-core's ``should_full_refresh()`` Jinja macro: a model-level
        ``full_refresh: true`` config always wins; ``full_refresh: false`` always
        prevents it. When absent, we default to False (i.e. respect incremental
        semantics) — the actual ``--full-refresh`` CLI flag is already folded
        into ``config_dict`` by dbt-core before ``submit_python_job`` is called.
        """
        return bool(config_dict.get("full_refresh", False))

    @staticmethod
    def _iceberg_table_exists(iceberg_writer, namespace: tuple, table_name: str) -> bool:
        """Return True when the Iceberg table already exists in the catalog."""
        try:
            table_id = namespace + (table_name,) if namespace else (table_name,)
            iceberg_writer._catalog.load_table(table_id)
            return True
        except Exception:
            return False

    def _build_relation_str(self, database: str, schema: str, identifier: str) -> str:
        """Return a quoted fully-qualified relation string for Dremio."""
        def q(s: str) -> str:
            return f'"{s}"'

        parts = [q(database)]
        if schema and schema != "no_schema":
            for folder in schema.split("."):
                parts.append(q(folder))
        parts.append(q(identifier))
        return ".".join(parts)

    def _build_refs_map(self, parsed_model: dict) -> Dict[str, str]:
        """Build a mapping of {ref_name: quoted_relation_string} from parsed_model."""
        refs_map: Dict[str, str] = {}
        refs = parsed_model.get("refs", [])
        for ref in refs:
            # refs is a list of RefArgs-like dicts: {"name": ..., "package": ..., "version": ...}
            if isinstance(ref, dict):
                name = ref.get("name", "")
            elif hasattr(ref, "name"):
                name = ref.name
            else:
                name = str(ref)
            if name:
                # We resolve the relation at query time by passing a SELECT statement;
                # the full catalog path is derived from depends_on_nodes if available,
                # otherwise we quote and use the name as-is and let Dremio resolve it.
                refs_map[name] = f'"{name}"'

        # Enrich with relation_name from depends_on info if available
        # (dbt-core populates relation_name on upstream nodes during compilation)
        nodes = parsed_model.get("nodes", {})
        for node_id, node_info in nodes.items():
            if isinstance(node_info, dict):
                node_name = node_info.get("alias") or node_info.get("name", "")
                relation_name = node_info.get("relation_name")
                if node_name and relation_name:
                    refs_map[node_name] = relation_name

        return refs_map

    def _build_sources_map(self, parsed_model: dict) -> Dict[tuple, str]:
        """Build a mapping of {(source_name, table_name): quoted_relation_string}."""
        sources_map: Dict[tuple, str] = {}
        sources = parsed_model.get("sources", [])
        for src in sources:
            # sources is a list of [source_name, table_name]
            if isinstance(src, (list, tuple)) and len(src) >= 2:
                source_name, table_name = src[0], src[1]
                sources_map[(source_name, table_name)] = f'"{source_name}"."{table_name}"'
        return sources_map
