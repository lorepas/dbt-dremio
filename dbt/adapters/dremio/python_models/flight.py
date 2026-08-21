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
Arrow Flight transport for dbt Python model data fetching.

When ``flight_port`` is set in the profile, ``DremioFlightReader`` is used
instead of the REST JSON API to read upstream relation data into pandas
DataFrames.  This is dramatically faster for large datasets because:

- Data is transferred as columnar Arrow record batches (binary, not JSON)
- A single ``do_get`` call streams all rows without REST pagination
- No Python-side JSON parsing overhead

Usage (profiles.yml)::

    my_profile:
      target: dev
      outputs:
        dev:
          type: dremio
          software_host: my-dremio-host
          port: 9047
          flight_port: 32010   # ← enables Arrow Flight for Python models
          UID: my_user
          PWD: my_password
          # or:
          # pat: my_personal_access_token

For Dremio Cloud, ``flight_host`` can also be set explicitly
(defaults to ``data.dremio.cloud`` / the cloud_host value).
"""

from __future__ import annotations

from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("dremio")


class DremioFlightReader:
    """Reads Dremio query results via Apache Arrow Flight into pandas DataFrames.

    Lifecycle
    ---------
    1. Instantiate with connection parameters from ``DremioCredentials``.
    2. Call :meth:`connect` once to open the ``FlightClient`` and authenticate.
    3. Call :meth:`fetch` for each SQL query.
    4. Call :meth:`close` when done (or use as a context manager).

    Authentication
    --------------
    - **PAT** (Personal Access Token): passed directly as a bearer token.
    - **Username/password**: the session token obtained from the Dremio REST
      ``/apiv2/login`` endpoint is reused here — both REST and Flight share the
      same authentication backend.
    """

    def __init__(
        self,
        host: str,
        port: int,
        use_ssl: bool = True,
        verify_ssl: bool = True,
    ):
        self._host = host
        self._port = port
        self._use_ssl = use_ssl
        self._verify_ssl = verify_ssl
        self._client = None
        self._auth_headers: list = []

    # ------------------------------------------------------------------
    # Connection / authentication
    # ------------------------------------------------------------------

    def connect(self, token: str) -> "DremioFlightReader":
        """Open the Flight client and store the bearer token.

        Parameters
        ----------
        token:
            Either a PAT (``Bearer`` scheme) or the session token obtained
            from the Dremio REST ``/apiv2/login`` endpoint.  The caller is
            responsible for providing the correct value; this class does not
            perform REST authentication itself.
        """
        try:
            import pyarrow.flight as flight
        except ImportError as exc:
            raise ImportError(
                "Arrow Flight support requires pyarrow to be installed. "
                "Run: pip install dbt-dremio[arrow]"
            ) from exc

        scheme = "grpc+tls" if self._use_ssl else "grpc"
        location = f"{scheme}://{self._host}:{self._port}"
        logger.debug(f"Opening Arrow Flight connection to {location}")

        client_kwargs = {}
        if self._use_ssl and not self._verify_ssl:
            client_kwargs["disable_server_verification"] = True

        self._client = flight.FlightClient(location, **client_kwargs)
        # bearer token header — used on every subsequent call
        self._auth_headers = [(b"authorization", f"bearer {token}".encode("utf-8"))]
        return self

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def fetch(self, sql: str) -> "pd.DataFrame":
        """Execute a SQL query and return all results as a pandas DataFrame.

        Uses Arrow Flight ``get_flight_info`` + ``do_get`` — a single streaming
        call that returns all rows as columnar Arrow record batches without any
        REST pagination.
        """
        if self._client is None:
            raise RuntimeError(
                "DremioFlightReader.fetch() called before connect(). "
                "Call connect(token) first."
            )

        try:
            import pyarrow.flight as flight
        except ImportError as exc:
            raise ImportError(
                "Arrow Flight support requires pyarrow. Run: pip install dbt-dremio[arrow]"
            ) from exc

        options = flight.FlightCallOptions(headers=self._auth_headers)

        logger.debug(f"Arrow Flight query: {sql[:200]}{'...' if len(sql) > 200 else ''}")

        # 1. Ask Dremio to plan the query and return a ticket
        descriptor = flight.FlightDescriptor.for_command(sql.encode("utf-8"))
        flight_info = self._client.get_flight_info(descriptor, options)

        # 2. Stream the results — Dremio typically returns a single endpoint
        chunks = []
        for endpoint in flight_info.endpoints:
            reader = self._client.do_get(endpoint.ticket, options)
            # read_all() returns a pyarrow.Table; convert to pandas in one shot
            arrow_table = reader.read_all()
            chunks.append(arrow_table)

        if not chunks:
            import pandas as pd
            return pd.DataFrame()

        import pyarrow as pa
        combined = pa.concat_tables(chunks)
        df = combined.to_pandas()
        logger.debug(f"Arrow Flight fetched {len(df)} rows × {len(df.columns)} columns")
        return df

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None

    def __enter__(self) -> "DremioFlightReader":
        return self

    def __exit__(self, *_) -> None:
        self.close()


def build_flight_reader(credentials) -> Optional[DremioFlightReader]:
    """Build a :class:`DremioFlightReader` from ``DremioCredentials``.

    Returns ``None`` when ``flight_port`` is not configured, so callers can
    do a simple ``if reader:`` check before using Flight.

    Parameters
    ----------
    credentials : DremioCredentials
        The profile credentials object.
    """
    if credentials.flight_port is None:
        return None

    # Resolve the Flight host: explicit flight_host → software_host → cloud_host
    host = (
        credentials.flight_host
        or credentials.software_host
        or credentials.cloud_host
    )
    if not host:
        logger.warning(
            "flight_port is set but no host could be resolved "
            "(need software_host, cloud_host, or flight_host). "
            "Falling back to REST."
        )
        return None

    return DremioFlightReader(
        host=host,
        port=credentials.flight_port,
        use_ssl=credentials.use_ssl if credentials.use_ssl is not None else True,
        verify_ssl=credentials.verify_ssl if credentials.verify_ssl is not None else True,
    )


def get_flight_token(credentials, rest_client) -> str:
    """Derive the bearer token to use with Arrow Flight.

    - For PAT authentication: returns the PAT directly.
    - For username/password: returns the session token that the REST client
      obtained during ``/apiv2/login``.  Both REST and Flight accept the same
      token so no extra round-trip is needed.

    Parameters
    ----------
    credentials : DremioCredentials
    rest_client : DremioRestClient
        An already-started REST client (``start()`` has been called).
    """
    # PAT: use directly as bearer token
    if credentials.pat is not None:
        return credentials.pat

    # Username/password: the REST client holds the session token after login
    auth = rest_client._parameters.authentication
    token = getattr(auth, "token", None)
    if token:
        return token

    raise RuntimeError(
        "Could not obtain an authentication token for Arrow Flight. "
        "Set pat or ensure UID/PWD are configured."
    )
