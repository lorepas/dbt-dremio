**[dbt](https://www.getdbt.com/)** enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.

dbt is the T in ELT. Organize, cleanse, denormalize, filter, rename, and pre-aggregate the raw data in your warehouse so that it's ready for analysis.

## Documentation

- [Dremio docs for our dbt adapter](https://github.com/dremio/dbt-dremio/wiki/Using-Materializations-with-Dremio)
- [Walkthrough For Using dbt-dremio](./docs/walkthrough.md)
- [Trouble Shooting Guide](./docs/troubleshoot.md)

The `dbt-dremio` package contains all of the code enabling dbt to work with [Dremio](https://www.dremio.com/). For more information on using dbt with Dremio, consult [the docs](https://docs.getdbt.com/reference/warehouse-profiles/dremio-profile).

The dbt-dremio package supports both Dremio Cloud and Dremio Software (versions 22.0 and later).

## dbt-dremio version 1.10.1

Version 1.10.1 of the dbt-dremio adapter is compatible with dbt-core versions 1.10.*.

> Prior to version 1.1.0b, dbt-dremio was created and maintained by [Fabrice Etanchaud](https://github.com/fabrice-etanchaud) on [their GitHub repo](https://github.com/fabrice-etanchaud/dbt-dremio). Code for using Dremio REST APIs was originally authored by [Ryan Murray](https://github.com/rymurr). Contributors in this repo are credited for laying the groundwork and maintaining the adapter till version 1.0.6.5. The dbt-dremio adapter is maintained and distributed by Dremio starting with version 1.1.0b.

## Getting started

-   [Install dbt-dremio](https://docs.getdbt.com/reference/warehouse-setups/dremio-setup)
    -   Version 1.10.1 of dbt-dremio requires dbt-core >= 1.10.*.
-   Read the [introduction](https://docs.getdbt.com/docs/introduction/) and [viewpoint](https://docs.getdbt.com/docs/about/viewpoint/)

## Python Models

dbt-dremio supports [dbt Python models](https://docs.getdbt.com/docs/build/python-models), allowing you to write transformations in Python that run client-side and persist results as Iceberg tables via any Iceberg REST-compatible catalog (Nessie, Polaris, AWS Glue REST, Unity Catalog, etc.).

### Supported materializations

| Materialization | Supported | Notes |
|---|---|---|
| `table` | ✅ | Writes via Iceberg REST catalog using PyIceberg |
| `incremental` | ✅ | First run creates the table; subsequent runs append |
| `view` | ❌ | Not supported — Python models produce data, not SQL queries |

### Installation

```bash
pip install dbt-dremio[iceberg]
```

This installs `pandas >= 3.0`, `pyiceberg[pyarrow] >= 0.8`, and `s3fs` for S3-compatible object storage.

Optionally, enable Arrow Flight for faster upstream data reads:

```bash
pip install dbt-dremio[arrow]
```

### Profile configuration

Add the following fields to your `profiles.yml` output:

```yaml
my_profile:
  outputs:
    dev:
      type: dremio
      software_host: localhost
      port: 9047
      user: admin
      password: ...
      dremio_space: my_space
      object_storage_source: nessie   # Dremio source name for Nessie
      object_storage_path: no_schema

      # --- Iceberg REST Catalog (required for Python models) ---
      iceberg_catalog_uri: "http://nessie:19120/iceberg/main"
      iceberg_catalog_name: rest             # optional, default "dremio"
      iceberg_catalog_namespace: staging     # optional; maps to Iceberg namespace
                                             # also used to resolve ref() paths

      # Authentication (optional — omit for catalogs with no auth)
      # iceberg_catalog_credential: "client_id:client_secret"  # OAuth2
      # iceberg_catalog_token: "my_bearer_token"               # bearer token

      # Optional warehouse / project identifier
      # iceberg_catalog_warehouse: my_warehouse

      # Extra PyIceberg properties (S3/MinIO, GCS, ADLS, etc.)
      iceberg_catalog_properties:
        region: us-east-1
        s3.endpoint: http://minio:9000
        s3.access-key-id: minioadmin
        s3.secret-access-key: minioadmin
        s3.path-style-access: "true"
        # Disable Nessie credential vending to use static credentials above:
        header.X-Iceberg-Access-Delegation: ""

      # --- Arrow Flight (optional, for faster reads) ---
      # flight_port: 32010
```

### Writing a Python model

```python
# models/staging/my_model.py
def model(dbt, session):
    dbt.config(materialized="table")

    # Read upstream models or sources as pandas DataFrames
    df = dbt.ref("my_sql_model")

    # Transform with pandas
    df["new_col"] = df["amount"] * 1.1

    return df
```

The model name must use underscores (not hyphens). Hyphens are not valid Iceberg identifiers and cause `ref()` resolution failures downstream.

### Incremental Python models

```python
# models/staging/my_incremental_model.py
def model(dbt, session):
    dbt.config(materialized="incremental")

    df = dbt.ref("upstream_table")

    if dbt.is_incremental:
        # Filter to only new rows on subsequent runs
        max_ts = session.sql(f"SELECT MAX(updated_at) FROM {dbt.this}").iloc[0, 0]
        df = df[df["updated_at"] > max_ts]

    return df
```

On the first run `dbt.is_incremental` is `False` and the table is created. On subsequent runs it is `True` and rows are appended via PyIceberg.

### Referencing Python model output from SQL models

When `iceberg_catalog_namespace` is set, the Python model's table lives at `{object_storage_source}.{namespace}.{model_name}` in Dremio. To reference it from a SQL model on a Nessie source, use `branch` in the config:

```sql
-- models/staging/my_view.sql
{{ config(materialized="view", branch="main") }}

select * from {{ ref('my_model') }}
```

For `ref()` to resolve the correct path (`nessie."staging"."my_model"`), the SQL model needs the `branch` config and `iceberg_catalog_namespace` must match the Dremio source folder structure.

### How data reads work

- **Arrow Flight** (when `flight_port` is set): `dbt.ref()`, `dbt.source()`, and `session.sql()` stream data via gRPC — single call, no JSON parsing, no REST pagination. Recommended for datasets with more than a few thousand rows.
- **REST fallback**: standard Dremio REST API with JSON pagination (500 rows/request by default).

Note: Dremio Arrow Flight is **read-only** — writes always go through the Iceberg REST catalog.

---

## Join the dbt Community

-   Be part of the conversation in the [dbt Community Slack](http://community.getdbt.com/)
-   Read more on the [dbt Community Discourse](https://discourse.getdbt.com)

## Reporting bugs and contributing code

-   Open bugs and feature requests can be found at [dbt-dremio's GitHub issues](https://github.com/dremio/dbt-dremio/issues).
-   Want to report a bug or request a feature? Let us know on [Slack](https://getdbt.slack.com/archives/C049G61TKBK), or by opening [an issue](https://github.com/dremio/dbt-dremio/issues/new).
-   For direct feedback, you can also email us at [dremio-dbt-feedback@dremio.com](mailto:dremio-dbt-feedback@dremio.com).
-   Want to help us build dbt-dremio? Check out the [Contributing Guide](https://github.com/dremio/dbt-dremio/blob/main/CONTRIBUTING.md).

## Code of Conduct

Everyone interacting in the dbt-dremio project's codebases, issue trackers, chat rooms, and mailing lists is expected to follow the [dbt-dremio Code of Conduct](https://github.com/dremio/dbt-dremio/blob/main/CODE_OF_CONDUCT.md).
