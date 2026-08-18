# fabric-platform-airflow-builtin

Runs `contoso-data-product` on **fabric-emulator using Fabric's own built-in Apache Airflow** — the `ApacheAirflowJob` item backed by the real Airflow 2.10.5 sidecar the emulator ships (its E1 tier, witnessed in `fabric-emulator/e2e/airflow`).

Distinct from [`fabric-platform-airflow3`](https://github.com/calvinchengx/fabric-platform-airflow3), which orchestrates the same product from an *external* Apache Airflow 3, and from [`fabric-platform-notebook-pipelines`](https://github.com/calvinchengx/fabric-platform-notebook-pipelines), which uses Fabric notebooks and Data Pipelines. Same product, three orchestrators.

The platform installs the product and knows no Contoso.

## Status

**Not started.** This repository exists so the family index is complete;
it holds no platform yet. See the [family layout](https://github.com/calvinchengx/contoso-data-product#readme).

Apache-2.0.
