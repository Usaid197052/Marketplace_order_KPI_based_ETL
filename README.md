# Marketplace order KPI ETL

Read-only MariaDB extract of `cartlow_prod.marketplace_orders` into ClickHouse (`marketplace`), orchestrated by the existing Cartlow Airflow container.

MariaDB is never written. Every source session uses `SET SESSION TRANSACTION READ ONLY` and a consistent snapshot. Pipeline state (watermarks, delete flags, publish gates) lives in ClickHouse.

## Stack

- Source: MariaDB replica (`SELECT` only)
- Transform: PySpark (`local[*]`)
- Warehouse: ClickHouse already used by Cartlow orders (`localhost:8123`)
- Orchestration: existing `cartlow-airflow` on port 8081

Copy `.env.example` to `.env` and set MariaDB and ClickHouse values locally. `.env` is gitignored. The warehouse database is `marketplace`, not `ecommerce`.

## Layers

`raw_marketplace_orders` → `stg_marketplace_orders` → `fact_marketplace_order` → views + `kpi_monthly`

Raw/staging/fact use `ReplacingMergeTree` on `id`. Publish rebuilds `_build` tables, runs six quality tests, then `EXCHANGE TABLES`. Failed tests leave published tables and the watermark unchanged.

Incremental extract uses `updated_at` with a 1-day overlap. Hard deletes are flagged nightly (`_is_deleted`); raw history is kept.

## DAGs

| DAG | Schedule |
|---|---|
| `marketplace_orders_full_load` | manual |
| `marketplace_orders_incremental` | every 15 minutes |
| `marketplace_orders_reconcile_deletes` | 02:00 UTC |

They share Airflow pool `marketplace_orders_publish` (1 slot). Mount this repo into Cartlow Airflow using [deploy/cartlow-airflow-mount.yml](deploy/cartlow-airflow-mount.yml), then:

```bash
cd D:\Projects\Cartlow_orders
docker compose up -d
```

Create the pool if missing: `airflow pools set marketplace_orders_publish 1 "marketplace publish"`.

Run full load first. Unpause incremental and nightly after it succeeds.

## KPIs (ClickHouse views)

Completed rows only: realized GMV, commission, seller payable, AOV, take rate (`vw_certified_revenue`).

Funnel: booked checkout value, unpaid rate, paid completion rate, fail rate (`vw_checkout_funnel`), unpaid aging, in-flight pipeline.

Fulfillment: return rate, cash refunded, cycle time, physical on-time.

Sellers: active sellers, physical concentration (top 5/10/20), discount and fee attachment.

## Tests

```bash
pip install -r requirements.txt
pytest
```
