"""Shared helpers for the Iceberg lab: a local catalog, a timer and a synthetic orders dataset."""
import os, shutil, time, statistics
from datetime import datetime, timedelta

import numpy as np
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WAREHOUSE = os.path.join(ROOT, "warehouse")
START = datetime(2026, 10, 1)
DAYS = 30


def fresh_catalog(name="lab"):
    """A local SQLite catalog with the warehouse on the local disk. Wipes any previous run."""
    if os.path.exists(WAREHOUSE):
        shutil.rmtree(WAREHOUSE)
    os.makedirs(WAREHOUSE)
    cat = SqlCatalog(name, uri=f"sqlite:///{WAREHOUSE}/catalog.db", warehouse=f"file://{WAREHOUSE}")
    cat.create_namespace("shop")
    return cat


def timed(fn, repeats=1):
    """Run fn repeats times, return (median seconds, last result)."""
    runs, out = [], None
    for _ in range(repeats):
        t = time.perf_counter()
        out = fn()
        runs.append(time.perf_counter() - t)
    return statistics.median(runs), out


def orders(n, seed=7, start=START, days=DAYS, first_id=0):
    """n synthetic order events spread over `days` days. Includes a wide user_agent string, like real clickstream."""
    rng = np.random.default_rng(seed)
    secs = rng.integers(0, days * 86400, n)
    ts = np.array([start], dtype="datetime64[us]") + (secs * 1_000_000).astype("timedelta64[us]")
    agents = np.array([
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
        "Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Mobile Safari/537.36",
    ])
    return pa.table({
        "order_id": pa.array(np.arange(first_id, first_id + n, dtype=np.int64)),
        "customer_id": pa.array(rng.integers(1, 200_000, n, dtype=np.int64)),
        "product_id": pa.array(rng.integers(1, 2_000, n, dtype=np.int64)),
        "amount": pa.array(np.round(rng.gamma(2.0, 25.0, n), 2)),
        "status": pa.array(rng.choice(["paid", "shipped", "returned"], n, p=[0.6, 0.35, 0.05])),
        "user_agent": pa.array(rng.choice(agents, n)),
        "ts": pa.array(ts, type=pa.timestamp("us")),
    })


def dir_bytes(path):
    total = 0
    for dp, _, fs in os.walk(path):
        for f in fs:
            total += os.path.getsize(os.path.join(dp, f))
    return total


def files_on_disk(table_location, sub):
    base = table_location.replace("file://", "")
    out = []
    for dp, _, fs in os.walk(os.path.join(base, sub)):
        for f in fs:
            out.append(os.path.join(dp, f))
    return out
