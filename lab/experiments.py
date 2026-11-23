"""Iceberg lab experiments. Run: python lab/experiments.py [--rows 2000000] [--batches 300]

Every number in results/results.json comes from this script on the machine it ran on.
"""
import argparse, json, os, shutil, sys, time
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, BucketTransform
from pyiceberg.types import (NestedField, LongType, DoubleType, StringType, TimestampType)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import fresh_catalog, timed, orders, dir_bytes, files_on_disk, WAREHOUSE, ROOT, START

SCHEMA = Schema(
    NestedField(1, "order_id", LongType(), required=False),
    NestedField(2, "customer_id", LongType(), required=False),
    NestedField(3, "product_id", LongType(), required=False),
    NestedField(4, "amount", DoubleType(), required=False),
    NestedField(5, "status", StringType(), required=False),
    NestedField(6, "user_agent", StringType(), required=False),
    NestedField(7, "ts", TimestampType(), required=False),
)
BY_DAY = PartitionSpec(PartitionField(source_id=7, field_id=1000, transform=DayTransform(), name="ts_day"))
R = {}


def log(k, v):
    R[k] = v
    print(f"{k}: {v}", flush=True)


def data_files(tbl):
    return set(tbl.inspect.files()["file_path"].to_pylist())


def health(tbl):
    """The table health check: the numbers I want on a dashboard for every table."""
    files = tbl.inspect.files()
    sizes = files["file_size_in_bytes"].to_pylist()
    referenced = set(tbl.inspect.all_files()["file_path"].to_pylist()) if tbl.snapshots() else set()
    on_disk = files_on_disk(tbl.location(), "data")
    orphans = [f for f in on_disk if "file://" + f not in referenced and f not in referenced]
    meta = files_on_disk(tbl.location(), "metadata")
    return {
        "data_files": len(sizes),
        "avg_file_kb": round(sum(sizes) / max(1, len(sizes)) / 1024, 1),
        "snapshots": len(tbl.snapshots()),
        "manifests": len(tbl.inspect.manifests()),
        "metadata_files": len(meta),
        "metadata_json_files": len([m for m in meta if m.endswith(".metadata.json")]),
        "metadata_mb": round(sum(os.path.getsize(m) for m in meta) / 1e6, 2),
        "data_files_on_disk": len(on_disk),
        "unreferenced_files": len(orphans),
        "data_on_disk_mb": round(sum(os.path.getsize(f) for f in on_disk) / 1e6, 1),
    }


def main(rows, batches, batch_rows):
    cat = fresh_catalog()
    data = orders(rows)
    log("env", {"pyiceberg": __import__("pyiceberg").__version__, "pyarrow": pa.__version__, "rows": rows,
                "cpus": os.cpu_count()})

    # ---------- Setup: one table, partitioned by day(ts) ----------
    tbl = cat.create_table("shop.orders", schema=SCHEMA, partition_spec=BY_DAY)
    t_load, _ = timed(lambda: tbl.append(data))
    good = tbl.current_snapshot().snapshot_id
    total = round(pc.sum(data["amount"]).as_py(), 2)
    log("setup", {"load_s": round(t_load, 2), "data_files": len(data_files(tbl)), "sum_amount": total})

    # ---------- E1: the undo button ----------
    # The mistake: UPDATE orders SET amount = 0 (someone forgot the WHERE clause).
    def mistake():
        cur = tbl.scan().to_arrow()
        zeroed = cur.set_column(cur.schema.get_field_index("amount"), "amount", pa.array([0.0] * cur.num_rows))
        tbl.overwrite(zeroed)
    t_mistake, _ = timed(mistake)
    after_mistake = pc.sum(tbl.scan(selected_fields=("amount",)).to_arrow()["amount"]).as_py()
    t_undo, _ = timed(lambda: tbl.manage_snapshots().rollback_to_snapshot(good).commit())
    tbl = cat.load_table("shop.orders")
    restored = round(pc.sum(tbl.scan(selected_fields=("amount",)).to_arrow()["amount"]).as_py(), 2)
    # What a plain data lake would do instead: restore by rewriting every file from a backup copy.
    lake = os.path.join(WAREHOUSE, "plain_lake_restore")
    t_restore, _ = timed(lambda: ds.write_dataset(data, lake, format="parquet",
                                                  existing_data_behavior="overwrite_or_ignore"))
    log("e1_undo", {"mistake_s": round(t_mistake, 2), "sum_after_mistake": after_mistake,
                    "rollback_s": round(t_undo, 4), "sum_after_rollback": restored, "sum_original": total,
                    "restored_ok": abs(restored - total) < 0.01,
                    "plain_lake_rewrite_s": round(t_restore, 2), "plain_lake_rewrite_mb": round(dir_bytes(lake) / 1e6, 1),
                    "speedup_vs_rewrite": round(t_restore / t_undo, 0)})
    shutil.rmtree(lake)

    # ---------- E2: the bad pipeline run (find exactly what one run changed) ----------
    before_bad = tbl.current_snapshot().snapshot_id
    day = data.filter(pc.and_(pc.greater_equal(data["ts"], pa.scalar(START + timedelta(days=12), pa.timestamp("us"))),
                              pc.less(data["ts"], pa.scalar(START + timedelta(days=13), pa.timestamp("us")))))
    replay = day.slice(0, 6000)   # a retried job re-sends 6,000 events it had already written: false duplicates
    tbl.append(replay)
    bad = tbl.current_snapshot().snapshot_id
    t_find, found = timed(lambda: tbl.incremental_append_scan(from_snapshot_id_exclusive=before_bad,
                                                              to_snapshot_id_inclusive=bad).to_arrow())
    now_rows = tbl.scan(selected_fields=("order_id",)).to_arrow().num_rows
    then_rows = tbl.scan(selected_fields=("order_id",), snapshot_id=before_bad).to_arrow().num_rows
    dup_ids = len(set(found["order_id"].to_pylist()) & set(day["order_id"].to_pylist()))
    t_fix, _ = timed(lambda: tbl.manage_snapshots().rollback_to_snapshot(before_bad).commit())
    tbl = cat.load_table("shop.orders")
    log("e2_bad_run", {"false_rows_added": replay.num_rows, "rows_found_by_snapshot_diff": found.num_rows,
                       "found_are_duplicates": dup_ids == found.num_rows, "find_s": round(t_find, 3),
                       "rows_now": now_rows, "rows_as_of_before": then_rows,
                       "fix_rollback_s": round(t_fix, 4), "rows_after_fix": tbl.scan(selected_fields=("order_id",)).to_arrow().num_rows})

    # ---------- E3: hidden partitioning (filter on the timestamp, prune by day) ----------
    lo, hi = START + timedelta(days=20), START + timedelta(days=21)
    flt = And(GreaterThanOrEqual("ts", lo.isoformat()), LessThan("ts", hi.isoformat()))
    planned = len(list(tbl.scan(row_filter=flt).plan_files()))
    all_files = len(list(tbl.scan().plan_files()))
    t_ice, got = timed(lambda: tbl.scan(row_filter=flt).to_arrow(), repeats=3)
    # Plain Parquet lake, partitioned by a separate dt string column (the classic Hive layout)
    hive = os.path.join(WAREHOUSE, "plain_hive")
    with_dt = data.append_column("dt", pc.strftime(data["ts"], format="%Y-%m-%d"))
    ds.write_dataset(with_dt, hive, format="parquet", partitioning=["dt"], partitioning_flavor="hive")
    dset = ds.dataset(hive, format="parquet", partitioning="hive")
    lo_s, hi_s = pa.scalar(lo, pa.timestamp("us")), pa.scalar(hi, pa.timestamp("us"))
    expr = (ds.field("ts") >= lo_s) & (ds.field("ts") < hi_s)
    frags_ts = len(list(dset.get_fragments(filter=expr)))
    frags_dt = len(list(dset.get_fragments(filter=ds.field("dt") == lo.strftime("%Y-%m-%d"))))
    t_hive, got_h = timed(lambda: dset.to_table(filter=expr), repeats=3)
    log("e3_hidden_partitioning", {"iceberg_files_planned": planned, "iceberg_files_total": all_files,
                                   "iceberg_s": round(t_ice, 3), "rows": got.num_rows,
                                   "hive_files_opened_filter_on_ts": frags_ts, "hive_files_opened_filter_on_dt": frags_dt,
                                   "hive_files_total": len(list(dset.get_fragments())),
                                   "hive_s": round(t_hive, 3), "hive_rows": got_h.num_rows})
    shutil.rmtree(hive)

    # ---------- E4 + E5: schema and partition evolution on a copy ----------
    small = cat.create_table("shop.orders_evo", schema=SCHEMA, partition_spec=BY_DAY)
    small.append(data.slice(0, 200_000))
    before = data_files(small)
    def evolve_schema():
        with small.update_schema() as u:
            u.add_column("discount", DoubleType())
            u.rename_column("amount", "total_amount")
    t_schema, _ = timed(evolve_schema)
    small = cat.load_table("shop.orders_evo")
    after = data_files(small)
    sample = small.scan(selected_fields=("total_amount", "discount"), limit=3).to_arrow()
    def evolve_spec():
        with small.update_spec() as s:
            s.add_field("customer_id", BucketTransform(16), "customer_bucket")
    t_spec, _ = timed(evolve_spec)
    small = cat.load_table("shop.orders_evo")
    after_spec = data_files(small)
    new = orders(20_000, seed=11, first_id=10_000_000).rename_columns(
        ["order_id", "customer_id", "product_id", "total_amount", "status", "user_agent", "ts"])
    new = new.append_column("discount", pa.array([0.1] * new.num_rows))
    small.append(new)
    parts = small.inspect.partitions()
    log("e4_schema_evolution", {"seconds": round(t_schema, 4), "data_files_before": len(before),
                                "data_files_rewritten": len(before - after), "old_rows_discount": sample["discount"].to_pylist()})
    log("e5_partition_evolution", {"seconds": round(t_spec, 4), "data_files_rewritten": len(before - after_spec),
                                   "specs": len(small.specs()), "partitions_after_new_data": parts.num_rows})

    # ---------- E6 + E7: small files, compaction and the maintenance bill ----------
    stream = cat.create_table("shop.events_stream", schema=SCHEMA, partition_spec=BY_DAY)
    src = orders(batches * batch_rows, seed=21, days=3)
    t0 = time.perf_counter()
    for i in range(batches):
        stream.append(src.slice(i * batch_rows, batch_rows))
    t_stream = time.perf_counter() - t0
    h_small = health(stream)
    t_plan_small, _ = timed(lambda: list(stream.scan().plan_files()), repeats=3)
    t_read_small, _ = timed(lambda: stream.scan().to_arrow(), repeats=3)

    t_compact, _ = timed(lambda: stream.overwrite(stream.scan().to_arrow()))   # bin-pack: read all, write few big files
    stream = cat.load_table("shop.events_stream")
    h_compacted = health(stream)
    t_plan_big, _ = timed(lambda: list(stream.scan().plan_files()), repeats=3)
    t_read_big, _ = timed(lambda: stream.scan().to_arrow(), repeats=3)

    current = stream.current_snapshot().snapshot_id
    old = [s.snapshot_id for s in stream.snapshots() if s.snapshot_id != current]
    t_expire, _ = timed(lambda: stream.maintenance.expire_snapshots().by_ids(old).commit())
    stream = cat.load_table("shop.events_stream")
    h_expired = health(stream)
    referenced = set(stream.inspect.all_files()["file_path"].to_pylist())
    orphans = [f for f in files_on_disk(stream.location(), "data") if "file://" + f not in referenced and f not in referenced]
    freed = sum(os.path.getsize(f) for f in orphans)
    for f in orphans:
        os.remove(f)
    h_clean = health(stream)
    log("e6_small_files", {"batches": batches, "rows_per_batch": batch_rows, "append_loop_s": round(t_stream, 1),
                           "plan_s_small": round(t_plan_small, 3), "read_s_small": round(t_read_small, 3),
                           "compact_s": round(t_compact, 2),
                           "plan_s_compacted": round(t_plan_big, 4), "read_s_compacted": round(t_read_big, 3)})
    log("e7_maintenance", {"health_small_files": h_small, "health_after_compaction": h_compacted,
                           "health_after_expire": h_expired, "health_after_orphan_cleanup": h_clean,
                           "expire_s": round(t_expire, 3), "orphan_files_deleted": len(orphans),
                           "orphan_mb_freed": round(freed / 1e6, 1)})

    # ---------- E8: the same stream with metadata retention switched on ----------
    kept = cat.create_table("shop.events_stream_tuned", schema=SCHEMA, partition_spec=BY_DAY,
                            properties={"write.metadata.delete-after-commit.enabled": "true",
                                        "write.metadata.previous-versions-max": "10"})
    for i in range(batches):
        kept.append(src.slice(i * batch_rows, batch_rows))
    h_kept = health(kept)
    log("e8_metadata_retention", {"default_metadata_json_files": h_small["metadata_json_files"],
                                  "default_metadata_mb": h_small["metadata_mb"],
                                  "tuned_metadata_json_files": h_kept["metadata_json_files"],
                                  "tuned_metadata_mb": h_kept["metadata_mb"],
                                  "data_mb": h_small["data_on_disk_mb"]})

    os.makedirs(os.path.join(ROOT, "results"), exist_ok=True)
    with open(os.path.join(ROOT, "results", "results.json"), "w") as f:
        json.dump(R, f, indent=2, default=str)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=2_000_000)
    ap.add_argument("--batches", type=int, default=300)
    ap.add_argument("--batch-rows", type=int, default=2_000)
    a = ap.parse_args()
    main(a.rows, a.batches, a.batch_rows)
