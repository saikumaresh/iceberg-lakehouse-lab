"""Builds iceberg_basics_to_production.ipynb. Run: python build_notebook.py && jupyter nbconvert --to notebook --execute --inplace iceberg_basics_to_production.ipynb"""
import nbformat as nbf

nb = nbf.v4.new_notebook()
C = []
md = lambda s: C.append(nbf.v4.new_markdown_cell(s.strip()))
code = lambda s: C.append(nbf.v4.new_code_cell(s.strip()))

md("""
# Apache Iceberg: from basics to production

A hands-on walk through Apache Iceberg with **PyIceberg**: no Spark, no Docker, no cloud account. Everything runs on your laptop against a local folder.

You will:

1. Create an Iceberg table and look at what it writes to disk
2. Undo a disaster in milliseconds (rollback)
3. Find exactly what one bad pipeline run changed (snapshot diff)
4. Filter on a timestamp and let Iceberg prune by day (hidden partitioning)
5. Change the schema and the partitioning without rewriting data
6. Create the small files problem on purpose, then fix it (compaction)
7. Pay the maintenance bill: expire snapshots, clean orphan files, cap metadata

This notebook uses 200,000 rows so it runs in about a minute. `lab/experiments.py` runs the same ideas at 2 million rows and records the numbers in `results/results.json`.
""")

md("## 0. Setup\nA local SQLite catalog and a warehouse folder. The catalog is the one thing that knows where each table's current metadata lives.")
code("""
import os, sys, time, shutil
sys.path.insert(0, "lab")
import pyarrow as pa, pyarrow.compute as pc
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan
from pyiceberg.transforms import BucketTransform
from pyiceberg.types import DoubleType
from common import orders, files_on_disk, START
from experiments import SCHEMA, BY_DAY, health
from datetime import timedelta

WH = os.path.abspath("nb_warehouse")
shutil.rmtree(WH, ignore_errors=True); os.makedirs(WH)
catalog = SqlCatalog("nb", uri=f"sqlite:///{WH}/catalog.db", warehouse=f"file://{WH}")
catalog.create_namespace("shop")
data = orders(200_000)
data.slice(0, 3).to_pandas()
""")

md("## 1. Create a table and look inside\nThe table is partitioned by `day(ts)`. Note there is no separate date column. Iceberg derives the day from the timestamp. That is **hidden partitioning**.")
code("""
tbl = catalog.create_table("shop.orders", schema=SCHEMA, partition_spec=BY_DAY)
tbl.append(data)
print(tbl.spec())
print("rows:", tbl.scan(selected_fields=("order_id",)).to_arrow().num_rows)
""")
md("Iceberg is just files plus metadata. Data lives in Parquet files under `data/`. Everything that makes it behave like a database lives under `metadata/`: a JSON file per table version, a manifest list per snapshot, and manifests that list the data files with their statistics.")
code("""
for sub in ["data", "metadata"]:
    fs = files_on_disk(tbl.location(), sub)
    print(f"{sub}/: {len(fs)} files, e.g. {os.path.relpath(fs[0], tbl.location().replace('file://',''))}")
tbl.inspect.files().select(["file_path", "record_count", "file_size_in_bytes"]).slice(0, 3).to_pandas()
""")

md("## 2. The undo button\nSomeone runs `UPDATE orders SET amount = 0` and forgets the `WHERE`. In a plain data lake, the old files are gone. In Iceberg, the old snapshot is still there.")
code("""
good = tbl.current_snapshot().snapshot_id
before = pc.sum(tbl.scan(selected_fields=("amount",)).to_arrow()["amount"]).as_py()

cur = tbl.scan().to_arrow()
tbl.overwrite(cur.set_column(cur.schema.get_field_index("amount"), "amount", pa.array([0.0] * cur.num_rows)))
print("after the mistake:", pc.sum(tbl.scan(selected_fields=("amount",)).to_arrow()["amount"]).as_py())

t = time.perf_counter()
tbl.manage_snapshots().rollback_to_snapshot(good).commit()
print(f"rollback took {(time.perf_counter() - t) * 1000:.1f} ms")
tbl = catalog.load_table("shop.orders")
print("after rollback:", round(pc.sum(tbl.scan(selected_fields=("amount",)).to_arrow()["amount"]).as_py(), 2), "| before:", round(before, 2))
""")
md("Rollback only moves a pointer back to an older snapshot. No data is copied. That is also why it costs storage: the mistake wrote a full new copy, and the old files are kept until you expire them (section 7).")

md("## 3. The bad pipeline run\nA retried job re-sends events it already wrote. Every job reported success. Which rows are false? Ask Iceberg for exactly what one snapshot added.")
code("""
before_bad = tbl.current_snapshot().snapshot_id
day = data.filter(pc.and_(pc.greater_equal(data["ts"], pa.scalar(START + timedelta(days=12), pa.timestamp("us"))),
                          pc.less(data["ts"], pa.scalar(START + timedelta(days=13), pa.timestamp("us")))))
tbl.append(day.slice(0, 2000))          # the replay: 2,000 duplicate events
bad = tbl.current_snapshot().snapshot_id

added = tbl.incremental_append_scan(from_snapshot_id_exclusive=before_bad, to_snapshot_id_inclusive=bad).to_arrow()
print("rows added by the bad run:", added.num_rows)
print("rows now:", tbl.scan(selected_fields=("order_id",)).to_arrow().num_rows,
      "| rows as of the snapshot before:", tbl.scan(selected_fields=("order_id",), snapshot_id=before_bad).to_arrow().num_rows)
tbl.manage_snapshots().rollback_to_snapshot(before_bad).commit()
tbl = catalog.load_table("shop.orders")
""")
md("If good commits had landed after the bad one, a rollback would undo them too. Then you use the diff instead: the snapshot diff tells you exactly which rows to delete.")

md("## 4. Hidden partitioning\nFilter on the timestamp. Iceberg maps it to the day partition and only plans the matching file.")
code("""
lo, hi = START + timedelta(days=20), START + timedelta(days=21)
flt = And(GreaterThanOrEqual("ts", lo.isoformat()), LessThan("ts", hi.isoformat()))
print("files planned:", len(list(tbl.scan(row_filter=flt).plan_files())), "of", len(list(tbl.scan().plan_files())))
print("rows:", tbl.scan(row_filter=flt).to_arrow().num_rows)
""")
md("With a classic Hive-style folder layout partitioned by a separate `dt` column, the same filter on `ts` has to open every file. Only a filter on `dt` itself would prune. `lab/experiments.py` measures this side by side.")

md("## 5. Change the schema and the partitioning, rewrite nothing")
code("""
evo = catalog.create_table("shop.orders_evo", schema=SCHEMA, partition_spec=BY_DAY)
evo.append(data.slice(0, 50_000))
files_before = set(evo.inspect.files()["file_path"].to_pylist())
with evo.update_schema() as u:
    u.add_column("discount", DoubleType())
    u.rename_column("amount", "total_amount")
with evo.update_spec() as s:
    s.add_field("customer_id", BucketTransform(16), "customer_bucket")
evo = catalog.load_table("shop.orders_evo")
print("data files rewritten:", len(files_before - set(evo.inspect.files()["file_path"].to_pylist())))
print("partition specs:", len(evo.specs()))
evo.scan(selected_fields=("order_id", "total_amount", "discount"), limit=3).to_arrow().to_pandas()
""")
md("Columns are tracked by ID, not by name, so a rename is a metadata change. Old files keep the old partition spec; new writes use the new one.")

md("## 6. The small files problem, on purpose\nStreaming jobs commit often. Every commit adds files, a snapshot, a manifest list and a new metadata JSON.")
code("""
stream = catalog.create_table("shop.events_stream", schema=SCHEMA, partition_spec=BY_DAY)
src = orders(100 * 1000, seed=21, days=3)
for i in range(100):
    stream.append(src.slice(i * 1000, 1000))
h = health(stream); h
""")
code("""
t = time.perf_counter(); stream.scan().to_arrow(); slow = time.perf_counter() - t
stream.overwrite(stream.scan().to_arrow())          # compaction: read everything, write a few big files
stream = catalog.load_table("shop.events_stream")
t = time.perf_counter(); stream.scan().to_arrow(); fast = time.perf_counter() - t
print(f"full read before compaction {slow:.3f}s, after {fast:.3f}s")
health(stream)
""")
md("Read got faster. But look at `data_on_disk_mb`: it went **up**. The old small files are still referenced by old snapshots. Compaction does not free storage until you expire them.")

md("## 7. The maintenance bill\nExpire old snapshots, then delete files that no snapshot references any more (orphan files). PyIceberg 0.12 expires snapshots but does not delete files, so we find the orphans ourselves.")
code("""
current = stream.current_snapshot().snapshot_id
stream.maintenance.expire_snapshots().by_ids([s.snapshot_id for s in stream.snapshots() if s.snapshot_id != current]).commit()
stream = catalog.load_table("shop.events_stream")
referenced = set(stream.inspect.all_files()["file_path"].to_pylist())
orphans = [f for f in files_on_disk(stream.location(), "data") if "file://" + f not in referenced]
print("orphan data files:", len(orphans), f"({sum(os.path.getsize(f) for f in orphans)/1e6:.1f} MB)")
for f in orphans:
    os.remove(f)
health(stream)
""")
md("Metadata JSON files still pile up, one per commit. Two table properties cap them:")
code("""
tuned = catalog.create_table("shop.events_tuned", schema=SCHEMA, partition_spec=BY_DAY,
                             properties={"write.metadata.delete-after-commit.enabled": "true",
                                         "write.metadata.previous-versions-max": "10"})
for i in range(100):
    tuned.append(src.slice(i * 1000, 1000))
print("metadata.json files, default:", health(stream)["metadata_json_files"], "| tuned:", health(tuned)["metadata_json_files"])
""")

md("""
## What to take away

- Rollback and time travel are cheap because Iceberg never edits files in place. It writes new files and moves a pointer.
- That same design is the bill: every commit leaves files behind. Plan compaction, snapshot expiry and orphan cleanup from day one.
- Put a health check on every table: file count, average file size, snapshot count, metadata size, unreferenced files. Iceberg will not page you when these drift.

In Spark, the equivalents are the `rewrite_data_files`, `expire_snapshots` and `remove_orphan_files` procedures.
""")

nb["cells"] = C
nb["metadata"]["kernelspec"] = {"name": "python3", "display_name": "Python 3", "language": "python"}
nbf.write(nb, "iceberg_basics_to_production.ipynb")
print("written", len(C), "cells")
