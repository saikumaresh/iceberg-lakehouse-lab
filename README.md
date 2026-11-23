# iceberg-lakehouse-lab

Eight small, reproducible experiments that show what Apache Iceberg actually does on disk: the undo button, snapshot diffs, hidden partitioning, schema and partition evolution, and the maintenance bill nobody mentions in the demo.

Runs on a laptop with **PyIceberg**. No Spark, no Docker, no cloud account. One `pip install` and a local folder.

Companion code for the article *The Undo Button for a Petabyte* by [Sai Kumaresh](https://medium.com/@saikumaresh).

## Results

2 million synthetic order events over 30 days, table partitioned by `day(ts)`. PyIceberg 0.12.0, PyArrow 26.0.1, 2 CPU cores. Timings vary run to run; the shape does not.

| # | Experiment | What happened |
|---|---|---|
| 1 | `UPDATE orders SET amount = 0` with no `WHERE`, then undo | Rollback took **7 ms**. Total restored to the exact original. The mistake itself took 1.39 s and wrote a full new copy of the table. |
| 2 | A retried job appends 6,000 duplicate events | A snapshot diff returned **exactly the 6,000 false rows** in 7 ms. Time travel showed 2,000,000 rows before vs 2,006,000 after. |
| 3 | Filter on the timestamp for one day | Iceberg planned **1 of 30 files** (0.020 s). A Hive-style layout partitioned by a separate `dt` column opened all 30 for the same filter (0.114 s). |
| 4 | Add a column and rename another | 7 ms. **0 data files rewritten.** Old rows return null for the new column. |
| 5 | Add a `bucket(16, customer_id)` partition field | 11 ms. **0 data files rewritten.** Old files keep the old spec. |
| 6 | 300 tiny appends (2,000 rows each) | 900 data files averaging 17 KB. Full read 2.04 s, planning 0.46 s. After compaction to 3 files: read 0.086 s (**24x faster**), planning 0.007 s. |
| 7 | Compaction, snapshot expiry, orphan cleanup | Data on disk went **up** from 15.7 MB to 25.0 MB after compaction, stayed at 25.0 MB after expiring snapshots, and dropped to 9.2 MB only after deleting 900 unreferenced files. |
| 8 | Metadata retention | By default: 301 `metadata.json` files and **38.7 MB of metadata for 15.7 MB of data**. With `write.metadata.delete-after-commit.enabled` and `previous-versions-max=10`: 11 files, 6.4 MB. |

Raw numbers: [`results/results.json`](results/results.json).

## Run it

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python lab/experiments.py            # full run, about 2 minutes
python lab/experiments.py --rows 100000 --batches 20 --batch-rows 500   # quick run

jupyter notebook iceberg_basics_to_production.ipynb
```

The notebook starts from zero (create a table, look at the files it writes) and walks through every experiment at a smaller scale.

## What is in here

- `lab/common.py` local SQLite catalog, timer, synthetic orders data
- `lab/experiments.py` the eight experiments and the `health()` check
- `iceberg_basics_to_production.ipynb` the guided walkthrough, already executed
- `results/results.json` the numbers in the table above

## Notes

- PyIceberg 0.12 can expire snapshots but does not delete files, and has no compaction procedure. The lab compacts by reading and overwriting, and finds orphan files by comparing what is on disk with what the remaining snapshots reference. In Spark, use the `rewrite_data_files`, `expire_snapshots` and `remove_orphan_files` procedures.
- Experiment 1's "plain lake" comparison assumes you even have a backup to restore from.
- Data is synthetic. Sizes are small on purpose so it runs anywhere; the ratios are the point.

## License

MIT
