## Benchmarking TPCH with Apache Arrow and Polars

First generate TPC-H tables from the official repository.

Preprocess tables (.tbl): Run cells in `process-tpch.ipynb` by specifying the correct data_dir (where TPC-H) data has been loaded

Create parquet files: `python3 utils.py` (make sure to specify desired compression codec)

Run TPC-H queries and log results: `python3 run_tpch_queries.py`