import os
import timeit
from os.path import join
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import csv
import polars as pl
import pandas as pd
from linetimer import CodeTimer, linetimer

FILE_TYPE = "parquet"
INCLUDE_IO = True
DATASET_BASE_DIR = "/home/raunaks3/TPC-H/dbgen/data-10gb/"
DATASET_PARQUET_DIR = DATASET_BASE_DIR + "parquet/"

def _scan_ds(path: str):
    path = f"{path}.{FILE_TYPE}"
    if FILE_TYPE == "parquet":
        scan = pl.read_parquet(path, use_pyarrow=True, memory_map=True)
        # scan = pl.scan_parquet(path)

        # print(scan.columns)
    # elif FILE_TYPE == "feather":
    #     scan = pl.scan_ipc(path)
    else:
        raise ValueError(f"file type: {FILE_TYPE} not expected")
    if INCLUDE_IO:
        return scan
    # return scan.collect().lazy()
    return scan


def get_line_item_ds(base_dir: str = DATASET_PARQUET_DIR) -> pl.LazyFrame:
    return _scan_ds(join(base_dir, "lineitem"))


def get_orders_ds(base_dir: str = DATASET_PARQUET_DIR) -> pl.LazyFrame:
    return _scan_ds(join(base_dir, "orders"))


def get_customer_ds(base_dir: str = DATASET_PARQUET_DIR) -> pl.LazyFrame:
    return _scan_ds(join(base_dir, "customer"))


def get_region_ds(base_dir: str = DATASET_PARQUET_DIR) -> pl.LazyFrame:
    return _scan_ds(join(base_dir, "region"))


def get_nation_ds(base_dir: str = DATASET_PARQUET_DIR) -> pl.LazyFrame:
    return _scan_ds(join(base_dir, "nation"))


def get_supplier_ds(base_dir: str = DATASET_PARQUET_DIR) -> pl.LazyFrame:
    return _scan_ds(join(base_dir, "supplier"))


def get_part_ds(base_dir: str = DATASET_PARQUET_DIR) -> pl.LazyFrame:
    return _scan_ds(join(base_dir, "part"))


def get_part_supp_ds(base_dir: str = DATASET_PARQUET_DIR) -> pl.LazyFrame:
    return _scan_ds(join(base_dir, "partsupp"))


def save_tpch_parquet_files(compression_codec: str = "qpl"):
    """
    Saves the TPC-H data in parquet format with the given compression codec.
    :param compression_codec: Compression codec to use. Valid values are: 'qpl', 'snappy', 'gzip', 'brotli', 'lz4', 'zstd'
    """
    for table_name in ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]:
    # for table_name in ["nation"]:

        csv_file = DATASET_BASE_DIR + table_name + ".csv"
        # table = pd.read_csv(csv_file, parse_dates=True)
        # print(table.dtypes)

        pyarrow_table = csv.read_csv(csv_file)

        # pyarrow_table = pa.Table.from_pandas(table)
        print(pyarrow_table.schema)
        # Write to disk using QPL compression in Parquet format using PyArrow
        parquet_file = DATASET_PARQUET_DIR + table_name + ".parquet"
        pq.write_table(pyarrow_table, parquet_file, compression=compression_codec)


def save_query_result(q_res, Q_NUM, compression_codec: str = "qpl"):
    q_res.write_parquet(f"{DATASET_PARQUET_DIR}queries/q{Q_NUM}.parquet", use_pyarrow=True, compression=compression_codec)


def run_query(q_num: int, lp: pl.LazyFrame):
    @linetimer(name=f"Overall execution of polars Query {q_num}", unit="s")
    def query():
        # if SHOW_PLAN:
        #     print(lp.describe_optimized_plan())

        with CodeTimer(name=f"Get result of polars Query {q_num}", unit="s"):
            t0 = timeit.default_timer()
            result = lp.collect()
            secs = timeit.default_timer() - t0

        # if LOG_TIMINGS:
        #     append_row(
        #         solution="polars", version=pl.__version__, q=f"q{q_num}", secs=secs
        #     )

        # if TEST_RESULTS:
        #     test_results(q_num, result)

        # if SHOW_RESULTS:
        #     print(result)

        # if SAVE_RESULTS:
        #     output_file = f"{OUTPUT_BASE_DIR}/q{q_num}.csv"
        #     print(f"Saving Results to {output_file}")
        #     result.write_csv(output_file)
    query()

if __name__ == "__main__":
    save_tpch_parquet_files(compression_codec="qpl")