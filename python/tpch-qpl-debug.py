import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import pandas as pd
from tpch_arrow_queries import q1
import pandas as pd
import polars as pl

# Read CSV file using PyArrow
data_dir = "/home/raunaks3/TPC-H/dbgen/data-1gb/"
csv_file = data_dir + "part.csv"
table = pd.read_csv(csv_file)
print("Number of rows: {}".format(table.shape[0]))
print("Number of columns: {}".format(table.shape[1]))

pyarrow_table = pa.Table.from_pandas(table)

# Write to disk using QPL compression in Parquet format using PyArrow
output_dir = "/home/raunaks3/TPC-H/dbgen/data-1gb/parquet/"
parquet_file = output_dir + "part.parquet"
pq.write_table(pyarrow_table, parquet_file, compression='qpl')

# decompressed_table = pq.read_table(parquet_file)
# # get number of rows and columns of decompressed table:
# num_rows = decompressed_table.num_rows
# num_cols = decompressed_table.num_columns
# print("Number of rows: {}".format(num_rows))
# print("Number of columns: {}".format(num_cols))

# df = pd.read_parquet(parquet_file, engine='pyarrow')
# print(df.head())

df = pl.read_parquet(parquet_file, use_pyarrow=True, memory_map=True)
print(df.head())
q1()

# q1(decompressed_table)


