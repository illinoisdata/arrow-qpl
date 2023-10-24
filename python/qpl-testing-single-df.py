import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

arr1 = pa.array(np.arange(100, ))
arr2 = pa.array(np.linspace(1000, 2000, 100))
arr3 = pa.array(np.linspace(3000, 10000, 100))
table = pa.Table.from_arrays([arr1, arr2, arr3], names=["col1", "col2", "col3"])

### SNAPPY ###
start_time = datetime.now()
pq.write_table(table, "./qpl-testing/parquet-example-snappy.parquet", compression='snappy')
end_time = datetime.now()

snappy_write_time = end_time - start_time
print("Snappy Write Time: {}".format(snappy_write_time.total_seconds()*1000))

start_time = datetime.now()
decompressed_table = pq.read_table("./qpl-testing/parquet-example-snappy.parquet")
end_time = datetime.now()

snappy_read_time = end_time - start_time
print("Snappy Read Time: {}".format(snappy_read_time.total_seconds()*1000))

### QPL ###
start_time = datetime.now()
pq.write_table(table, "./qpl-testing/parquet-example-qpl.parquet", compression='qpl')
end_time = datetime.now()

qpl_write_time = end_time - start_time
print("QPL Write Time: {}".format(qpl_write_time.total_seconds()*1000))

start_time = datetime.now()
decompressed_table = pq.read_table("./qpl-testing/parquet-example-qpl.parquet")
end_time = datetime.now()

qpl_read_time = end_time - start_time
print("QPL Read Time: {}".format(qpl_read_time.total_seconds()*1000))

start_time = datetime.now()
pa.TableGroupBy(decompressed_table,"col2").aggregate([("col3", "sum")])
end_time = datetime.now()

operation_time = end_time - start_time
print("GroupBy + Agg (SUM) time: {}".format(operation_time.total_seconds()*1000))