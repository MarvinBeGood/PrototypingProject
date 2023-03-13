import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq



def write_example_table():
    fields = [
        pa.field('id', pa.float64()),
        pa.field('secondaryid', pa.string()),
        pa.field('some_bool', pa.bool_()),
    ]

    my_schema = pa.schema(fields)
    df = pd.DataFrame({'id': [-1, np.nan, 2.5],
                       'secondaryid': ['foo', 'bar', 'baz'],
                       'some_bool': [True, False, True]},
                      index=list('abc'))

    table = pa.Table.from_pandas(df=df, schema=my_schema, preserve_index=False)
    pq.write_table(table, 'example.parquet')






# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    write_example_table()

    fields = [
        pa.field('id', pa.float64()),
        pa.field('secondaryid', pa.string()),
        pa.field('some_bool', pa.bool_()),
        pa.field('date', pa.date64()),
    ]
    my_schema = pa.schema(fields)

    table2 = pd.read_parquet('example.parquet', schema=my_schema)
    print(table2.to_string())
