from decimal import Decimal

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pandas._libs.lib import infer_dtype


def write_example_table():
    fields = [
        pa.field('id', pa.float64()),
        pa.field('secondaryid', pa.string()),
        pa.field('some_bool', pa.bool_()),
        pa.field('some_decimal', pa.decimal128(21, 3)),
    ]

    my_schema = pa.schema(fields)
    df = pd.DataFrame({'id': [-1, 0, 2.5],
                       'secondaryid': ['foo', 'bar', 'baz'],
                       'some_bool': [True, False, True],
                       'some_decimal': [Decimal("122123.123"), Decimal("12321.153"), Decimal("25.112")]},
                      index=list('abc'))

    table = pa.Table.from_pandas(df=df, schema=my_schema, preserve_index=False)
    pq.write_table(table, 'example.parquet')


def next_example():
    import pyarrow as pa
    from decimal import Decimal

    # Definieren Sie die Spaltennamen und -typen als PyArrow-Schema
    schema = pa.schema([
        ('Dezimal-Spalte', pa.decimal128(10, 2))
    ])

    # Wandeln Sie die Dezimalzahlen in Decimal-Objekte um
    data = [
        pa.array([Decimal('0.1'), Decimal('0.2'), Decimal('0.3')], type=pa.decimal128(10, 2))
    ]

    # Erstellen Sie ein PyArrow-Array mit den Daten und dem Schema
    table = pa.Table.from_arrays(data, schema=schema)

    # Konvertieren Sie das PyArrow-Table in einen Pandas DataFrame
    df = table.to_pandas()

    # Zeigen Sie den erstellten DataFrame an

    print(pa.Table.from_pandas(
        df, preserve_index=False, schema=schema).schema)
    print(table.schema)
    print(df)
    print(df.dtypes)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    next_example()
    # write_example_table()

# fields = [
#    pa.field('id', pa.float64()),
#    pa.field('secondaryid', pa.string()),
#    pa.field('some_bool', pa.bool_()),
#    pa.field('date', pa.date64()),
#    pa.field('some_decimal', pa.decimal128(21,3)),
# ]
# my_schema = pa.schema(fields)

# table2 = pd.read_parquet('example.parquet', schema=my_schema)

# print(infer_dtype(table2["some_decimal"]))
