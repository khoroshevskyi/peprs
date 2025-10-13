import polars as pl
from peprs import Project

df = pl.read_csv(
    "/Users/nathanleroy/Desktop/databio-bedbase_raw-default/sample_table.csv",
    infer_schema_length=10_000
)
proj = Project.from_polars(df)