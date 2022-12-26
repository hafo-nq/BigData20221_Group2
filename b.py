import pandas as pd
df = pd.read_parquet('part-00000-75b178b3-2807-45d8-b13e-11747c285dbd-c000.snappy.parquet', engine='pyarrow')
print(df)