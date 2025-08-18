# School boundaries

## Step 1

```bash
wget https://api.dataforsyningen.dk/adgangsadresser?format=csv
```

```python
import polars as pl
import polars.selectors as cs

df = pl.scan_csv('adgangsadresser?format=csv').collect(engine = 'streaming').with_columns(
    cs.integer().shrink_dtype()
)
df.write_parquet('data/adresser.pq')
```