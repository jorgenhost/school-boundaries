import os

os.environ['POLARS_MAX_THREADS'] = '16'

import polars as pl
import polars_st as st
import geopandas as gpd

def main():
    print("Hello from school-boundaries!")


lf = pl.scan_parquet('data/adresser.pq').select(
    pl.col("vejnavn", "husnr", "postnr", "kommunekode", "landsdelsnuts3"), pl.col("etrs89koordinat_øst").alias("etrs89_east"), pl.col("etrs89koordinat_nord").alias("etrs89_north")
).filter(pl.struct(pl.col("etrs89_east", "etrs89_north")).is_first_distinct()).with_columns(
    pl.struct(pl.col("etrs89_east", "etrs89_north")).hash().alias("address_id").rank("dense").shrink_dtype()
)

df = lf.collect(engine = 'streaming').sample(fraction=0.1)

gdf = st.GeoDataFrame(df.with_columns(
    coords = pl.concat_list(pl.col("etrs89_east", "etrs89_north"))
).with_columns(
    geometry = st.point("coords")
).with_columns(
    voronoi = st.voronoi_polygons("geometry")
))

gdf = gdf.st.to_geopandas(use_pyarrow_extension_array=True)

gdf.to_parquet('data/adresser_geo.pq')


if __name__ == "__main__":
    main()
