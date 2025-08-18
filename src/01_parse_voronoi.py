import os

os.environ['POLARS_MAX_THREADS'] = '16'

import polars as pl
from polars import selectors as cs
import geopandas as gpd
import concurrent.futures
import tqdm, glob
import pandas as pd
from functools import lru_cache

df = pl.scan_csv('adgangsadresser?format=csv').collect(engine = 'streaming').with_columns(
    cs.integer().shrink_dtype()
)
df.write_parquet('data/adresser.pq')

@lru_cache(maxsize=1)
def load_kommune_data():
    gdf_kom = gpd.read_parquet('data/kommune.pq').to_crs(25832)
    gdf_kom['code'] = gdf_kom['code'].astype('int')
    return gdf_kom

def parse_voronoi(kom: int):

    gdf_kom = load_kommune_data()    

    df = (pl.scan_parquet('data/adresser.pq', low_memory = True)
          .filter(pl.col("kommunekode")==kom)
          .select(pl.col("vejnavn", "husnr", "postnr", "kommunekode", "landsdelsnuts3"), pl.col("etrs89koordinat_øst").alias("etrs89_east"), pl.col("etrs89koordinat_nord").alias("etrs89_north"))
          .filter(pl.struct(pl.col("etrs89_east", "etrs89_north")).is_first_distinct())
          .collect(engine = 'streaming')
          .to_pandas()
    )

    gdf_adr = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(x = df.etrs89_east, y=df.etrs89_north), crs=25832)


    kom_shape = gdf_kom[gdf_kom['code']==kom].reset_index(drop=True)
    gdf_adr['voronoi'] = gdf_adr.voronoi_polygons()

    gdf_adr['points'] = gdf_adr['geometry']

    gdf_adr = gdf_adr.drop('geometry', axis=1)
    gdf_adr = gdf_adr.set_geometry('voronoi')

    
    gdf_out = gpd.overlay(gdf_adr, kom_shape, how = 'intersection')

    print(f'{kom} to voronoi done.')

    gdf_out.to_parquet(f'data/voronoi/adresser_voronoi_{kom}.pq')


lf = pl.scan_parquet('data/adresser.pq').select(pl.col("kommunekode").unique())
kommunerz = lf.collect().to_series().to_list()
with concurrent.futures.ThreadPoolExecutor(max_workers = 8) as executor:
    res = list(tqdm.tqdm(executor.map(parse_voronoi, kommunerz)))

vo_list = glob.glob('data/voronoi/adresser_v*.pq')

gdf = gpd.read_parquet(vo_list[0])

for data in vo_list:
    if data == vo_list[0]:
        pass
    gdf2 = gpd.read_parquet(data)

    gdf = pd.concat([gdf, gdf2])

gdf = gdf.reset_index(drop = True)

gdf.to_parquet('data/adresser_voronoi.pq')