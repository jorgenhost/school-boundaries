import os

os.environ['POLARS_MAX_THREADS'] = '16'

import polars as pl
from polars import selectors as cs
import geopandas as gpd
import concurrent.futures
import tqdm, glob
import pandas as pd
from functools import lru_cache

df = pl.scan_csv('data/dk_adresser.csv').collect(engine = 'streaming').with_columns(
    cs.integer().shrink_dtype()
)
df.write_parquet('data/dk_adresser.pq')

@lru_cache(maxsize=1)
def load_kommune_data():
    gdf_kom = gpd.read_file('data/au_inspire.gpkg').to_crs(25832)
    gdf_kom = gdf_kom[~gdf_kom['nationalcode'].str.contains('DK')].reset_index(drop=True)
    gdf_kom['nationalcode'] = gdf_kom['nationalcode'].astype('int')
    return gdf_kom

def parse_voronoi(kom: int):

    gdf_kom = load_kommune_data()    

    df = (pl.scan_parquet('data/dk_adresser.pq', low_memory = True)
          .filter(pl.col("kommunekode")==kom)
          .select(pl.col("vejnavn", "husnr", "postnr", "kommunekode", "landsdelsnuts3"), pl.col("etrs89koordinat_øst").alias("etrs89_east"), pl.col("etrs89koordinat_nord").alias("etrs89_north"))
          .filter(pl.struct(pl.col("etrs89_east", "etrs89_north")).is_first_distinct())
          .collect(engine = 'streaming')
          .to_pandas()
    )

    gdf_adr = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(x = df.etrs89_east, y=df.etrs89_north), crs=25832)


    kom_shape = gdf_kom[gdf_kom['nationalcode']==kom].reset_index(drop=True)
    gdf_adr['voronoi'] = gdf_adr.voronoi_polygons()

    gdf_adr['points'] = gdf_adr['geometry']

    gdf_adr = gdf_adr.drop('geometry', axis=1)
    gdf_adr = gdf_adr.set_geometry('voronoi')

    
    gdf_out = gpd.overlay(gdf_adr, kom_shape, how = 'intersection')

    print(f'{kom} to voronoi done.')

    gdf_out.to_parquet(f'data/voronoi/dk_adresser_voronoi_{kom}.pq')


lf = pl.scan_parquet('data/dk_adresser.pq').select(pl.col("kommunekode").unique())
kommunerz = lf.collect().to_series().to_list()
with concurrent.futures.ThreadPoolExecutor(max_workers = 8) as executor:
    res = list(tqdm.tqdm(executor.map(parse_voronoi, kommunerz)))

files = glob.glob('data/voronoi/dk_adresser_v*.pq')

def concat_voronoi_data(list: list[str] | str):
    if len(list)==1:
        gdf = gpd.read_parquet(list[0])
        gdf = gdf.reset_index(drop = True)
        gdf.to_parquet('data/dk_adresser_voronoi.pq')
    else:
        gdf = gpd.read_parquet(list[0])
        for data in list:
            if data == list[0]:
                pass
            gdf2 = gpd.read_parquet(data)

            gdf = pd.concat([gdf, gdf2])

            gdf = gdf.reset_index(drop = True)
        gdf.to_parquet('data/dk_adresser_voronoi.pq')

concat_voronoi_data(files)