import os

os.environ['POLARS_MAX_THREADS'] = '16'

import polars as pl
from polars import selectors as cs
import geopandas as gpd
import concurrent.futures
import tqdm, glob
import pandas as pd
import ibis
import utils

SRC_DIR =  os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SRC_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')

utils.parse_voronoi()

def clip_voronoi(kom: int):
    con = ibis.duckdb.connect(extensions=['spatial'])
    voronoi_table = con.read_parquet(f'{DATA_DIR}/dk_adr_voronoi.pq')
    kom_table  = con.read_parquet(f'{DATA_DIR}/dk_kom_geo_raw.pq')
    out_path = f'{DATA_DIR}/voronoi/dk_adresser_voronoi_{kom}.pq'

    kom_table = kom_table.filter(kom_table.nationalcode == kom).select('geometry')

    kom_geom = kom_table.geometry.execute()[0]
    kom_geom_literal = ibis.literal(kom_geom, "geometry")

    # Compute intersection and add as new column
    intersected = voronoi_table.mutate(voronoi=voronoi_table.geometry.intersection(kom_geom_literal))

    # Filter for non-empty intersections
    filtered = intersected.filter(intersected.voronoi.area() > 0)

    # To get the results as a pandas DataFrame:
    df = filtered.execute().drop(['__index_level_0__', 'geometry'], axis = 1).set_geometry('voronoi')
    
    df.to_parquet(f'{out_path}')

    print(f'Clipped voronoi polygon within kom = {kom}.')

def clip_voronoi_no_interior(kom: int):
    con = ibis.duckdb.connect(extensions=['spatial'])
    voronoi_table = con.read_parquet(f'{DATA_DIR}/dk_adr_voronoi.pq')
    kom_table  = con.read_parquet(f'{DATA_DIR}/dk_kom_geo_interior_holes.pq')
    out_path = f'{DATA_DIR}/voronoi/no_interior/dk_adresser_voronoi_no_interior_holes_{kom}.pq'

    kom_table = kom_table.filter(kom_table.nationalcode == kom).select('geometry')

    kom_geom = kom_table.geometry.execute()[0]
    kom_geom_literal = ibis.literal(kom_geom, "geometry")

    # Compute intersection and add as new column
    intersected = voronoi_table.mutate(voronoi=voronoi_table.geometry.intersection(kom_geom_literal))

    # Filter for non-empty intersections
    filtered = intersected.filter(intersected.voronoi.area() > 0)

    # To get the results as a pandas DataFrame:
    df = filtered.execute().drop(['__index_level_0__', 'geometry'], axis = 1).set_geometry('voronoi')
    
    df.to_parquet(f'{out_path}')

    print(f'Clipped voronoi polygon (no interior holes) within kom = {kom}.')



lf = pl.scan_parquet(f'{DATA_DIR}/dk_adresser.pq').select(pl.col("kommunekode").unique())
kommunerz = lf.collect().to_series().to_list()
with concurrent.futures.ThreadPoolExecutor(max_workers = 8) as executor:
    res = list(tqdm.tqdm(executor.map(clip_voronoi_no_interior, kommunerz)))


# files = glob.glob(f'{DATA_DIR}/voronoi/dk_adresser_v*.pq')
# concat_voronoi_data(files)

files = glob.glob(f'{DATA_DIR}/voronoi/no_interior/dk_adresser_v*.pq')
utils.concat_geo_data(files, path = f'{DATA_DIR}/data/dk_adresser_voronoi_no_interior.pq')
