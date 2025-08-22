import geopandas as gpd
import numpy as np
import glob, tqdm
import polars as pl
import polars.selectors as cs
import concurrent.futures
import osmnx as ox
import pandas as pd
import time, os
import utils
import ibis
from functools import lru_cache

np.random.seed(1234)

SRC_DIR =  os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SRC_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')

## some quick data parsing ##
def parse_kommune_data():
    gdf_kom = gpd.read_file(f'{DATA_DIR}/au_inspire.gpkg').to_crs(25832)
    gdf_kom = gdf_kom[gdf_kom['nationallevelname']=='Kommune']
    gdf_kom = gdf_kom[~gdf_kom['nationalcode'].str.contains('DK')].reset_index(drop=True)
    gdf_kom['nationalcode'] = gdf_kom['nationalcode'].astype('int')
    gdf_kom.to_parquet(f'{DATA_DIR}/dk_kom_geo_raw.pq')

df = pl.scan_csv(f'{DATA_DIR}/dk_adresser.csv').collect(engine = 'streaming').with_columns(
    cs.integer().shrink_dtype()
)
df.write_parquet(f'{DATA_DIR}/dk_adresser.pq')

parse_kommune_data()

@lru_cache(maxsize=1)
def load_kommune_data():
    gdf_kom = gpd.read_parquet(f'{DATA_DIR}/dk_kom_geo_raw.pq')
    return gdf_kom

gdf_kom = load_kommune_data().to_crs(4326)
kommunerz = gdf_kom['nationalcode'].to_list()

def get_geo_features(kommunerz: list[int], gdf_kom: gpd.GeoDataFrame):
    for kom in kommunerz:
        gdf_kom_filtered = gdf_kom[gdf_kom['nationalcode']==kom].reset_index(drop=True)
        geom_kom = gdf_kom_filtered['geometry'][0]

        parks = ox.features_from_polygon(geom_kom, tags={
            'leisure': ['park', 'nature_reserve', 'recreation_ground', 'garden'],
            'landuse': ['recreation_ground', 'forest', 'meadow']
        }).polygonize()

        time.sleep(2)

        parks = gpd.GeoDataFrame(parks).set_geometry('polygons').to_crs(25832)

        parks.to_parquet(f'{DATA_DIR}/geometry/parks_{kom}.pq')
        print(f'Parsed parks for kom={kom}')

        water = ox.features.features_from_polygon(
            geom_kom,
            tags={
                'natural': ['water'],
                'waterway': ['river', 'stream', 'canal'],
                'water': True
            }).polygonize()
        time.sleep(2)
        water = gpd.GeoDataFrame(water).set_geometry('polygons').to_crs(25832)
        water.to_parquet(f'{DATA_DIR}/geometry/water_{kom}.pq')
        print(f'Parsed water for kom={kom}')

def kommune_interior_holes(kom: int):
    
    con = ibis.duckdb.connect(extensions=['spatial'])
    kom_table  = con.read_parquet(f'{DATA_DIR}/dk_kom_geo_raw.pq')
    kom_table = kom_table.filter(kom_table.nationalcode == kom).select('geometry')

    water_table = con.read_parquet(f'{DATA_DIR}/geometry/water_{kom}.pq')
    park_table = con.read_parquet(f'{DATA_DIR}/geometry/parks_{kom}.pq')

    # Assuming only one geometry per water/park table
    water_union = water_table.polygons.execute().union_all()
    park_union = park_table.polygons.execute().union_all()

    water_geom_lit = ibis.literal(water_union, "geometry")
    park_geom_lit = ibis.literal(park_union, "geometry")

    # Chain the difference operations
    kom_table_clean = kom_table.mutate(
        geometry=kom_table.geometry
            .difference(water_geom_lit)
            .difference(park_geom_lit),
        nationalcode = kom
    )

    df = kom_table_clean.execute()

    print(f'Removed interior holes (parks/water) from kom = {kom}.')

    df.to_parquet(f'{DATA_DIR}/geometry/kom_geom_cleaned_{kom}.pq')

# Fetch features from OpenStreetMap
get_geo_features(kommunerz=kommunerz, gdf_kom=gdf_kom)


with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
    res = tqdm.tqdm(executor.map(kommune_interior_holes, kommunerz))

files = glob.glob(f'{DATA_DIR}/geometry/kom_geom_cleaned_*.pq')

utils.concat_geo_data(files, path = f'{DATA_DIR}/dk_kom_geo_interior_holes.pq')