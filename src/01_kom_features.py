import geopandas as gpd
import os
os.environ['POLARS_MAX_THREADS'] = "2"
import numpy as np
import polars as pl
import polars_st as st
import osmnx as ox
from concurrent.futures import ProcessPoolExecutor, as_completed
import time
import tqdm
import utils

np.random.seed(1234)

SRC_DIR =  os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SRC_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')

## Some quick data parsing ##
utils.parse_data()

lf_kom = pl.scan_parquet(f'{DATA_DIR}/dk_kom_geo_raw_after_2007.pq')


kommunerz = lf_kom.select(pl.col("kom").unique()).collect().to_series().to_list()

def get_geo_features(kom: int):

    lf_kom = pl.scan_parquet(f'{DATA_DIR}/dk_kom_geo_raw_after_2007.pq').filter(pl.col("kom")==kom)

    gdf_kom = lf_kom.collect().st.to_geopandas(geometry_name = 'geometry_kom').to_crs(4326)
    geom_kom = gdf_kom['geometry_kom'][0]

    # Combine tags for parks and waterways
    tags = {
        'leisure': ['park', 'nature_reserve', 'recreation_ground', 'garden'],
        'landuse': ['recreation_ground', 'forest', 'meadow'],
        'natural': ['water'],
        'waterway': ['river', 'stream', 'canal'],
        'water': True
    }
    features = ox.features_from_polygon(geom_kom, tags=tags).to_crs(25832)
    features_poly = features.polygonize()

    time.sleep(2)

    # Save features (lines etc)
    features.to_parquet(f'{DATA_DIR}/geometry/osm/lines/parks_and_water_{kom}.pq')
    # Convert to polygons and save
    features_gdf = gpd.GeoDataFrame(features_poly).set_geometry('polygons')

    # Save full result (parks & water together)
    features_gdf.to_parquet(f'{DATA_DIR}/geometry/osm/parks_and_water_{kom}.pq')
    print(f'Parsed parks and water for kom={kom}')

def main():
   with ProcessPoolExecutor(max_workers=4) as executor:
       futures = {executor.submit(get_geo_features, kom): kom for kom in kommunerz}
       for future in tqdm.tqdm(as_completed(futures), total=len(futures)):
           year = futures[future]
           try:
               future.result()
           except Exception as e:
               print(f'{e}')

if __name__ == "__main__":
   main()
