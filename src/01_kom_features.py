import geopandas as gpd
import numpy as np
import glob, tqdm
import concurrent.futures
import osmnx as ox
import pandas as pd
import time
from functools import lru_cache

np.random.seed(1234)

@lru_cache(maxsize=1)
def load_kommune_data():
    gdf_kom = gpd.read_file('data/au_inspire.gpkg').to_crs(25832)
    gdf_kom = gdf_kom[gdf_kom['nationallevelname']=='Kommune']
    gdf_kom = gdf_kom[~gdf_kom['nationalcode'].str.contains('DK')].reset_index(drop=True)
    gdf_kom['nationalcode'] = gdf_kom['nationalcode'].astype('int')
    return gdf_kom

gdf_kom = load_kommune_data().to_crs(4326)
kommunerz = gdf_kom['nationalcode'].to_list()

# def get_geo_features(kommunerz: list[int], gdf_kom: gpd.GeoDataFrame):
#     for kom in kommunerz:
#         gdf_kom_filtered = gdf_kom[gdf_kom['nationalcode']==kom].reset_index(drop=True)
#         geom_kom = gdf_kom_filtered['geometry'][0]

#         parks = ox.features_from_polygon(geom_kom, tags={
#             'leisure': ['park', 'nature_reserve', 'recreation_ground', 'garden'],
#             'landuse': ['recreation_ground', 'forest', 'meadow']
#         }).polygonize()

#         time.sleep(3)

#         parks = gpd.GeoDataFrame(parks).set_geometry('polygons')

#         parks.to_parquet(f'data/geometry/parks_{kom}.pq')
#         print(f'Parsed parks for kom={kom}')

#         water = ox.features.features_from_polygon(
#             geom_kom,
#             tags={
#                 'natural': ['water'],
#                 'waterway': ['river', 'stream', 'canal'],
#                 'water': True
#             }).polygonize()

#         water = gpd.GeoDataFrame(water).set_geometry('polygons')
#         water.to_parquet(f'data/geometry/water_{kom}.pq')
#         print(f'Parsed water for kom={kom}')

# get_geo_features(kommunerz=kommunerz, gdf_kom=gdf_kom)

def clean_kommune(kom: int):
    
    gdf_kom = load_kommune_data().to_crs(4326)

    gdf_kom_filtered = gdf_kom[gdf_kom['nationalcode']==kom].reset_index(drop = True)
    
    gdf_water = gpd.read_parquet(f'data/geometry/water_{kom}.pq')
    gdf_park = gpd.read_parquet(f'data/geometry/parks_{kom}.pq')
    
    gdf_cleaned = gpd.overlay(gdf_kom_filtered, gdf_water, how='difference')
    gdf_cleaned = gpd.overlay(gdf_cleaned, gdf_park, how='difference').to_crs(25832)
    gdf_cleaned[['nationalcode', 'geometry']].to_parquet(f'data/geometry/kom_geom_cleaned_{kom}.pq')
    print(f'Cleaned shapefile for kom={kom}')

with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
    res = list(tqdm.tqdm(executor.map(clean_kommune, kommunerz)))

files = glob.glob('data/geometry/kom_geom_cleaned_*.pq')

def concat_geo_data(list: list[str] | str):
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
        gdf.to_parquet('data/dk_kom_geo.pq')

concat_geo_data(files)