import os
import ibis
from concurrent.futures import ProcessPoolExecutor, as_completed
import geopandas as gpd
import polars as pl
import tqdm
import utils

SRC_DIR =  os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SRC_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')

utils.parse_voronoi()

lf = pl.scan_parquet(f'{DATA_DIR}/dk_adr.pq').select(pl.col("kommunekode").unique())
kommunerz = lf.collect().to_series().to_list()

def create_con():
    return ibis.duckdb.connect(extensions = ['spatial'])

def clip_voronoi_stream(
    kom: int

):
    # Adresses as voronoi polygons, but not clipped within a boundary
    voronoi_path=f'{DATA_DIR}/dk_adr_voronoi.pq'

    # Kommune shapefiles with natural boundaries
    kom_path=f'{DATA_DIR}/dk_kom_geo_natural_boundaries.pq'
    out_dir=f'{DATA_DIR}/voronoi/adr_by_kom/dk_adr_voronoi_clipped_{kom}.pq'

    con = create_con()

    con.raw_sql("SET arrow_large_buffer_size=true;")
    con.raw_sql("SET memory_limit = '2GB'")
    con.raw_sql("SET threads to 2;")
    con.raw_sql("SET preserve_insertion_order=false;")

    
    con.raw_sql(f"""
        COPY (
          SELECT
            v.access_address_id,
            ST_Intersection(v.geometry, k.geometry) AS voronoi_clipped
          FROM read_parquet('{voronoi_path}') v
          JOIN read_parquet('{kom_path}') k
          ON v.kommunekode = k.nationalcode
          WHERE ST_Intersects(v.geometry, k.geometry)
            AND v.kommunekode = {kom}
        )
        TO '{out_dir}' (FORMAT 'PARQUET');
    """)

    gpd.read_parquet(f'{out_dir}').set_crs(25832, allow_override=True).to_parquet(f'{out_dir}')

    print(f'Clipped voronoi polygons within kom = {kom}.')

def main():
    with ProcessPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(clip_voronoi_stream, kom): kom for kom in kommunerz}
        for future in tqdm.tqdm(as_completed(futures), total=len(futures)):
            year = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f'{e}')

if __name__ == "__main__":
    main()