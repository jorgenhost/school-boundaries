import os
import polars as pl
import polars_st as st
import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed


SRC_DIR =  os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SRC_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')


lf_kom = pl.scan_parquet(f'{DATA_DIR}/dk_kom_geo_raw_after_2007.pq').select(pl.col("kom"), st.geom("geometry_kom").st.set_srid(25832))

kommunerz = lf_kom.select(pl.col("kom").unique()).collect().to_series().to_list()

def parse_voronoi(kom: int):

    lf = (pl.scan_parquet(f'{DATA_DIR}/dk_adr.pq')
            .select(pl.col("kommunekode").alias("kom"), pl.col("etrs89koordinat_øst").alias("etrs89_east"), pl.col("etrs89koordinat_nord").alias("etrs89_north"))
            .filter(pl.struct(pl.col("etrs89_east", "etrs89_north")).is_first_distinct())
            .filter(pl.col("kom")==kom)
            .with_columns(access_address_id = pl.struct(pl.col("etrs89_east", "etrs89_north")).hash().rank('dense').shrink_dtype(),
                point = st.point(pl.concat_arr("etrs89_east", "etrs89_north")).st.set_srid(25832)
            )
            .group_by('kom').agg(
            st.geom("point").st.voronoi_polygons().st.parts().alias("voronoi")
            )
            .join(lf_kom, on = 'kom'
            )
            .explode("voronoi").with_columns(
            voronoi_clipped = st.geom("voronoi").st.intersection('geometry_kom')
            )
    )

    lf.collect(engine = 'streaming').write_parquet(f'{DATA_DIR}/voronoi/voronoi_clipped_{kom}.pq')
    print(f'Parsed voronoi polygons for kom = {kom}.')

def main():
   with ProcessPoolExecutor(max_workers=4) as executor:
       futures = {executor.submit(parse_voronoi, kom): kom for kom in kommunerz}
       for future in tqdm.tqdm(as_completed(futures), total=len(futures)):
           year = futures[future]
           try:
               future.result()
           except Exception as e:
               print(f'{e}')

if __name__ == "__main__":
   main()
   pl.scan_parquet(f'{DATA_DIR}/voronoi/*.pq').collect().write_parquet(f'{DATA_DIR}/dk_adr_voronoi.pq')