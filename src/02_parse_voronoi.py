import os
import polars as pl
import polars_st as st

SRC_DIR =  os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SRC_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')


lf_kom = pl.scan_parquet(f'{DATA_DIR}/dk_kom_geo_raw_after_2007.pq').select(pl.col("kom"), st.geom("geometry_kom").st.set_srid(25832))


lf = (pl.scan_parquet(f'{DATA_DIR}/dk_adr.pq')
        .select(pl.col("kommunekode").alias("kom"), pl.col("etrs89koordinat_øst").alias("etrs89_east"), pl.col("etrs89koordinat_nord").alias("etrs89_north"))
        .filter(pl.struct(pl.col("etrs89_east", "etrs89_north")).is_first_distinct())
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

lf.sink_parquet(f'{DATA_DIR}/dk_adr_voronoi.pq', engine = 'streaming')

print(f'Clipped voronoi polygons.')