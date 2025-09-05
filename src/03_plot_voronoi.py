import geopandas as gpd
import polars as pl
import polars_st as st
import utils
import os

SRC_DIR =  os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SRC_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
FIG_DIR = os.path.join(PROJECT_ROOT, 'figs')

kom = 147


lf_adr = (pl.scan_parquet(f'{DATA_DIR}/dk_adr.pq')
            .select(pl.col("kommunekode").alias("kom"), pl.col("etrs89koordinat_øst").alias("etrs89_east"), pl.col("etrs89koordinat_nord").alias("etrs89_north"))
            .filter(pl.struct(pl.col("etrs89_east", "etrs89_north")).is_first_distinct())
            .with_columns(access_address_id = pl.struct(pl.col("etrs89_east", "etrs89_north")).hash().rank('dense').shrink_dtype(),
                          point = st.point(pl.concat_arr("etrs89_east", "etrs89_north")).st.set_srid(25832)
            )
            .select(pl.col("etrs89_east", "etrs89_north", "kom", "access_address_id"), st.geom("point"))
)
df_adr = lf_adr.collect(engine = 'streaming')


lf = pl.scan_parquet(f'{DATA_DIR}/dk_adr_voronoi.pq').filter(pl.col("kom")==kom).select(st.geom("voronoi_clipped").st.set_srid(25832)).st.sjoin(
    df_adr.lazy(), left_on = 'voronoi_clipped', right_on = 'point'
)

geo_cols = ['voronoi_clipped', 'point']
gdf_adr = lf.collect().st.to_geopandas(geometry_name = 'voronoi_clipped')
gdf_adr['point'] = lf.select('point').collect().st.to_geopandas(geometry_name = 'point')

# Define zoom coordinates
zoom_xlim = (721305, 721575)
zoom_ylim = (6177113, 6177353)

fig, ax = utils.plot_voronoi_with_inset(gdf = gdf_adr, zoom_xlim=zoom_xlim, zoom_ylim=zoom_ylim)
fig.savefig(f'{FIG_DIR}/voronoi_tess_fberg.pdf', bbox_inches = 'tight')
fig.savefig(f'{FIG_DIR}/voronoi_tess_fberg.svg', bbox_inches = 'tight')


gdf_school = gdf_adr.sample(6, random_state=1234).reset_index(drop=True)
gdf_adr['school_district'] = utils.assign_school(gdf_adr = gdf_adr, gdf_school = gdf_school)
fig,ax = utils.plot_school_districts(gdf_adr = gdf_adr, gdf_school = gdf_school)

fig.savefig(f'{FIG_DIR}/voronoi_tess_fberg_districts.pdf', bbox_inches = 'tight')
fig.savefig(f'{FIG_DIR}/voronoi_tess_fberg_districts.svg', bbox_inches = 'tight')

# Optional: Compute natural boundaries - difference these polygons from municipality
lf_features = gpd.read_parquet(f'{DATA_DIR}/geometry/kom_features.pq')
