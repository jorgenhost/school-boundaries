import geopandas as gpd
import utils
import os

SRC_DIR =  os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SRC_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')


gdf_adr = gpd.read_parquet(f'{DATA_DIR}/dk_adresser_voronoi_no_interior.pq')

# Define zoom coordinates
zoom_xlim = (721305, 721575)
zoom_ylim = (6177113, 6177353)

fig, ax = utils.plot_voronoi_with_inset(gdf = gdf_adr, kommunekode = 147, zoom_xlim=zoom_xlim, zoom_ylim=zoom_ylim)

fig.savefig(f'figs/voronoi_tess_fberg.pdf', bbox_inches = 'tight')
fig.savefig(f'figs/voronoi_tess_fberg.svg', bbox_inches = 'tight')


gdf_adr = gdf_adr[gdf_adr['kommunekode']==147].reset_index(drop=True)
gdf_school = gdf_adr.sample(6, random_state=1234).reset_index(drop=True)

gdf_adr['school_district'] = utils.assign_school(gdf_adr = gdf_adr, gdf_school = gdf_school)
fig,ax = utils.plot_school_districts(gdf_adr = gdf_adr, gdf_school = gdf_school)
fig.savefig(f'figs/voronoi_tess_fberg_districts.pdf', bbox_inches = 'tight')
fig.savefig(f'figs/voronoi_tess_fberg_districts.svg', bbox_inches = 'tight')
