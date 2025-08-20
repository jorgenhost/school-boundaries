import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
from scipy.spatial.distance import cdist

fig, ax = plt.subplots(figsize = (12, 8))

gdf = gpd.read_parquet('data/dk_adresser_voronoi.pq')
gdf_kom = gdf[gdf['kommunekode']==147].reset_index(drop=True)
gdf_kom.plot(ax = ax, facecolor='none', edgecolor='k', linewidth=0.25)
ax.set_axis_off()

# Define zoom coordinates
zoom_xlim = [721305, 721575]
zoom_ylim = [6177113, 6177353]

# Create inset axes for the zoom
# [x_position, y_position, width, height] in axes coordinates (0-1)
axins = ax.inset_axes([0.75, 0.7, 0.3, 0.3])

# Filter data for the zoom area
gdf_zoom = gdf_kom.cx[zoom_xlim[0]:zoom_xlim[1], zoom_ylim[0]:zoom_ylim[1]]

# Plot the zoomed data
gdf_zoom.plot(ax=axins, facecolor='none', edgecolor='k', linewidth=0.8)

# Set the zoom limits
axins.set_xlim(zoom_xlim)
axins.set_ylim(zoom_ylim)

# Remove tick labels from inset
axins.set_xticklabels([])
axins.set_yticklabels([])

# Add a border around the inset
for spine in axins.spines.values():
    spine.set_edgecolor('black')
    spine.set_linewidth(1.5)

# Draw lines connecting the main plot to the inset
ax.indicate_inset_zoom(axins, edgecolor="k", linewidth=2, linestyle='--', alpha=1)

plt.tight_layout()
plt.show()
fig.savefig('figs/voronoi_tess_fberg.pdf', bbox_inches = 'tight')
fig.savefig('figs/voronoi_tess_fberg.png', bbox_inches = 'tight')

##############################
## RANDOM SCHOOL ASSIGNMENT ##
##############################

n_districts = 8

# Randomly select seed polygons as schools
school_seeds = gdf_kom.sample(n=n_districts, random_state=1234).copy().reset_index(drop=True)
school_seeds['school_district'] = range(n_districts)

def assign_school(gdf_adr: gpd.GeoDataFrame, gdf_school: gpd.GeoDataFrame, prob: bool = True) -> gpd.GeoSeries:
    p1 = gdf_adr[['etrs89_east', 'etrs89_north']].to_numpy()

    p2 = gdf_school[['etrs89_east', 'etrs89_north']].to_numpy()

    distances = cdist(p1, p2)

    # Get indices of the 3 nearest schools for each address
    nearest_schools = np.argsort(distances, axis=1)[:, :3]
    

    if prob is True:
        # Probabilities for 1st, 2nd, 3rd nearest
        probs = [0.85, 0.1, 0.05]

        # Randomly assign based on probabilities
        assignments = [
            np.random.choice(nearest_schools[i], p=probs)
            for i in range(len(nearest_schools))
        ]

        school_assignment = np.array(assignments)
    
    else:
        school_assignment = np.argmin(distances, axis=1)

    return school_assignment

gdf_kom['school_district'] = assign_school(gdf_adr=gdf_kom, gdf_school=school_seeds, prob = False)


fig, ax = plt.subplots(figsize = (12, 8))
gdf_kom.plot(ax = ax, facecolor='none', edgecolor='k', linewidth=0.25)
ax.set_axis_off()

# Define zoom coordinates
zoom_xlim = [721305, 721575]
zoom_ylim = [6177113, 6177353]

# Create inset axes for the zoom
# [x_position, y_position, width, height] in axes coordinates (0-1)
axins = ax.inset_axes([0.75, 0.7, 0.3, 0.3])

# Filter data for the zoom area
gdf_zoom = gdf_kom.cx[zoom_xlim[0]:zoom_xlim[1], zoom_ylim[0]:zoom_ylim[1]]

# Plot the zoomed data
gdf_zoom.plot(ax=axins, facecolor='none', edgecolor='k', linewidth=0.8)

# Set the zoom limits
axins.set_xlim(zoom_xlim)
axins.set_ylim(zoom_ylim)

# Remove tick labels from inset
axins.set_xticklabels([])
axins.set_yticklabels([])

# Add a border around the inset
for spine in axins.spines.values():
    spine.set_edgecolor('black')
    spine.set_linewidth(1.5)

# Draw lines connecting the main plot to the inset
ax.indicate_inset_zoom(axins, edgecolor="k", linewidth=2, linestyle='--', alpha=1)

plt.tight_layout()
plt.show()
fig.savefig('figs/voronoi_tess_fberg_school.pdf', bbox_inches = 'tight')
fig.savefig('figs/voronoi_tess_fber_school.png', bbox_inches = 'tight')