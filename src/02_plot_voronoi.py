import geopandas as gpd
import matplotlib.pyplot as plt

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