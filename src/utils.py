import os

os.environ['POLARS_MAX_THREADS'] = '16'

import polars as pl
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from functools import lru_cache
from scipy.spatial.distance import cdist
import numpy as np


@lru_cache(maxsize=1)
def load_kommune_data(clean: bool = False):
    if clean is True:
        gdf_kom = gpd.read_parquet('data/dk_kom_geo.pq')
    else: 
        gdf_kom = gpd.read_parquet('data/dk_kom_geo_raw.pq')
    return gdf_kom

def parse_voronoi():

    df = (pl.scan_parquet('data/dk_adr.pq', low_memory = True)
            .select(pl.col("kommunekode"), pl.col("etrs89koordinat_øst").alias("etrs89_east"), pl.col("etrs89koordinat_nord").alias("etrs89_north"))
            .filter(pl.struct(pl.col("etrs89_east", "etrs89_north")).is_first_distinct())
            .with_columns(access_address_id = pl.struct(pl.col("etrs89_east", "etrs89_north")).hash().rank('dense').shrink_dtype())
            .collect(engine = 'streaming')
            .to_pandas()
    )

    gdf_adr = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(x = df.etrs89_east, y=df.etrs89_north), crs=25832)


    voronoi_polys = gpd.GeoDataFrame(geometry=gdf_adr.voronoi_polygons())
    gdf_adr = gdf_adr.sjoin(voronoi_polys, how = 'right', predicate='within').drop(['index_left'], axis=1)
    gdf_adr['point'] = gpd.points_from_xy(x = gdf_adr.etrs89_east, y=gdf_adr.etrs89_north, crs = 25832)
    gdf_adr.to_parquet('data/dk_adr_voronoi.pq')

    print('Parsed Danish addresses as voronoi polygons.')

def plot_voronoi_with_inset(
    gdf: gpd.GeoDataFrame,
    kommunekode: int,
    zoom_xlim: tuple[float, float],
    zoom_ylim: tuple[float, float],
    inset_pos: tuple[float, float, float, float] = (0.75, 0.7, 0.3, 0.3),
    linewidth_main: float = 0.25,
    linewidth_zoom: float = 0.8
):
    """
    Plot a Voronoi tessellation for a given kommune with an inset zoom.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        Input GeoDataFrame with geometries.
    kommunekode : int
        Kommune code to filter on.
    zoom_xlim : list
        [xmin, xmax] for inset zoom.
    zoom_ylim : list
        [ymin, ymax] for inset zoom.
    inset_pos : list
        Position of inset axes in figure coords: [x, y, width, height].
    linewidth_main : float
        Line width for main plot edges.
    linewidth_zoom : float
        Line width for inset plot edges.
    savepath : str or None
        Base path for saving (without extension). If None, figure is just shown.

    Returns
    -------
    fig, ax : matplotlib Figure and Axes
    """
    fig, ax = plt.subplots(figsize=(12, 8))

    # Filter kommune
    gdf_kom = gdf[gdf['kommunekode'] == kommunekode].reset_index(drop=True)
    gdf_kom.plot(ax=ax, facecolor='none', edgecolor='k', linewidth=linewidth_main)
    ax.set_axis_off()

    # Inset axes
    axins = ax.inset_axes(inset_pos)

    # Filter zoom area
    gdf_zoom = gdf_kom.cx[zoom_xlim[0]:zoom_xlim[1], zoom_ylim[0]:zoom_ylim[1]]
    gdf_zoom.plot(ax=axins, facecolor='none', edgecolor='k', linewidth=linewidth_zoom)

    # Set limits
    axins.set_xlim(zoom_xlim)
    axins.set_ylim(zoom_ylim)

    # Remove ticks
    axins.set_xticklabels([])
    axins.set_yticklabels([])

    # Style inset border
    for spine in axins.spines.values():
        spine.set_edgecolor('black')
        spine.set_linewidth(1.5)

    # Connector lines
    ax.indicate_inset_zoom(axins, edgecolor="k", linewidth=2, linestyle='--', alpha=1)

    plt.tight_layout()
    plt.close()

    return fig, ax

def make_custom_palette(n, saturation=0.65, value=0.9):
    """
    Generate n distinct colors using HSV evenly spaced around the hue circle.
    """
    hues = np.linspace(0, 1, n, endpoint=False)
    return [mcolors.hsv_to_rgb((h, saturation, value)) for h in hues]


def plot_school_districts(
    gdf_adr: gpd.GeoDataFrame,
    gdf_school: gpd.GeoDataFrame,
    district_col: str = "school_district",
    colors: list | None = None,
    cmap: str = "tab10",
    figsize: tuple = (15, 9)):
    """
    Plot school district assignment with colored polygons and school seeds.

    Parameters
    ----------
    gdf_adr : gpd.GeoDataFrame
        GeoDataFrame of addresses (with a 'district_col' column).
    gdf_school : gpd.GeoDataFrame
        GeoDataFrame with school locations (expects 'point' geometry).
    district_col : str
        Column with school district assignment (int).
    colors : list or None
        List of colors to use. If None, colors are taken from cmap.
    cmap : str
        Matplotlib colormap name (used if colors=None).
    figsize : tuple
        Size of figure.
    savepath : str or None
        Base path for saving (without extension). If None, figure is just shown.

    Returns
    -------
    fig, ax : matplotlib Figure and Axes
    """
    fig, ax = plt.subplots(figsize=figsize)
    ax.set_axis_off()

    # Plot base geometry
    gdf_adr.plot(ax=ax, facecolor="none", edgecolor="k", linewidth=0.3)

    # Determine districts
    districts = sorted(gdf_adr[district_col].unique())
    n_districts = len(districts)

    # Handle color scheme
    if colors is None:
        colors = make_custom_palette(n_districts)

    # Plot each district
    for i, d in enumerate(districts):
        gdf_adr[gdf_adr[district_col] == d].plot(
            ax=ax, facecolor=colors[i], alpha=0.6, edgecolor="none"
        )

    # Plot school seeds
    gdf_school["point"].plot(
    ax=ax,
    color = 'white',
    edgecolor="black",
    markersize=120,
    marker="^",
    label="Schools"
    )

    # Add legend
    ax.legend()
    plt.tight_layout()
    plt.close()

    return fig, ax


def assign_school(
    gdf_adr: gpd.GeoDataFrame,
    gdf_school: gpd.GeoDataFrame,
    prob: bool = False,
    probs: list = [0.85, 0.1, 0.05],
    n_neighbors: int = 3,
    east_col: str = "etrs89_east",
    north_col: str = "etrs89_north"
) -> np.ndarray:
    """
    Assign each address to a school based on proximity.

    Parameters
    ----------
    gdf_adr : gpd.GeoDataFrame
        GeoDataFrame with address coordinates.
    gdf_school : gpd.GeoDataFrame
        GeoDataFrame with school coordinates.
    prob : bool, default=True
        If True, assign probabilistically among nearest schools.
        If False, assign deterministically to the nearest school.
    probs : list, default=[0.85, 0.1, 0.05]
        Probabilities for choosing among nearest schools (used if prob=True).
        Must sum to 1 and match n_neighbors length.
    n_neighbors : int, default=3
        Number of nearest schools to consider.
    east_col : str, default="etrs89_east"
        Column name for x-coordinate.
    north_col : str, default="etrs89_north"
        Column name for y-coordinate.

    Returns
    -------
    np.ndarray
        Array of assigned school indices (aligned with gdf_adr).
    """
    # Coordinates
    p1 = gdf_adr[[east_col, north_col]].to_numpy()
    p2 = gdf_school[[east_col, north_col]].to_numpy()

    # Distance matrix
    distances = cdist(p1, p2)

    # Get indices of nearest schools
    nearest_schools = np.argsort(distances, axis=1)[:, :n_neighbors]

    if prob:
        if len(probs) != n_neighbors:
            raise ValueError("Length of probs must match n_neighbors")
        if not np.isclose(sum(probs), 1.0):
            raise ValueError("Probabilities must sum to 1")

        assignments = [
            np.random.choice(nearest_schools[i], p=probs)
            for i in range(len(nearest_schools))
        ]
        school_assignment = np.array(assignments)

    else:
        school_assignment = np.argmin(distances, axis=1)

    return school_assignment

def concat_geo_data(list: list[str] | str, path):
    if len(list)==1:
        gdf = gpd.read_parquet(list[0])
        gdf = gdf.reset_index(drop = True)
        gdf.to_parquet(f'{path}')
    else:
        gdf = gpd.read_parquet(list[0])
        for data in list:
            if data == list[0]:
                pass
            gdf2 = gpd.read_parquet(data)

            gdf = pd.concat([gdf, gdf2])

            gdf = gdf.reset_index(drop = True)
        gdf.to_parquet(f'{path}')
