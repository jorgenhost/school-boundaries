import os

os.environ['POLARS_MAX_THREADS'] = '16'

import polars as pl
import polars.selectors as cs
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import polars_st as st
from functools import lru_cache
from scipy.spatial.distance import cdist
import numpy as np
import re
from rapidfuzz import process, fuzz

SRC_DIR =  os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SRC_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')

## some quick data parsing ##
def parse_data(old_kom_codes: bool = True):
    df = pl.scan_csv(f'{DATA_DIR}/dk_adr.csv').collect(engine = 'streaming').with_columns(
    cs.integer().shrink_dtype()
    )
    df.write_parquet(f'{DATA_DIR}/dk_adr.pq')
    os.remove(f'{DATA_DIR}/dk_adr.csv')
    
    # From https://www.dst.dk/extranet/staticsites/TIMES3/html/97f2b67b-25e5-424f-ae92-0e4477d5d299.htm
    kom_old = pl.read_csv(f'{DATA_DIR}/kom_old.csv')

    def fuzzy_match(x, choices, scorer=fuzz.WRatio):
        match = process.extractOne(x, choices, scorer=scorer, score_cutoff=80)
        return match[0] if match else None

    if old_kom_codes is True:

        gdf = gpd.read_file(f'{DATA_DIR}/KOMMUNAL_SHAPE_UTM32-EUREF89/Kommune.shp')
        gdf = gdf[gdf['til'].str.contains('2006')].reset_index(drop = True)

        def remove_kommune(text):
            """Remove 'kommune' (case-insensitive) from text"""
            if not text:
                return ""
            return re.sub(r'\bkommune\b', '', str(text), flags=re.IGNORECASE).strip()

        # Preprocess the choices once
        kom_old_names = kom_old["navn"].to_list()
        kom_old_cleaned = [remove_kommune(name) for name in kom_old_names]

        gdf["match"] = gdf["navn"].apply(lambda x: fuzzy_match(remove_kommune(x), kom_old_cleaned))

        # Join back
        gdf = gdf.merge(kom_old.to_pandas()[['navn', 'kom']], left_on='match', right_on='navn', how="left")

        gdf = gdf[['kom', 'geometry']]
        gdf['kom'] = gdf['kom'].astype('int16')
        gdf['geometry'] = gdf['geometry'].force_2d()
        gdf.to_parquet(f'{DATA_DIR}/dk_kom_geo_raw.pq')
    
    else:
        gdf = gpd.read_file(f'{DATA_DIR}/au_inspire.gpkg')
        gdf = st.from_geopandas(gdf).select(pl.col("nationalcode").alias("kom").cast(pl.Int16), st.geom("geometry").alias("geometry_kom").st.set_srid(25832))
        gdf.write_parquet(f'{DATA_DIR}/dk_kom_geo_raw_after_2007.pq')


def load_spatial_pq(path: str, crs: int = 25832):
    return st.from_geopandas(gpd.read_parquet(path).set_crs(crs, allow_override=True))

@lru_cache(maxsize=1)
def load_kommune_data(clean: bool = False):
    if clean is True:
        gdf_kom = gpd.read_parquet('data/dk_kom_geo.pq')
    else: 
        gdf_kom = gpd.read_parquet('data/dk_kom_geo_raw.pq')
    return gdf_kom

def plot_voronoi_with_inset(
    gdf: gpd.GeoDataFrame,
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
    gdf.plot(ax=ax, facecolor='none', edgecolor='k', linewidth=linewidth_main)
    ax.set_axis_off()

    # Inset axes
    axins = ax.inset_axes(inset_pos)

    # Filter zoom area
    gdf_zoom = gdf.cx[zoom_xlim[0]:zoom_xlim[1], zoom_ylim[0]:zoom_ylim[1]]
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

def concat_geo_data(list: list[str] | str, path, save: bool = True):
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
        if save is True:
            gdf.to_parquet(f'{path}')
        else:
            return gdf
