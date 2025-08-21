#!/usr/bin/env -S uv run --script

uv sync
mkdir data
mkdir data/voronoi
mkdir data/voronoi/no_interior
mkrdir data/geometry

# Fetch all adresses
curl -L -o data/dk_adresser.csv "https://api.dataforsyningen.dk/adgangsadresser?&format=csv"

# Fetch administrative boundaries
curl -L -o data/admin_boundaries.zip "https://ftp.sdfe.dk/main.html?download&weblink=60a0dc5e27d9561a3d761e876cb2684d&realfilename=DK%5FAdministrativeUnit%2Ezip"
unzip data/admin_boundaries.zip -d data/
rm data/admin_boundaries.zip

# Run .py-scripts
uv run src/01_kom_features.py
uv run src/02_parse_voronoi.py
uv run src/03_plot_voronoi.py