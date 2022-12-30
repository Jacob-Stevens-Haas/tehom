[![Documentation Status](https://readthedocs.org/projects/tehom/badge/?version=latest)](https://tehom.readthedocs.io/en/latest/?badge=latest)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![PyPI version](https://badge.fury.io/py/tehom.svg)](https://badge.fury.io/py/tehom)
[![Downloads](https://pepy.tech/badge/tehom)](https://pepy.tech/project/tehom)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
# Tehom - Machine Learning on Underwater Acoustics

This package facilitates the creation of machine learning training data on
underwater acoustics.  While the raw data is available from Marine Cadastre and
Ocean Networks Canada (via the `onc` package), `tehom` tracks downloads that it conducts
and exposes more useful queries for data exploration and sampling.  e.g.:

> "What hydrophones outside Vancouver have data during April-June, 2017?"

> "Where have I downloaded time/geo overlapping data?"

> "Give me acoustic snippets as numpy arrays, labeled with whether a container
ship was close to the hydrophone that recorded the snippet."

## Requirements
- `sqlite3` available as a command line program
- Create [Ocean Networks Canada account](https://data.oceannetworks.ca/Login?service=https://data.oceannetworks.ca/LandingPage)
and get your API token [here](https://data.oceannetworks.ca/Profile#api_tab>)

## How to:
example.ipynb shows the basic data access and navivgation.  Before working with any ONC
data, you must once run
```python
python -m tehom save-token <token>
```

Useful additional commands to start with:

- `tehom.download_ships`
- `tehom.download_acoustics`
- `tehom.downloads.get_ais_downloads`
- `tehom.downloads.get_onc_downloads`
- `tehom.downloads.get_audio_availability`
- `tehom.downloads.show_available_data` (plotting, use Jupyter terminal)
- `tehom.sample` (in progress!)
- 
In addition to `save-token`, when run as a module/CLI, `tehom` can also download data.

## About
"Tehom" is the Hebrew word for abyss, specifically the dark, chaotic oceans from which
order and the world emerged.  