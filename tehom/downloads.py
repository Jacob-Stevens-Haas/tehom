"""Create data samples of labeled acoustics

Datalib calls on hydrophone data from Ocean Networks Canada and
Automated Information System records from Marine Cadastre to
label acoustic recordings for machine learning.

With the datalib, you can:

* Download blocks of AIS records
* Download hydrophone recordings
* Show the data either available for download or already downloaded
* Sample downloaded data into a labeled format, ready for ``model.fit``
"""
from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple, Union

import pandas as pd

from matplotlib.figure import Figure as MFigure
from pandas._libs.tslibs.timedeltas import Timedelta
from pandas._libs.tslibs.timestamps import Timestamp
from pandas.core.frame import DataFrame
from plotly.graph_objs._figure import Figure as PFigure

from . import _persistence

ais_site = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/"


def download_ships(year: int, month: int, zone: int) -> None:
    """Download AIS records from Marine Cadastre.  Records are stored in
    a local sqlite database.  Marine Cadastre organizes records into
    csv files by year, month, and UTM zone
    (wikipedia the Universal_Transverse_Mercator_coordinate_system)

    Parameters:
        year (int): year to download
        month (int): month to download
        zone (int): UTM zone to download
    """
    _persistence._init_data_folder()
    _persistence._init_ais_db(_persistence.AIS_DB)
    pass


def download_acoustics(
    hydrophones: List[str],
    begin: Union[datetime, str, Timestamp],
    end: Union[datetime, str, Timestamp],
    extension: str,
) -> None:
    """Download acoustic data from ONC.  Data is stored in files in the
    package data folder, but metadata and paths are stored in a local
    sqlite database.

    Parameters:
        hydrophones (List[str]): A list of hydrophone names, which ONC
            refers to as 'deviceCode's.
        begin (Union[datetime, str, pd.Timestamp]): start time to
            download.  Will download the file that starts before this
            time but finishes after.
        end (Union[datetime, str, Timestamp]): end time to
            download.  Will download the file that ends after this
            time but begins before.
        extension (str): The file type to download the acoustics.  Can
            be mp3, wav, png, or mat
    """
    _persistence._init_data_folder()
    _persistence._init_onc_db(_persistence.ONC_DB)
    pass


def show_available_data(
    begin: Union[datetime, str, Timestamp],
    end: Union[datetime, str, Timestamp],
    bottomleft: Tuple[float] = (-90.0, -180.0),
    topright: Tuple[float] = (90.0, 180.0),
    style: str = "map",
) -> Union[MFigure, PFigure]:
    """Creates a visualization of what data is available to download and
    what data is available locally for sampling.

    Parameters:
        begin (Union[datetime, str, Timestamp]): start time to display
        end (Union[datetime, str, Timestamp]): end time to display
        bottomleft (Tuple[float]): Latitude, longitude tuple.  Only
            include hydrophones to the northeast of this point.
        topright (Tuple[float]): Latitude, longitude tuple.  Only
            include hydrophones to the southwest of this point.
        style (str): Either 'map' for a geographic map with hydrophones
            identified or 'bar' for a bar chart showing overlapping
            downloads.

    Returns:
        A plotly figure if ``style='map'`` or a matplotlib figure if
        ``style='bar'``
    """
    pass


@dataclass
class SampleParams:
    """Settings for repeatable data samples.  For you probability nerds,
    this should control the X in X(omega), while the other parameters to
    sample() control the omega.
    """

    interval: Union[str, Timedelta] = "5 min"
    duration: Union[str, Timedelta] = "1 second"
    extension: str = "mp3"

    interval = pd.Timedelta(interval)
    duration = pd.Timedelta(duration)
    if extension not in ["mp3"]:
        raise ValueError("Only extension with sample() implemented is mp3")


def sample(
    hydrophones: List[str],
    begin: Union[datetime, str, Timestamp],
    end: Union[datetime, str, Timestamp],
    sample_params: SampleParams,
    verbose: bool = False,
) -> DataFrame:
    """Sample the downloaded acoustics and AIS data to create a labeled
    dataset for machine learning, ready for ``model.fit()``.

    Parameters:
        hydrophones (List[str]): List of hydrophone names (what ONC
            calls 'deviceCode's) to sample.
        begin (Union[datetime, str, Timestamp]): start time for sample
        end (Union[datetime, str, Timestamp]): end time for sample
        sample_params (SampleParams): parameters for repeatable or
            related data samples.
        verbose (bool): heavy print output for debugging.

    Returns:
        DataFrame indexed by (hydrophone, time), a column for acoustic
        data as a numpy array, and columns for each label
    """
    pass