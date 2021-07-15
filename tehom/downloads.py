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
import shutil

from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple, Union, Set
from pathlib import Path
from functools import lru_cache

import pandas as pd

from matplotlib.figure import Figure as MFigure
from pandas._libs.tslibs.timedeltas import Timedelta
from pandas._libs.tslibs.timestamps import Timestamp
from pandas.core.frame import DataFrame
from plotly.graph_objs._figure import Figure as PFigure
from onc.onc import ONC

from . import _persistence

ais_site = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/"
onc = ONC(
    _persistence.load_user_token(),
    showInfo=True,
    outpath=str(_persistence.ONC_DIR),
)


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
    if (year, month, zone) not in _get_ais_downloads(_persistence.AIS_DB):
        zipfile_path = _download_ais_to_temp(year, month, zone)
        unzipped_tree, unzipped_target = _unzip_ais(zipfile_path)
        failure = _load_ais_csv_to_db(unzipped_target)
        if not failure:
            shutil.rmtree(unzipped_tree)
        else:
            raise RuntimeError(
                "Failed to load data to database; check format of"
                f" {unzipped_target}"
            )
    else:
        print(f"AIS data already stored for {year}, {month} zone {zone}.")
    pass


def _download_ais_to_temp(year: int, month: int, zone: int) -> Path:
    """Downloads AIS records from Marine Cadastre.

    Arguments:
        year: year to download
        month: month to download
        zone: UTM zone to download

    Returns:
        location of download result
    """
    # morgan
    pass


def _unzip_ais(zipfile: Path) -> Tuple[Path]:
    """Unzips the temporary zipfile

    Arguments:
        zipfile: file to unzip

    Returns:
        tuple comprising the root of the unzip tree and the specific
        unzipped file of interest
    """
    # morgan
    pass


def _get_ais_downloads(ais_db: Path) -> Set:
    """Identify which AIS year-month-zone combinations have already been
    added to the AIS database

    Arguments:
        ais_db: path to the database of AIS records

    Returns:
        set of records, each arragned as a tuple comprising (year,
        month, zone)
    """
    pass


def _load_ais_csv_to_db(csvfile: Path, ais_db: Path) -> int:
    """Loads the AIS records from the given file into the appropriate
    table in ais_db and updates the metadata table in ais_db

    Arguments:
        csvfile: location of AIS records to add
        ais_db: location of AIS database to update

    Returns:
        Return value from the sqlite subprocess
    """
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

    def code_from_extension(ext):
        if ext.lower() in ["mp3", "wav", "flac"]:
            return "AD"
        elif ext.lower() in ["png"]:
            return "HSD"

    for hphone in hydrophones:
        request = onc.requestDataProduct(
            filters={
                "dataProductCode": code_from_extension(extension),
                "extension": extension,
                "dateFrom": str(begin),
                "dateTo": str(end),
                "deviceCode": hphone,
            }
        )
        run_id = request["dpRequestId"]
        result = onc.runDataProduct(run_id)
        if result != 200:
            raise RuntimeError(
                f"Received HTTP status code {result} when downloading ONC data"
            )
        files = onc.downloadDataProduct(run_id)
        _update_onc_tracker(_persistence.ONC_DB, files)


def _update_onc_tracker(onc_db: Path, files: List[Path]) -> None:
    """Updates the ONC database to track downloads

    Arguments:
        onc_db: database to track ONC downloads
        files: list of files downloaded to add to the tracker
    """
    pass


@lru_cache(maxsize=1)
def _get_deployments():
    hphones = onc.getDeployments(filters={"deviceCategoryCode": "HYDROPHONE"})
    return pd.DataFrame(hphones)


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
