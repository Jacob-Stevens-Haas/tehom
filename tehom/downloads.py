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

import wget
import pandas as pd
import numpy as np
import spans

from matplotlib.figure import Figure as MFigure
from pandas._libs.tslibs.timedeltas import Timedelta
from pandas._libs.tslibs.timestamps import Timestamp
from pandas.core.frame import DataFrame
from plotly.graph_objs._figure import Figure as PFigure
from onc.onc import ONC

from . import _persistence

# from zipfile import ZipFile


ais_site = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/"
onc = ONC(
    _persistence.load_user_token(),
    showInfo=True,
    outPath=str(_persistence.ONC_DIR),
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
    _persistence.init_data_folder()
    _persistence.init_ais_db(_persistence.AIS_DB)
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
    # JMSH: morgan. MWM, 07/23/2021: Done.
    url = (
        f"https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2015/AIS_{year}_  "
        f"                       {month}_{zone}.zip"
    )
    wget.download(url, _persistence.AIS_TEMP_DIR)
    return _persistence.AIS_TEMP_DIR


def _unzip_ais(zipfile: Path) -> Tuple[Path]:
    """Unzips the temporary zipfile

    Arguments:
        zipfile: file to unzip

    Returns:
        tuple comprising the root of the unzip tree and the specific
        unzipped file of interest
    """
    # JMSH: morgan.
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

    files = []
    for hphone in hydrophones:
        request = onc.requestDataProduct(
            filters={
                "dataProductCode": code_from_extension(extension),
                "extension": extension,
                "dateFrom": _onc_iso_fmt(begin),
                "dateTo": _onc_iso_fmt(end),
                "deviceCode": hphone,
                "dpo_hydrophoneDataDiversionMode": "OD",
            }
        )
        req_id = request["dpRequestId"]
        run_ids = onc.runDataProduct(req_id)["runIds"]
        for id in run_ids:

            downloads = onc.downloadDataProduct(id, includeMetadataFile=False)
            files += [
                download["file"]
                for download in downloads
                if download["status"] == "complete" and download["downloaded"]
            ]
    _update_onc_tracker(_persistence.ONC_DB, files)


def _update_onc_tracker(onc_db: Path, files: List[Path]) -> None:
    """Updates the ONC database to track downloads

    Arguments:
        onc_db: database to track ONC downloads
        files: list of files downloaded to add to the tracker
    """
    pass


def _get_onc_downloads(onc_db: Path) -> Set:
    """Identify which ONC hydrophone data ranges have been downloaded
    and tracked in the ONC database.

    Arguments:
        onc_db: path to the database of ONC records

    Returns:
        set of records, each arragned as a tuple comprising (hydrophone,
        begin, end, extension)
    """
    pass


@lru_cache(maxsize=1)
def _get_deployments():
    hphones = onc.getDeployments(filters={"deviceCategoryCode": "HYDROPHONE"})
    return pd.DataFrame(hphones)


def _onc_iso_fmt(dt: Union[Timestamp, str]) -> str:
    """Formats the datetime according to how ONC needs it in requests."""
    dt = np.datetime64(dt, "ms")
    return np.datetime_as_string(dt, timezone="UTC")


def show_available_data(
    begin: Union[datetime, str, Timestamp],
    end: Union[datetime, str, Timestamp],
    bottomleft: Tuple[float] = (-90.0, -180.0),
    topright: Tuple[float] = (90.0, 180.0),
    style: str = "map",
    ais_db: Path = _persistence.AIS_DB,
    onc_db: Path = _persistence.ONC_DB,
) -> Union[MFigure, PFigure]:
    """Creates a visualization of what data is available to download and
    what data is available locally for sampling.

    Parameters:
        begin: start time to display
        end: end time to display
        bottomleft: Latitude, longitude tuple.  Only
            include hydrophones to the northeast of this point.
        topright: Latitude, longitude tuple.  Only
            include hydrophones to the southwest of this point.
        style: Either 'map' for a geographic map with hydrophones
            identified or 'bar' for a bar chart showing overlapping
            downloads.
        ais_db: path to the database of AIS records
        onc_db: database to track ONC downloads

    Returns:
        A plotly figure if ``style='map'`` or a matplotlib figure if
        ``style='bar'``
    """
    # Ok, Morgan, this should dispatch to two different helper functions
    # depending on the `style` argument.  Brief descriptions follow.

    # Map style:
    # Simpler. Basically a scatterplot of hydrophone locations on a map
    # of the pacific northwest.  Hydrophone locations and deployment
    # times available in the _get_deployments() function.  You can build
    # Geo scatter plot with plotly.graph_objects.Scattergeo().  See
    # https://plotly.com/python/map-configuration/ for examples
    #
    # Bar style:
    # Something like matplotlib.pyplot.barh()
    # A horizontal bar chart that shows both AIS and ONC data available.
    # Y axis is a set of bars for each hydrophone, organized by UTM zone
    # (ONC hydrophones are exclusively in UTM zones 9 and 10).  X axis
    # is date.  When a single hydrophone has multiple deployments,
    # it's row should have multiple bars.  Behind the hydrophone bars,
    # there should be a different-colored bar chart for AIS data
    # availability, built off of _get_ais_downloads().
    #
    # Once ONC data is downloaded, depending on the format, it should
    # have differently-colored bars overlapping the data-availability
    # bars.  See _get_onc_downloads().
    #
    # In addition, each row should have a label of the hydrophone name
    # aligned to the left side of the row.
    #
    # The trick here is managing the spacing for all the different bars,
    # of which there could be dozens.  Try to find a good way of
    # calculating the appropriate height for each bar based upon the
    # number of hydrophones in each zone.
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


def sample(
    hydrophones: List[str],
    begin: Union[datetime, str, Timestamp],
    end: Union[datetime, str, Timestamp],
    sample_params: SampleParams,
    ais_db: Path = _persistence.AIS_DB,
    onc_db: Path = _persistence.ONC_DB,
    verbose: bool = False,
) -> DataFrame:
    """Sample the downloaded acoustics and AIS data to create a labeled
    dataset for machine learning, ready for ``model.fit()``.

    Parameters:
        hydrophones: List of hydrophone names (what ONC
            calls 'deviceCode's) to sample.
        begin: start time for sample
        end: end time for sample
        sample_params (SampleParams): parameters for repeatable or
            related data samples.
        ais_db: path to the database of AIS records
        onc_db: database to track ONC downloads
        verbose: heavy print output for debugging.

    Returns:
        DataFrame indexed by (hydrophone, time), a column for acoustic
        data as a numpy array, and columns for each label
    """
    overlaps = _determine_data_overlaps(
        hydrophones,
        sample_params.extension,
        begin,
        end,
        ais_db,
        onc_db,
    )
    samples = []
    for hydrophone, tranges in overlaps:
        for trange in tranges:
            times = _choose_sample_times(
                trange, sample_params.duration, sample_params.interval
            )
            file_dict = {
                time: _get_sample_filepaths(
                    time,
                    sample_params.duration,
                    hydrophone,
                    sample_params.extension,
                    onc_db,
                )
                for time in times
            }
            acoustic_array_dict = {
                time: _stitch_files_to_array(
                    files,
                    time,
                    sample_params.duration,
                    sample_params.extension,
                )
                for time, files in file_dict.items()
            }
            x_vals = pd.Series(
                acoustic_array_dict,
                name="x",
            )

            lat, lon = _get_hphone_posit(hydrophone, list(times)[0])
            filtered_ais = _get_ais_data(
                trange.lower, trange.higher, lat, lon, ais_db
            )
            interpolated_ships = _interpolate_and_group_ais(
                filtered_ais, times
            )
            labels = interpolated_ships.apply(
                lambda df: _ais_labeler(df, sample_params)
            )
            labels["hydrophone"] = hydrophone
            samples += labels.join(x_vals).reset_index(["hydrophone", "time"])
    samples = pd.DataFrame().append(samples)
    samples["x"] = _truncate_equal_shapes(samples["x"])
    samples = samples.dropna(subset=["x"])
    return samples


def _determine_data_overlaps(
    hydrophones: List[str],
    extension: str,
    begin: Union[datetime, str, Timestamp],
    end: Union[datetime, str, Timestamp],
    ais_db: Path = _persistence.AIS_DB,
    onc_db: Path = _persistence.ONC_DB,
) -> Set[Tuple[str, spans.datetimerangeset]]:
    """Identify the ranges of overlapping downloaded ONC and AIS data.

    Arguments:
        hydrophones: List of hydrophone names (what ONC
            calls 'deviceCode's) to sample.
        extension: File type, e.g. "mp3"
        begin: start time for sample
        end: end time for sample
        ais_db: path to the database of AIS records
        onc_db: database to track ONC downloads

    Returns:
        A set of tuples, each comprising:
        - The hydrophone deviceCode
        - The datetimerangeset of overlapping intervals
    """
    pass


def _choose_sample_times(
    trange: spans.datetimerange,
    duration: pd.Timedelta,
    interval: pd.Timedelta,
) -> Set[pd.Timestamp]:
    """Calculate evenly-spaced sample times.

    Sample times must be a) no earlier than duration after the lower
    bound of the range and separated by no less than interval.

    Arguments:
        trange: the range of times to sample from
        duration: The duration of observations.
        interval: the time between observations.

    Returns:
        Set of times.
    """
    pass


def _get_sample_filepaths(
    time: pd.Timestamp,
    duration: pd.Timedelta,
    hydrophone: str,
    extension: str,
    onc_db: Path,
) -> List[Path]:
    """Identifies the filepaths to an acoustic data observation.

    Files should contain the file that occurs at given time, the time
    at time minus duration, and any files in between.

    Arguments:
        time: the finish time of the observation
        duration: the duration of the observation
        hydrophone: which hydrophone sample comes from
        extension: file type extension
        onc_db: database to track ONC downloads

    Returns:_truncate_equal_shapes
        List of files, ordered from first to last chronologically.
    """
    pass


def _stitch_files_to_array(
    files: List[Path],
    time: pd.Timestamp,
    duration: pd.Timedelta,
    extension: str,
) -> np.ndarray:
    """Turn the acoustic data for an observation into a numpy array

    Arguments:
        files: the files, in sequence from earliest to latest, that
            comprise the observation
        time: the time of the observation
        duration: the length of the observation (finishing at ``time``)

    Returns:
        The acoustic wave or spectrogram as an array

    Todo:
        standardize the array numerical format (uint8? float? int16?)
    """
    pass


def _get_hphone_posit(
    hydrophone: str, time: pd.Timestamp
) -> Tuple[float, float]:
    """Determine hydrophone position at a specific time.

    Hydrophones do not move frequently, but they are repositioned
    between deployments

    Arguments:
        hydrophone: which hydrophone to get the location of
        time: time to identify where the hydrophone was

    Returns:
        tuple of lat, lon
    """
    pass


def _get_ais_data(
    begin: pd.Timestamp,
    end: pd.Timestamp,
    lat: float,
    lon: float,
    ais_db: Path,
) -> pd.DataFrame:
    """Returns the ais records filtered by time and location

    Only records within a 40x40 NM square box, centered on the lat and
    lon, and between 1 hr before begin and 1 hr after end, are returned.

    Arguments:
        begin: The starting time for observations.
        end: The ending time for observations
        lat: latitude coordinate of the center of area of interest
        lon: longitude coordinate of the center of area of interest
        ais_db: path to the database of AIS records

    Returns:
        DataFrame in same structure as stored ship records, but with
        datetime strings converted to pd.Timestamp/np.datetime64
    """
    pass


def _interpolate_and_group_ais(
    ais_df: pd.DataFrame, times
) -> pd.core.groupby.generic.DataFrameGroupBy:
    """Interpolate the lat/lon of ships to the specified time.

    Interpolation rules:
        A ship has observations near the specified time, before and
            after: linear interpolation
        A ship has one observation very near the specified time, either
            before or after, but not both: constant interpolation
        A ship does not meet above criteria: do not create an
            interpolated record for this ship at this time

    Note:
        What counts as "near" and "very near" is subject to change and
        may be refactored out into an interpolation parameters object

    Arguments:
        ais_df: ship records, including a basedatetime column.
        times: when to interpolate the ship positions.

    Returns:
        The interpolated records, grouped by time.
    """
    # Morgan, you'll have to first groupby mmsi (ship unique id) and
    # then apply an interpolation function for each timepoint.  The
    # interpolation function will take a dataframe and a timepoint,
    # and will determine, based on the nearest records before/after
    # the timepoint, which interpolation rule to apply.
    #
    # While this function sounds like it takes a long time, its ok at
    # the outset to accomplish this somewhat inefficiently.
    pass


def _ais_labeler(
    ais_df: pd.DataFrame,
    sample_params: SampleParams,
) -> pd.Series:
    """Calculate the labels appropriate for a single point in time

    Arguments:
        ais_df: The ship records interpolated to a single point in time
        sample_params: What kinds of labels to apply

    Returns:
        A record of labels.
    """
    pass


def _truncate_equal_shapes(ser: pd.Series[np.ndarray]) -> pd.Series:
    """Truncate most of the data arrays and reject the others

    Using an outlier criterion for thresholding, reject all arrays of a
    shape smaller in a dimension than the threshold.  Truncate all
    larger arrays to the threshold.  This usually solves the issue
    whereby sample points and byte times don't line up perfectly,
    and some arrays are slightly off.  Outliers are usually caused by
    some degenerate condition of the data, and should be rejected.

    Arguments:
        ser: a Series of data arrays

    Returns:
        The same series, but with individual elements either truncated
        replaced with None, as appropriate

    """
    pass
