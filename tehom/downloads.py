"""Create data samples of labeled acoustics

Uses hydrophone data from Ocean Networks Canada and
Automated Information System records from Marine Cadastre to
label acoustic recordings for machine learning.

With this module, you can:

* Download blocks of AIS records
* Download hydrophone recordings
* Show the data either available for download or already downloaded
* Sample downloaded data into a labeled format, ready for ``model.fit``
"""
import logging
import shutil
import subprocess
import re
import warnings

from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple, Union, Set
from pathlib import Path
from functools import lru_cache
from zipfile import ZipFile

import requests
import pandas as pd
import numpy as np
import spans
import pytz
import plotly.graph_objects as go

from matplotlib import pyplot as plt
from matplotlib.text import Text
from matplotlib.figure import Figure as MFigure
from pandas._libs.tslibs.timedeltas import Timedelta
from pandas._libs.tslibs.timestamps import Timestamp
from pandas.core.frame import DataFrame
from plotly.graph_objs._figure import Figure as PFigure
from spans import datetimerange

from tehom import _persistence
from tehom._persistence import get_ais_downloads, get_onc_downloads  # noqa: F401

logger = logging.getLogger(__name__)
DateTime = Union[str, pd.Timestamp]
ais_site = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/"

OVERLAP_PRECISION = pd.Timedelta(500, "ms")
MODULE_LOADED_DATETIME = pd.Timestamp.utcnow()
DEFAULT_COLORS = plt.rcParams["axes.prop_cycle"].by_key()["color"]
try:
    onc = _persistence.onc_session
except NameError:
    pass


def download_ships(year: int, month: int, zone: int) -> None:
    """Download AIS records from Marine Cadastre.  Records are stored in
    a local sqlite database.  Marine Cadastre organizes records into
    csv files by year, month, and UTM zone
    (wikipedia the Universal_Transverse_Mercator_coordinate_system)

    Arguments:
        year (int): year to download
        month (int): month to download
        zone (int): UTM zone to download
    """
    ais_db = _persistence.AIS_DB
    _persistence.init_data_folder()
    _persistence.init_ais_db(ais_db)
    if (year, month, zone) not in _persistence.get_ais_downloads(ais_db):
        zipfile_path = _download_ais_to_temp(year, month, zone)
        unzipped_tree, unzipped_target = _unzip_ais(zipfile_path)
        returncode = _load_ais_csv_to_db(unzipped_target, ais_db).returncode
        if not returncode:
            shutil.rmtree(unzipped_tree)
            zipfile_path.unlink()
            _persistence.update_ais_downloads(year, month, zone, ais_db)
        else:
            raise RuntimeError(
                f"Failed to load data to database; check format of {unzipped_target}"
            )
    else:
        print(f"AIS data already stored for {year}, {month} zone {zone}.")


def _download_ais_to_temp(year: int, month: int, zone: int) -> Path:
    """Downloads AIS records from Marine Cadastre.

    Arguments:
        year: year to download
        month: month to download
        zone: UTM zone to download

    Returns:
        location of download result
    """
    month = f"{month:02}"

    if year > 2017:
        if zone < 10:
            zone = f"0{zone}"
        midfolder = ""
        filename = f"AIS_{year}_{month}_{zone}.zip"
    elif year > 2014:
        if zone < 10:
            zone = f"0{zone}"
        midfolder = ""
        filename = f"AIS_{year}_{month}_Zone{zone}.zip"
    elif year == 2014:
        midfolder = month + "/"
        filename = f"Zone{zone}_{year}_{month}.zip"
    elif year > 2010:
        midfolder = month + "/"
        filename = f"Zone{zone}_{year}_{month}.gdb.zip"
    elif year < 2010:
        months = {
            "01": "January",
            "02": "February",
            "03": "March",
            "04": "April",
            "05": "May",
            "06": "June",
            "07": "July",
            "08": "August",
            "09": "September",
            "10": "October",
            "11": "November",
            "12": "December",
        }
        midfolder = f"{month}_{months[month]}_{year}/"
        filename = f"Zone{zone}_{year}_{month}.zip"

    url = (
        f"https://coast.noaa.gov/htdata/CMSP/AISDataHandler/{year}/"
        + midfolder
        + filename
    )
    if isinstance(zone, int) and zone < 10:
        zone = f"0{zone}"
    filepath = _persistence.AIS_TEMP_DIR / filename
    if not filepath.exists():
        logger.info(f"Downloading data for {year} {month}, Zone {zone}...")
        r = requests.get(url)
        with open(filepath, "wb") as f:
            f.write(r.content)
    else:
        logger.info(f"{filepath} already exists.")
    return filepath


def _unzip_ais(zipfile: Path) -> Tuple[Path]:
    """Unzips the temporary zipfile

    Arguments:
        zipfile: directory to unzip.  Must obey marinecadastre's' layout

    Returns:
        tuple comprising the root of the unzip tree and the specific
        unzipped file of interest
    """
    with ZipFile(f"{zipfile}", "r") as zipObj:
        zipObj.extractall(_persistence.AIS_TEMP_DIR)
    zip_root = _persistence.AIS_TEMP_DIR / "AIS_ASCII_by_UTM_Month"
    year = re.search("[0-9]{4}", zipfile.stem).group(0)
    return zip_root, zip_root / year / (zipfile.stem + ".csv")


def _load_ais_csv_to_db(csv_file: Path, ais_db: Path) -> int:
    """Loads the AIS records from the given file into the appropriate
    table in ais_db and updates the metadata table in ais_db

    Arguments:
        csvfile: location of AIS records to add
        ais_db: location of AIS database to update

    Returns:
        Return value from the sqlite subprocess
    """
    ais_db = Path(ais_db).resolve()
    csv_file = Path(csv_file).resolve()
    with open(csv_file, "r") as source:
        source.readline()
        headless_file = str(csv_file) + "_nohead"
        with open(headless_file, "w") as target:
            shutil.copyfileobj(source, target)

    result = subprocess.run(
        [
            "sqlite3",
            str(ais_db),
            "-cmd",
            ".mode csv",
            ".import "
            + str(headless_file).replace("\\", "\\\\")  # make it Windows-safe
            + " ships",
        ],
        capture_output=True,
    )
    return result


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
    _persistence.init_data_folder()
    _persistence.init_onc_db(_persistence.ONC_DB)

    def code_from_extension(ext):
        if ext.lower() in ["mp3", "wav", "flac"]:
            return "AD"
        elif ext.lower() in ["png"]:
            return "HSD"

    files = []
    for hphone in hydrophones:
        try:
            request = onc.requestDataProduct(
                filters={
                    "dataProductCode": code_from_extension(extension),
                    "extension": extension,
                    "dateFrom": _onc_iso_fmt(begin),
                    "dateTo": _onc_iso_fmt(end),
                    "deviceCode": hphone,
                    "dpo_hydrophoneDataDiversionMode": "OD",
                    "dpo_audioDownsample": -1,
                }
            )
        except TypeError as exc:
            # See https://github.com/OceanNetworksCanada/api-python-client/issues/3
            if "sting indices must be integers" in str(exc):
                continue
            else:
                raise
        except Exception:
            # See https://github.com/OceanNetworksCanada/api-python-client/issues/4
            try:
                request = onc.requestDataProduct(
                    filters={
                        "dataProductCode": code_from_extension(extension),
                        "extension": extension,
                        "dateFrom": _onc_iso_fmt(begin),
                        "dateTo": _onc_iso_fmt(end),
                        "deviceCode": hphone,
                        # "dpo_hydrophoneDataDiversionMode": "OD",
                        "dpo_audioDownsample": -1,
                    }
                )
            except TypeError as exc:
                # See https://github.com/OceanNetworksCanada/api-python-client/issues/3
                if "sting indices must be integers" in str(exc):
                    continue
                else:
                    raise
        req_id = request["dpRequestId"]
        run_ids = onc.runDataProduct(req_id)["runIds"]
        for id in run_ids:
            downloads = onc.downloadDataProduct(id, includeMetadataFile=False)
            files += [
                download["file"]
                for download in downloads
                if download["status"] == "complete" and download["downloaded"]
            ]
    if files:
        _persistence.update_onc_tracker(_persistence.ONC_DB, files, extension)


@lru_cache(maxsize=1)
def _get_deployments():
    hphones = onc.getDeployments(filters={"deviceCategoryCode": "HYDROPHONE"})
    df = pd.DataFrame(hphones)
    df["begin"] = pd.to_datetime(df["begin"])
    df["end"] = pd.to_datetime(df["end"])
    df["zone"] = df["lon"].apply(_identify_utm_zone)
    return df


def _identify_utm_zone(lon):
    return int(lon // 6 + 31)


def certify_audio_availability():
    """Works with ONC server to determine data availability intervals

    As this is a long-running-process, it saves its progress along the
    way in a pickle file and restarts from the last pickle.
    """
    _persistence.init_data_folder()
    _persistence.init_onc_db(_persistence.ONC_DB)
    hphones = _get_deployments()
    processed_df = _persistence.load_audio_availability_progress()
    rows_to_process = _what_to_certify(hphones, processed_df)
    for i, row in rows_to_process.iterrows():
        tranges = _query_single_audio_availability(
            row["deviceCode"], row["begin"], row["end"]
        )
        _persistence.save_audio_availability_progress(tranges, row, _persistence.ONC_DB)


def _query_single_audio_availability(
    hydrophone: str, start: DateTime, finish: DateTime
) -> List[spans.datetimerange]:
    page = 1
    files = []
    while True:
        response = onc.getListByDevice(
            {
                "deviceCode": hydrophone,
                "dateFrom": _onc_iso_fmt(start),
                "dateTo": _onc_iso_fmt(finish),
                "fileExtension": "wav",
                "rowLimit": 100000,
                "page": page,
            }
        )
        files += response["files"]
        if response["next"] is None:
            break
        page += 1
    if not files:
        return []
    else:
        return _deterime_tranges_from_files(files)


def _deterime_tranges_from_files(files):
    time_pattern = r"_(\d{8}T\d{6}(?:\.\d+)?Z)"
    file_times = (
        pd.Series(files).str.extract(time_pattern, expand=False).astype(np.datetime64)
    )
    file_times = file_times.dropna()
    finish_times = file_times + pd.Timedelta("00:05:00")
    start_and_finish = zip(file_times, finish_times)
    # merging datetimeranges into datetimerangesets is less verbose, but O(n^2)
    tranges = []
    curr = datetimerange(*next(start_and_finish))
    for begin, end in start_and_finish:
        if curr.upper > begin - OVERLAP_PRECISION:
            curr = datetimerange(curr.lower, end)
        else:
            tranges.append(curr)
            curr = datetimerange(begin, end)
    tranges.append(curr)
    return tranges


def _what_to_certify(hphones: pd.DataFrame, processed_df: pd.DataFrame) -> pd.DataFrame:
    """Subtract processed records from total records

    Individual records can either be identical, new, or have the same
    start but different end dates.  The final option is likely if a
    deployment was ongoing when records were previously processed.

    Arguments:
        hphones: total records that may need processing
        processed_df: existing records of processing.

    Returns:
        Rows that need to be processed.
    """
    hphones["end"] = hphones["end"].fillna(MODULE_LOADED_DATETIME)
    if processed_df.empty:
        return hphones
    partial_index_labels = ["deviceCode", "begin"]
    full_index_labels = partial_index_labels + ["end"]
    hphones = hphones.set_index(full_index_labels, drop=False)
    pro_index = pd.Index(processed_df.loc[:, full_index_labels])
    overlap_index = hphones.index.intersection(pro_index)
    hphones = hphones.drop(overlap_index)

    # For hphone records that were ongoing, and thus have a processed
    # record with the same start time, but different end time
    hphones = hphones.set_index(partial_index_labels, drop=False)
    pro_index = pd.Index(processed_df.loc[:, partial_index_labels])
    partial_overlap_index = hphones.index.intersection(pro_index)
    hphones.loc[partial_overlap_index, "begin"] = processed_df.set_index(
        partial_index_labels
    ).loc[partial_overlap_index, "end"]

    return hphones.reset_index(drop=True)


def get_audio_availability(
    start: Union[Timestamp, str] = "2010",
    finish: Union[Timestamp, str] = pd.to_datetime("now", utc=True),
    certified: bool = False,
) -> DataFrame:
    """Show what hydrophones have data available within an interval

    Arguments:
        start: beginning of interval
        finish: end of interval
        certified: Whether to show certified availaibility (or just deployments)
    """
    if certified:
        hphones = _persistence.get_onc_certified()
        hphones["zone"] = hphones["lon"].apply(_identify_utm_zone)
    else:
        hphones = _get_deployments()

    def localize_or_convert(time: pd.Timestamp):
        if time.tz is None:
            return time.tz_localize("UTC")
        elif time.tz is not pytz.UTC:
            return time.tz_convert("UTC")
        return time

    start = localize_or_convert(pd.to_datetime(start))
    finish = localize_or_convert(pd.to_datetime(finish))
    after_start = hphones["begin"] < finish
    before_finish = hphones["end"] > start
    return hphones[after_start & before_finish]


def filter_hphones_rect(hphones, sw_corner=(-90, -180), ne_corner=(90, 180)):
    """Filter a hydrophone table by geographic area"""
    lat_filter = (hphones["lat"] > sw_corner[0]) & (hphones["lat"] < ne_corner[0])
    lon_filter = (hphones["lon"] > sw_corner[1]) & (hphones["lon"] < ne_corner[1])
    return hphones.loc[lat_filter & lon_filter]


def _onc_iso_fmt(dt: Union[Timestamp, str]) -> str:
    """Formats the datetime according to how ONC needs it in requests.

    Note: all datetimes are changed to UTC, and assumed UTC if no
    timezone present.
    """
    pdt = pd.to_datetime(dt)
    if pdt.tz is not None:
        pdt = pdt.tz_convert(None)
    dt = np.datetime64(pdt, "ms")
    return np.datetime_as_string(dt, timezone="UTC")


def show_available_data(
    begin: Union[datetime, str, Timestamp],
    end: Union[datetime, str, Timestamp],
    style: str,
    bottomleft: Tuple[float] = (-90.0, -180.0),
    topright: Tuple[float] = (90.0, 180.0),
    ais_db: Path = _persistence.AIS_DB,
    onc_db: Path = _persistence.ONC_DB,
    certified: bool = False,
) -> Union[MFigure, PFigure]:
    """Creates a visualization of what data is available to download and
    what data is available locally for sampling.

    Parameters:
        begin: start time to display
        end: end time to display
        bottomleft: Latitude, longitude tuple.  Only
            include hydrophones north and east of this point.
        topright: Latitude, longitude tuple.  Only
            include hydrophones south and west of this point.
        style: Either 'map' for a geographic map with hydrophones
            identified or 'bar' for a bar chart showing overlapping
            downloads.
        ais_db: path to the database of AIS records
        onc_db: database to track ONC downloads
        certified: Whether to restrict ranges to when data actually
            available

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

    ais_data = _persistence.get_ais_downloads(_persistence.AIS_DB)
    ais_data = pd.DataFrame(ais_data, columns=["year", "month", "zone"])
    if not ais_data.empty:
        ais_data["begin"] = ais_data.apply(
            lambda row: pd.to_datetime(
                str(row["year"]) + f"{row['month']:02}01", utc=True
            ),
            axis=1,
        )
        ais_data["end"] = ais_data.apply(
            lambda row: pd.to_datetime(
                str(row["year"]) + f"{row['month']+1:02}01", utc=True
            ),
            axis=1,
        )
    else:
        ais_data = pd.concat((ais_data, pd.DataFrame(columns=["begin", "end"])), axis=1)
    ais_data = ais_data[(ais_data["end"] > begin) & (ais_data["begin"] < end)]
    if certified:
        hphones = get_audio_availability(begin, end, True)
    else:
        hphones = get_audio_availability(begin, end)
    begin = pd.to_datetime(begin, utc=True)
    end = pd.to_datetime(end, utc=True)
    spans_df = _persistence.get_onc_downloads()
    spans_df = spans_df[(spans_df["start"] < end) & (spans_df["finish"] > begin)]
    hphones = filter_hphones_rect(hphones, bottomleft, topright)

    def lookup_zone(deviceCode):
        zones = hphones.query(f"deviceCode=='{deviceCode}'")["zone"]
        if len(zones.unique()) > 0:
            return zones.iloc[0]
        if len(zones.unique()) > 1:
            warnings.warn(
                "Assigning data to a zone is ambiguous because hydrophone moved zones"
                " over interval;  Please report this warning"
            )

    spans_df["zone"] = spans_df["hydrophone"].apply(lookup_zone)
    spans_df = spans_df.dropna(subset=["zone"])
    spans_df = spans_df.rename(
        columns={"hydrophone": "deviceCode", "start": "begin", "finish": "end"}
    )
    if style == "bar":

        def all_same_loc(df):
            """See if a hydrophone moves across multiple deployments"""
            lat0 = df["lat"].iloc[0]
            lon0 = df["lon"].iloc[0]
            lat_same = df["lat"] == lat0
            lon_same = df["lon"] == lon0
            # Maybe a "close enough" way
            if not lat_same.all() or not lon_same.all():
                return False
            return True

        hphones = hphones.sort_values(["zone", "deviceCode"])
        zone_nbars = (
            hphones.groupby("zone")["deviceCode"].nunique().sort_index(ascending=False)
        )
        zone_barstart = zone_nbars.sort_index().cumsum() - zone_nbars.sort_index() - 0.5
        ais_data["bottom"] = ais_data["zone"].apply(lambda z: zone_barstart.loc[z])
        ais_data["height"] = ais_data["zone"].apply(lambda z: zone_nbars.loc[z])

        def id_bar_coords(df):
            if not df.empty:
                label = df.apply(
                    lambda row: "Zone " + str(row["zone"]) + ": " + row["deviceCode"],
                    axis=1,
                )
            else:
                label = pd.Series()
            left = df["begin"].apply(
                lambda time: max(pd.to_datetime(begin, utc=True), time)
            )
            right = df["end"].apply(
                lambda time: min(pd.to_datetime(end, utc=True), time)
            )
            return label, left, right

        hphones["label"], hphones["left"], hphones["right"] = id_bar_coords(hphones)
        spans_df["label"], spans_df["left"], spans_df["right"] = id_bar_coords(spans_df)
        ys = pd.Series(
            range(len(hphones["label"].drop_duplicates())),
            index=hphones["label"].drop_duplicates(),
        )
        spans_df["y"] = spans_df["label"].apply(lambda label: ys.loc[label])
        fig = plt.figure(figsize=[8, 10])
        ax = fig.add_subplot(1, 1, 1)
        if not ais_data.empty:
            ax.barh(
                ais_data["bottom"],
                (ais_data["end"] - ais_data["begin"]).view(int),
                ais_data["height"],
                ais_data["begin"].view(int),
                align="edge",
                color=DEFAULT_COLORS[1],
            )
        ax.barh(
            hphones["label"],
            (hphones["right"] - hphones["left"]).view(int),
            height=0.8,
            left=hphones["left"].view(int),
            color=DEFAULT_COLORS[0],
        )
        mp3_df = spans_df.query("format=='mp3'")
        wav_df = spans_df.query("format=='wav'")
        if not mp3_df.empty:
            ax.barh(
                mp3_df["y"] - 0.05,
                (mp3_df["right"] - mp3_df["left"]).view(int),
                height=-0.35,
                left=mp3_df["left"].view(int),
                align="edge",
                color=DEFAULT_COLORS[2],
            )
        if not wav_df.empty:
            ax.barh(
                wav_df["y"] + 0.05,
                (wav_df["right"] - wav_df["left"]).view(int),
                height=0.35,
                left=wav_df["left"].view(int),
                align="edge",
                color=DEFAULT_COLORS[3],
            )
        old_tics = ax.get_xticks()
        ax.set_xticks(
            old_tics,
            [Text(val, 0, pd.Timestamp(val / 1e9, unit="s")) for val in old_tics],
            rotation=70,
        )
        return fig
    elif style == "map":
        # calculate months of AIS data for each deployment
        count_ais = lambda zone: (ais_data["zone"] == zone).sum()  # noqa: E731
        hphones["months_ais"] = hphones["zone"].apply(count_ais)

        def sum_downloaded(deviceCode, extension):
            device_match = spans_df["deviceCode"] == deviceCode
            ext_match = spans_df["format"] == extension
            match_df = spans_df[device_match & ext_match]
            intervals = match_df["end"] - match_df["begin"]
            return intervals.sum()

        hphones["mp3data"] = hphones["deviceCode"].apply(sum_downloaded, args=("mp3",))
        hphones["wavdata"] = hphones["deviceCode"].apply(sum_downloaded, args=("wav",))
        labelme = lambda row: (  # noqa: E731
            # line breaks fixed in unreleased black
            # fmt: off
            f"{row['deviceCode']}<br>"
            f"({row['lat']},{row['lon']})<br>"
            f"deployment from {row['begin']} to {row['end']}<br>"
            f"{row['months_ais']} months of AIS data downloaded<br>"
            f"{row['wavdata']} wav data downloaded<br>"
            f"{row['mp3data']} mp3 data downloaded<br>"
            f"across all deployments for this hydrophone and interval"
            # fmt: on
        )
        hphones["label"] = hphones.apply(labelme, axis=1)
        fig = go.Figure(
            go.Scattergeo(
                lat=hphones["lat"],
                lon=hphones["lon"],
                text=hphones["label"],
                hoverinfo="text",
            )
        )
        fig.update_geos(fitbounds="locations")
        fig.update_layout(height=400, margin={"r": 0, "t": 0, "l": 0, "b": 0})
        return fig
    else:
        raise ValueError("Only allowed styles are 'map' and 'bar'.")


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
            filtered_ais = _get_ais_data(trange.lower, trange.higher, lat, lon, ais_db)
            interpolated_ships = _interpolate_and_group_ais(filtered_ais, times)
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


def _get_hphone_posit(hydrophone: str, time: pd.Timestamp) -> Tuple[float, float]:
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


def _truncate_equal_shapes(ser: pd.Series) -> pd.Series:
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
