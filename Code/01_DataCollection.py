import pandas as pd
import numpy as np
import datetime as dt
import time
import requests
from sodapy import Socrata
import json
import matplotlib.pyplot as plt
import seaborn as sns
import os


def conv_to_days(waittime: pd.Timedelta) -> float | None:  # must be a timedelta type
    """Convert wait times to days

    Args:
        waittime (pd.Timedelta): Time difference between closing and created date

    Returns:
        float: If the waittime is a valid time delta, then return the time in days
        None: Returned when waittime is NaT
    """
    try:
        days = abs(waittime.components[0])
        hours = waittime.components[1] / 24
        mins = waittime.components[2] / 60 / 24
        secs = waittime.components[3] / 60 / 60 / 24
        if waittime.components[0] < 0:
            return (days + hours + mins + secs) * -1
        else:
            return days + hours + mins + secs
    except:
        # if NaT, return None
        pass


def get_311_data(
    limit: int = 2000,
    app_token: str | None = None,
    date_max: str = "2026-01-01T00:00:00",
) -> pd.DataFrame:
    """Collects 311 service request data and returns a data frame
    NOTE: 25 sweeps of `limit` rows will be performed.

    Args:
        limit (int, optional): Collects `limit` x 25 rows of data. Defaults to 2000 (MAX).
        app_token (str|None, optional): Socrata App Token from NYC OpenData. Data colection is faster if provided. Defaults to None.
        date_max (str, optional): Most recent date you want to collect from in the form 'YYYY-MM-DDTHH:MM:SS'. Defaults to Jan 1 2026, 00:00:00.

    Returns:
        pd.DataFrame: DataFrame object containing all the fetched data.
    """

    # Columns to be taken
    SUBFIELDS = [
        "unique_key",
        "created_date",
        "closed_date",
        "agency",
        "agency_name",
        "complaint_type",
        "descriptor",
        "location_type",
        "status",
        "community_board",
        "borough",
        "open_data_channel_type",
        "park_facility_name",
        "park_borough",
        "incident_zip",
        "incident_address",
        "street_name",
        "cross_street_1",
        "cross_street_2",
        "intersection_street_1",
        "intersection_street_2",
        "city",
        "landmark",
        "bbl",
        "x_coordinate_state_plane",
        "y_coordinate_state_plane",
        "latitude",
        "longitude",
        "location",
        "resolution_description",
        "resolution_action_updated_date",
    ]

    print("Establishing Source...")
    client = Socrata("data.cityofnewyork.us", app_token, timeout=300)

    data = []
    print("Obtaining Data...")
    for offs in range(0, 48001, 2000):
        # For 25 sweeps, `limit` rows are collected. Each sweep is seperated by an offset of 2000.
        print(f"\r{offs}", end="")

        results = client.get(
            "erm2-nwe9",
            limit=limit,
            offset=offs,
            # Take only NYPD agencies with closed requests, before `data_max`
            where=f"agency = 'NYPD' and status = 'Closed' and created_date < '{date_max}'",
            order="created_date desc",
        )

        results_df = pd.DataFrame.from_records(results, columns=SUBFIELDS)

        data.append(results_df)
        time.sleep(3)
    print()

    full = pd.concat(data)
    full.reset_index(inplace=True)

    # Basic feature engineering
    full["created_date"] = pd.to_datetime(full["created_date"])
    full["closed_date"] = pd.to_datetime(full["closed_date"])
    full["waittime"] = full["closed_date"] - full["created_date"]
    full["waittime"] = full["waittime"].map(conv_to_days)

    return full


def get_earliest_date(df: pd.DataFrame) -> str:
    """Utility function to obtain earliest date in the request dataframe

    Args:
        df (pd.DataFrame): Service requests DataFrame object

    Returns:
        str: Earliest data in 'YYYY-MM-DDTHH:MM:SS' format
    """
    return str(df["created_date"].min()).replace(" ", "T")


def get_all_data(
    limit: int = 2000,
    app_token: str | None = None,
    date_max: str = "2026-01-01T00:00:00",
) -> pd.DataFrame:
    """Performs 12 iterations of get_311_data(*args) with the specified parameters and returns the combined DataFrame object.

    Args:
        limit (int, optional): Collects `limit` x 25 x 12 rows of data. Defaults to 2000 (MAX).
        app_token (str | None, optional): Socrata App Token from NYC OpenData. Data colection is faster if provided. Defaults to None.
        date_max (str, optional): Most recent date you want to collect from in the form 'YYYY-MM-DDTHH:MM:SS'. Defaults to "2026-01-01T00:00:00".

    Returns:
        pd.DataFrame: Consolidated DataFrame object with the service request data.
    """

    print("Iteration: 1/12")
    dfs = []

    df1 = get_311_data(limit, app_token=app_token, date_max=date_max)
    df_last = get_earliest_date(df1)
    dfs.append(df1)

    for _ in range(2, 13):
        print()
        print(f"Iteration: {_}/12")
        df = get_311_data(limit=limit, app_token=app_token, date_max=df_last)
        df_last = get_earliest_date(df)
        dfs.append(df)

    full_data = pd.concat(dfs, ignore_index=True)

    return full_data


if __name__ == "__main__":
    ENV = pd.read_json("env.json", typ="series")
    df = get_all_data(2000, app_token=ENV["APP_TOKEN"], date_max="2026-01-01T00:00:00")
    df.drop(columns="index", inplace=True)
    print(f"{len(df)} Rows")
    print(f"{len(df.columns)} Columns")
    print()
    print(f"Columns: {df.columns}")
    print()
    print(f"Data from {df['created_date'].min()} to {df['created_date'].max()}")
    print()

    print(df.head())
    print()

    print(df.describe())

    base_dir = os.path.abspath(os.getcwd())
    data_dir = os.path.join(base_dir, "Data")

    os.makedirs(data_dir, exist_ok=True)

    output_path = os.path.join(data_dir, "requests.csv")
    df.to_csv(output_path, index=False)

    print("Saved to:", output_path)
