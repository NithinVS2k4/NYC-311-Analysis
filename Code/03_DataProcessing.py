import numpy as np
import pandas as pd
import sqlite3 as sq3
import os

YELLOW = "\x1b[33m"
GREEN = "\x1b[32m"
RED = "\x1b[31m"
RESET = "\x1b[0m"


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


def map_resolution_to_bucket(description: str) -> str:
    """
    Maps NYPD resolution descriptions to one of 8 OLAP buckets.
    """

    d = description.lower()

    # 1. ENFORCEMENT ACTION
    if any(
        kw in d
        for kw in [
            "issued a summons",
            "summons was issued",
            "police issued a summons",
            "made an arrest",
            "police made an arrest",
        ]
    ):
        return "ENFORCEMENT_ACTION"

    # 2. REFERRED TO OTHER AGENCY
    if any(
        kw in d
        for kw in [
            "referred to the department of homeless services",
            "referred to dhs",
            "does not fall under the police department's jurisdiction",
            "does not fall under the jurisdiction",
            "not under the jurisdiction",
        ]
    ):
        return "REFERRED_TO_OTHER_AGENCY"

    # 3. UNABLE TO COMPLETE INVESTIGATION
    if any(
        kw in d
        for kw in [
            "unable to gain entry",
            "insufficient contact information",
            "cannot be processed at this time",
            "can not be processed at this time",
        ]
    ):
        return "UNABLE_TO_COMPLETE_INVESTIGATION"

    # 4. PENDING / INCOMPLETE
    if any(
        kw in d
        for kw in [
            "has been received and assigned",
            "additional information will be available later",
            "complaint has been received",
        ]
    ):
        return "PENDING_INCOMPLETE"

    # 5. ADMINISTRATIVE / INFORMATIONAL
    if any(
        kw in d
        for kw in [
            "a report was prepared",
            "police department reviewed your complaint",
            "provided additional information",
        ]
    ):
        return "ADMINISTRATIVE_INFORMATIONAL"

    # 6. CONDITION RESOLVED WITHOUT ENFORCEMENT
    if any(
        kw in d
        for kw in [
            "condition was corrected",
            "took action to fix the condition",
            "those responsible for the condition were gone",
            "requested a tow truck",
            "another specific tow is required",
        ]
    ):
        return "CONDITION_RESOLVED_NO_ENFORCEMENT"

    # 7. POLICE RESPONSE — NO ACTION NECESSARY
    if any(
        kw in d
        for kw in ["police action was not necessary", "tow request was not necessary"]
    ):
        return "POLICE_RESPONSE_NO_ACTION"

    # 8. NO VIOLATION FOUND
    if any(
        kw in d
        for kw in [
            "observed no criminal violation",
            "no evidence of a criminal violation",
            "no evidence of the violation",
            "observed no encampment",
            "no encampment was found",
        ]
    ):
        return "NO_VIOLATION_FOUND"

    # Fallback
    return "NO_VIOLATION_FOUND"


class Printer:
    def __init__(self):
        self.msg = None

    def __call__(self, msg):
        print(f"{YELLOW}{msg}{RESET}", end="")
        self.msg = msg

    def ping(self):
        print(f"\r{GREEN}{self.msg}{RESET}")


if __name__ == "__main__":
    printer = Printer()
    base_dir = os.path.abspath(os.getcwd())
    db_dir = os.path.abspath(os.path.join(base_dir, "Data"))
    db_path = os.path.join(db_dir, "oltp_311.db")
    conn = sq3.connect(db_path)

    print("Connection to oltp_311.db established.")
    query = """
    SELECT
        -- service_request (core transaction)
        sr.request_id,
        sr.created_timestamp,
        sr.closed_timestamp,
        sr.resolution_action_updated_timestamp,
        sr.status,
        sr.channel,
        sr.resolution_description,

        -- agency
        a.agency_code,
        a.agency_name,

        -- complaint
        c.complaint_type,
        c.descriptor AS complaint_descriptor,

        -- location
        l.board_id,
        l.zip,
        l.city,
        l.latitude,
        l.longitude,
        l.x_coordinate_state_plane,
        l.y_coordinate_state_plane,
        l.location_type,
        l.incident_address,
        l.street_name,
        l.cross_street_1,
        l.cross_street_2,
        l.intersection_street_1,
        l.intersection_street_2,
        l.landmark,
        l.bbl,

        -- borough
        b.borough_name,

        -- park
        p.park_facility_name,
        p.park_borough

    FROM service_request sr
    LEFT JOIN agency a
        ON sr.agency_id = a.agency_id
    LEFT JOIN complaint c
        ON sr.complaint_id = c.complaint_id
    LEFT JOIN location l
        ON sr.location_id = l.location_id
    LEFT JOIN borough b
        ON l.borough_id = b.borough_id
    LEFT JOIN park p
        ON sr.park_id = p.park_id;
    """

    printer("Obtaining the OLTP data...")
    df = pd.read_sql_query(query, conn)
    printer.ping()
    print()
    print(df.head())
    print()

    printer("Converting timestamps to DateTime objects...")
    df["created_timestamp"] = pd.to_datetime(df["created_timestamp"])
    df["closed_timestamp"] = pd.to_datetime(df["closed_timestamp"])
    printer.ping()

    printer("Creating waittime feature...")
    df["waittime"] = df["closed_timestamp"] - df["created_timestamp"]
    df["waittime"] = df["waittime"].map(conv_to_days)
    printer.ping()

    printer("Filtering negative waittime rows...")
    df = df[df["waittime"] >= 0]
    printer.ping()

    printer("Filtering rows with invalid descriptions...")
    df = df.dropna(subset=["resolution_description"])
    printer.ping()

    print()
    print(f"{df.shape[0]} Rows, {df.shape[1]} Columns")

    print()
    print("=" * 22, "NAN Counts", "=" * 22)
    na_counts = df.isna().sum()
    print(f"{'Column':<35} {'NA Counts':<12} {'NA %':<5}")
    for col in na_counts.index:  # type:ignore
        if na_counts[col] == 0:  # type:ignore
            continue
        print(
            f"{col:<35} {na_counts[col]:<12} {na_counts[col]/df.shape[0]:.2%}"  # type:ignore
        )  # type:ignore

    print()

    print("Normalizing categorical columns...\n")
    CAT_COLS = [
        "agency_code",
        "agency_name",
        "complaint_type",
        "complaint_descriptor",
        "location_type",
        "board_id",
        "borough_name",
        "channel",
        "park_facility_name",
        "park_borough",
        "city",
    ]

    for i, col in enumerate(CAT_COLS):
        printer(f"Processing {col}... ({i+1}/{len(CAT_COLS)})")
        val_cts = df[col].value_counts()
        unique, counts = val_cts.index, val_cts.values

        for i in range(len(unique)):
            key, count = unique[i], counts[i]
            new_key = (
                key.upper() if count > 100 or key.upper() == "UNSPECIFIED" else "OTHER"
            )
            df.loc[df[col] == key, col] = new_key

        printer.ping()
    print()

    for col in CAT_COLS:
        val_cts = df[col].value_counts()
        unique, counts = val_cts.index, val_cts.values
        if len(unique) > 10:
            continue

        print(f"Column: {col}")
        print(f"{'Value':<35} {'Count':<7} {'Percentage %':<15}")
        for i in range(len(unique)):
            key, count = unique[i], counts[i]
            print(
                f"{unique[i]:<35} {counts[i]:<7} {counts[i]/(np.sum(list(counts)).item()):.2%}"
            )

        print()
        print("=" * 70)

    printer("Creating columns for time components of creation time...")
    df["created_timestamp"] = pd.to_datetime(df["created_timestamp"])

    df["created_year"] = df["created_timestamp"].dt.year  # type:ignore
    df["created_month"] = df["created_timestamp"].dt.month  # type:ignore
    df["created_day"] = df["created_timestamp"].dt.day  # type:ignore
    df["created_hour"] = df["created_timestamp"].dt.hour  # type:ignore
    df["created_weekday"] = df["created_timestamp"].dt.weekday  # type:ignore
    printer.ping()

    printer("Creating columns for time components of closure time...")
    df["closed_timestamp"] = pd.to_datetime(df["closed_timestamp"])

    df["closed_year"] = df["closed_timestamp"].dt.year  # type:ignore
    df["closed_month"] = df["closed_timestamp"].dt.month  # type:ignore
    df["closed_day"] = df["closed_timestamp"].dt.day  # type:ignore
    df["closed_hour"] = df["closed_timestamp"].dt.hour  # type:ignore
    df["closed_weekday"] = df["closed_timestamp"].dt.weekday  # type:ignore
    printer.ping()
    print()
    print(df.head())
    print()

    printer("Creating boolean column for precise location availability...")
    df["has_precise_location"] = df["latitude"].notna()
    printer.ping()

    printer("Creating resolution_type for categorizing resolution_description")
    df["resolution_type"] = df["resolution_description"].apply(map_resolution_to_bucket)
    printer.ping()

    printer("Saving processed dataset...")
    df_clean = df.copy()
    base_dir = os.path.abspath(os.getcwd())
    data_dir = os.path.abspath(os.path.join(base_dir, "Data"))

    os.makedirs(data_dir, exist_ok=True)

    output_path = os.path.join(data_dir, "requests_cleaned.csv")
    df_clean.to_csv(output_path, index=False)
    printer.ping()
    print("Saved to:", output_path)
