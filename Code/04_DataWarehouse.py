from datetime import date
import pandas as pd
import os
import sqlite3 as sq3

OLAP_SCHEMA_TABLES = [
    "dim_agency",
    "dim_complaint",
    "dim_date",
    "dim_channel",
    "dim_resolution",
    "dim_location",
    "fact_service_request",
]


def create_schema(conn: sq3.Connection):
    cur = conn.cursor()
    # * Enforce foreign keys

    olap_schema = """
    PRAGMA foreign_keys = ON;

    -- =========================
    -- Dimension tables
    -- =========================

    CREATE TABLE IF NOT EXISTS dim_date (
        date_key INTEGER PRIMARY KEY,
        date TEXT NOT NULL,
        year INTEGER NOT NULL,
        month INTEGER NOT NULL,
        day INTEGER NOT NULL,
        weekday INTEGER NOT NULL,
        is_weekend INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS dim_agency (
        agency_key INTEGER PRIMARY KEY,
        agency_code TEXT NOT NULL,
        agency_name TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS dim_complaint (
        complaint_key INTEGER PRIMARY KEY,
        complaint_type TEXT NOT NULL,
        descriptor TEXT
    );

    CREATE TABLE IF NOT EXISTS dim_channel (
        channel_key INTEGER PRIMARY KEY,
        channel_name TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS dim_resolution (
        resolution_key INTEGER PRIMARY KEY,
        resolution_type TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS dim_location (
        location_key INTEGER PRIMARY KEY,
        location_type TEXT NOT NULL,   -- ADDRESS / INTERSECTION / UNKNOWN
        board_id TEXT,
        borough TEXT,
        zip TEXT,
        city TEXT
    );

    -- =========================
    -- Fact table
    -- =========================

    CREATE TABLE IF NOT EXISTS fact_service_request (
        fact_id INTEGER PRIMARY KEY,

        date_key INTEGER NOT NULL,
        agency_key INTEGER NOT NULL,
        complaint_key INTEGER,
        location_key INTEGER,
        channel_key INTEGER,
        resolution_key INTEGER,

        wait_time_hours REAL NOT NULL,
        wait_time_days REAL NOT NULL,

        FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
        FOREIGN KEY (agency_key) REFERENCES dim_agency(agency_key),
        FOREIGN KEY (complaint_key) REFERENCES dim_complaint(complaint_key),
        FOREIGN KEY (location_key) REFERENCES dim_location(location_key),
        FOREIGN KEY (channel_key) REFERENCES dim_channel(channel_key),
        FOREIGN KEY (resolution_key) REFERENCES dim_resolution(resolution_key)
    );
    """
    cur.executescript(olap_schema)
    conn.commit()


def check_table(conn: sq3.Connection, table_name: str):
    cur = conn.cursor()
    cur.execute(
        f"""
    SELECT name
    FROM sqlite_master
    WHERE type='table' AND name='{table_name}';
    """
    )

    exists = cur.fetchone() is not None
    return exists


def check_schema(conn: sq3.Connection):
    flag = True
    for table in OLAP_SCHEMA_TABLES:
        exists = check_table(conn, table)
        print(f"{table} {'exists.' if exists else 'does not exist.'}")
        if not exists:
            print(
                "\x1b[31mERROR: Current schema does not match expected schema.\x1b[0m"
            )
            flag = False

    if flag:
        print("\x1b[32mSUCCESS: Schema matches fully.\x1b[0m")

    return flag


def clear_tables(conn: sq3.Connection):
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = OFF;")

    for table in OLAP_SCHEMA_TABLES:
        cur.execute(f"DELETE FROM {table};")

    cur.execute("PRAGMA foreign_keys = ON;")
    conn.commit()
    print("\x1b[32mAll tables cleared.\x1b[0m")


def add_agency(conn: sq3.Connection, df: pd.DataFrame) -> None:
    """
    dim_agency (
        agency_key INTEGER PRIMARY KEY,
        agency_code TEXT NOT NULL,
        agency_name TEXT NOT NULL
    );
    """
    cur = conn.cursor()
    for agency_id, agency in enumerate(df["agency_code"].unique()):
        agency_name = df["agency_name"][df["agency_code"] == agency].iloc[0]
        cur.execute(
            """
            INSERT OR IGNORE INTO dim_agency (agency_key, agency_code, agency_name)
            VALUES (?, ?, ?);
            """,
            (agency_id, agency, agency_name),
        )

    conn.commit()


def add_date(conn: sq3.Connection, df: pd.DataFrame) -> None:
    """
    dim_date (
        date_key INTEGER PRIMARY KEY,
        date TEXT NOT NULL,
        year INTEGER NOT NULL,
        month INTEGER NOT NULL,
        day INTEGER NOT NULL,
        weekday INTEGER NOT NULL,
        is_weekend INTEGER NOT NULL
    );
    """
    cur = conn.cursor()

    df["created_timestamp"] = pd.to_datetime(df["created_timestamp"])
    unique_dates = df["created_timestamp"].dt.date.unique()  # type:ignore
    for date_id, date in enumerate(unique_dates):
        date_obj = pd.to_datetime(date)
        year = date_obj.year
        month = date_obj.month
        day = date_obj.day
        weekday = date_obj.weekday()
        is_weekend = 1 if weekday >= 5 else 0

        cur.execute(
            """
            INSERT OR IGNORE INTO dim_date (
                date_key, date, year, month, day, weekday, is_weekend
            )
            VALUES (?, ?, ?, ?, ?, ?, ?);
            """,
            (date_id, date.isoformat(), year, month, day, weekday, is_weekend),
        )
    conn.commit()


def add_complaint_type(conn: sq3.Connection, df: pd.DataFrame) -> None:
    """
    dim_complaint (
        complaint_key INTEGER PRIMARY KEY,
        complaint_type TEXT NOT NULL,
        descriptor TEXT
    );
    """
    cur = conn.cursor()
    complaint_types = df[["complaint_type", "complaint_descriptor"]].drop_duplicates()
    complaint_id = 0
    for _, row in complaint_types.iterrows():
        cur.execute(
            """
            INSERT OR IGNORE INTO dim_complaint (complaint_key, complaint_type, descriptor)
            VALUES (?, ?, ?);
            """,
            (complaint_id, row["complaint_type"], row["complaint_descriptor"]),
        )
        complaint_id += 1

    conn.commit()


def add_channel(conn: sq3.Connection, df: pd.DataFrame) -> None:
    """
    dim_channel (
        channel_key INTEGER PRIMARY KEY,
        channel_name TEXT NOT NULL
    );
    """
    cur = conn.cursor()
    for channel_id, channel in enumerate(df["channel"].unique()):
        cur.execute(
            """
            INSERT OR IGNORE INTO dim_channel (channel_key, channel_name)
            VALUES (?, ?);
            """,
            (channel_id, channel),
        )

    conn.commit()


def add_resolution(conn: sq3.Connection, df: pd.DataFrame) -> None:
    """
    dim_resolution (
        resolution_key INTEGER PRIMARY KEY,
        resolution_type TEXT NOT NULL
    );
    """
    cur = conn.cursor()
    for resolution_id, resolution_type in enumerate(df["resolution_type"].unique()):
        cur.execute(
            """
            INSERT OR IGNORE INTO dim_resolution (resolution_key, resolution_type)
            VALUES (?, ?);
            """,
            (resolution_id, resolution_type),
        )

    conn.commit()


def add_location(conn: sq3.Connection, df: pd.DataFrame) -> None:
    """
    dim_location (
        location_key INTEGER PRIMARY KEY,
        location_type TEXT NOT NULL,
        board_id TEXT,
        borough TEXT,
        zip TEXT,
        city TEXT
    );
    """
    cur = conn.cursor()
    locations = df[
        [
            "location_type",
            "board_id",
            "borough_name",
            "zip",
            "city",
        ]
    ]
    locations = locations.drop_duplicates()
    location_id = 0
    for _, row in locations.iterrows():
        cur.execute(
            """
            INSERT OR IGNORE INTO dim_location (
                location_key, location_type, board_id, borough, zip, city
            )
            VALUES (?, ?, ?, ?, ?, ?);
            """,
            (
                location_id,
                row["location_type"],
                row["board_id"],
                row["borough_name"],
                row["zip"],
                row["city"],
            ),
        )
        location_id += 1
    conn.commit()


def add_service_request(conn: sq3.Connection, df: pd.DataFrame) -> None:
    """
    fact_service_request (
        fact_id INTEGER PRIMARY KEY,

        date_key INTEGER NOT NULL,
        agency_key INTEGER NOT NULL,
        complaint_key INTEGER,
        location_key INTEGER,
        channel_key INTEGER,
        resolution_key INTEGER,

        wait_time_hours REAL NOT NULL,
        wait_time_days REAL NOT NULL,

        FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
        FOREIGN KEY (agency_key) REFERENCES dim_agency(agency_key),
        FOREIGN KEY (complaint_key) REFERENCES dim_complaint(complaint_key),
        FOREIGN KEY (location_key) REFERENCES dim_location(location_key),
        FOREIGN KEY (channel_key) REFERENCES dim_channel(channel_key),
        FOREIGN KEY (resolution_key) REFERENCES dim_resolution(resolution_key)
    );
    """
    cur = conn.cursor()

    agency_map = {}
    cur.execute("SELECT agency_key, agency_code FROM dim_agency;")
    rows = cur.fetchall()
    for row in rows:
        agency_map[row[1]] = row[0]

    complaint_map = {}
    cur.execute("SELECT complaint_key, complaint_type FROM dim_complaint;")
    rows = cur.fetchall()
    for row in rows:
        complaint_map[row[1]] = row[0]

    date_map = {}
    cur.execute("SELECT date_key, date FROM dim_date;")
    rows = cur.fetchall()
    for row in rows:
        date_map[row[1]] = row[0]

    location_map = {}
    cur.execute(
        "SELECT location_key, location_type, board_id, borough, zip, city FROM dim_location;"
    )
    rows = cur.fetchall()
    for row in rows:
        key = (row[1], row[2], row[3], row[4], row[5])
        location_map[key] = row[0]

    channel_map = {}
    cur.execute("SELECT channel_key, channel_name FROM dim_channel;")
    rows = cur.fetchall()
    for row in rows:
        channel_map[row[1]] = row[0]

    resolution_map = {}
    cur.execute("SELECT resolution_key, resolution_type FROM dim_resolution;")
    rows = cur.fetchall()
    for row in rows:
        resolution_map[row[1]] = row[0]

    for fact_id, row in df.iterrows():
        created_date = pd.to_datetime(row["created_timestamp"]).date().isoformat()
        date_key = date_map[created_date]
        agency_key = agency_map[row["agency_code"]]
        complaint_key = complaint_map.get(row["complaint_type"], None)
        location_key = location_map.get(
            (
                row["location_type"],
                row["board_id"],
                row["borough_name"],
                str(row["zip"]),
                row["city"],
            ),
            None,
        )
        channel_key = channel_map[row["channel"]]
        resolution_key = resolution_map[row["resolution_type"]]

        wait_time_hours = row["waittime"] * 24
        wait_time_days = row["waittime"]

        cur.execute(
            """
            INSERT INTO fact_service_request (
                fact_id, date_key, agency_key, complaint_key, location_key,
                channel_key, resolution_key, wait_time_hours, wait_time_days
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                fact_id,
                date_key,
                agency_key,
                complaint_key,
                location_key,
                channel_key,
                resolution_key,
                wait_time_hours,
                wait_time_days,
            ),
        )

    conn.commit()


def add_contents(conn: sq3.Connection, df: pd.DataFrame):
    for table in OLAP_SCHEMA_TABLES:
        print(f"Adding contents to {table}...")
        match table:
            case "dim_agency":
                add_agency(conn, df)
            case "dim_date":
                add_date(conn, df)
            case "dim_complaint":
                add_complaint_type(conn, df)
            case "dim_channel":
                add_channel(conn, df)
            case "dim_resolution":
                add_resolution(conn, df)
            case "dim_location":
                add_location(conn, df)
            case "fact_service_request":
                add_service_request(conn, df)

    print("\x1b[32mSUCCESS: All contents added to database.\x1b[0m")


if __name__ == "__main__":
    base_dir = os.path.abspath(os.getcwd())
    db_dir = os.path.join(base_dir, "Data")
    db_path = os.path.join(db_dir, "olap_311.db")
    conn = sq3.connect(db_path)

    print("Connection to olap_311.db established.")

    create_schema(conn)

    print("OLAP Schema created.")
    print("Checking if schema is valid...")
    print()

    if not check_schema(conn):
        quit(1)

    print()

    clear = input("\x1b[33mClear all table contents?[y/N]: \x1b[0m")
    if clear.lower() == "y":
        clear_tables(conn)

    print()

    add = input("\x1b[33mAdd the table contents?[Y/n]: \x1b[0m")
    if add.lower() == "n":
        print("Exiting without adding contents.")
        quit(0)

    print("Reading the data...")
    data_path = os.path.join(db_dir, "requests_cleaned.csv")
    df = pd.read_csv(data_path, index_col="request_id")

    print("Adding dataframe contents to database...\n")
    add_contents(conn, df)
