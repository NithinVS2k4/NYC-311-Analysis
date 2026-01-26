from calendar import c
from xmlrpc.client import INVALID_XMLRPC
import pandas as pd
import os
import sqlite3 as sq3

OLTP_SCHEMA_TABLES = [
    "agency",
    "complaint",
    "borough",
    "park",
    "location",
    "service_request",
]


def create_schema(conn: sq3.Connection):
    cur = conn.cursor()
    # * Enforce foreign keys
    cur.execute("PRAGMA foreign_keys = ON;")

    oltp_schema = """
        CREATE TABLE IF NOT EXISTS agency (
        agency_id INTEGER PRIMARY KEY,
        agency_code TEXT UNIQUE,
        agency_name TEXT
        );

        CREATE TABLE IF NOT EXISTS complaint (
        complaint_id INTEGER PRIMARY KEY,
        complaint_type TEXT,
        descriptor TEXT
        );

        CREATE TABLE IF NOT EXISTS borough (
        borough_id INTEGER PRIMARY KEY,
        borough_name TEXT UNIQUE
        );

        CREATE TABLE IF NOT EXISTS park (
        park_id INTEGER PRIMARY KEY,
        park_facility_name TEXT,
        park_borough TEXT
        );

        CREATE TABLE IF NOT EXISTS location (
        location_id INTEGER PRIMARY KEY,
        borough_id INTEGER,
        board_id TEXT,
        zip TEXT,
        city TEXT,
        latitude REAL,
        longitude REAL,
        x_coordinate_state_plane REAL,
        y_coordinate_state_plane REAL,
        incident_address TEXT,
        street_name TEXT,
        cross_street_1 TEXT,
        cross_street_2 TEXT,
        intersection_street_1 TEXT,
        intersection_street_2 TEXT,
        landmark TEXT,
        bbl TEXT,
        FOREIGN KEY (borough_id) REFERENCES borough(borough_id)
        );

        CREATE TABLE IF NOT EXISTS service_request (
        request_id INTEGER PRIMARY KEY,
        created_timestamp TEXT,
        closed_timestamp TEXT,
        resolution_action_updated_timestamp TEXT,
        status TEXT,
        channel TEXT,
        agency_id INTEGER,
        complaint_id INTEGER,
        location_id INTEGER,
        park_id INTEGER,
        resolution_description TEXT,
        FOREIGN KEY (agency_id) REFERENCES agency(agency_id),
        FOREIGN KEY (complaint_id) REFERENCES complaint(complaint_id),
        FOREIGN KEY (location_id) REFERENCES location(location_id),
        FOREIGN KEY (park_id) REFERENCES park(park_id)
        );
    """
    cur.executescript(oltp_schema)
    conn.commit()


def check_table(conn: sq3.Connection, table_name: str):
    cur = conn.cursor()
    cur.execute(
        """
    SELECT name
    FROM sqlite_master
    WHERE type='table' AND name='service_request';
    """
    )

    exists = cur.fetchone() is not None
    return exists


def check_schema(conn: sq3.Connection):
    flag = True
    for table in OLTP_SCHEMA_TABLES:
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

    for table in OLTP_SCHEMA_TABLES:
        cur.execute(f"DELETE FROM {table};")

    cur.execute("PRAGMA foreign_keys = ON;")
    conn.commit()
    print("\x1b[32mAll tables cleared.\x1b[0m")


def add_agency(conn: sq3.Connection, df: pd.DataFrame) -> None:
    """
    agency (
        agency_id INTEGER PRIMARY KEY,
        agency_code TEXT UNIQUE,
        agency_name TEXT
    );
    """
    cur = conn.cursor()
    for agency_id, agency in enumerate(df["agency"].unique()):
        agency_name = df["agency_name"][df["agency"] == agency].iloc[0]
        cur.execute(
            """
            INSERT OR IGNORE INTO agency (agency_id, agency_code, agency_name)
            VALUES (?, ?, ?);
            """,
            (agency_id, agency, agency_name),
        )

    conn.commit()


def add_borough(conn: sq3.Connection, df: pd.DataFrame) -> None:
    """
    borough (
        borough_id INTEGER PRIMARY KEY,
        borough_name TEXT UNIQUE
    );
    """
    cur = conn.cursor()
    for borough_id, borough in enumerate(df["borough"].unique()):
        cur.execute(
            """
            INSERT OR IGNORE INTO borough (borough_id, borough_name)
            VALUES (?, ?);
            """,
            (borough_id, borough),
        )

    conn.commit()


def add_complaint_type(conn: sq3.Connection, df: pd.DataFrame) -> None:
    """
    complaint (
        complaint_id INTEGER PRIMARY KEY,
        complaint_type TEXT,
        descriptor TEXT
    );
    """
    cur = conn.cursor()
    complaint_types = df[["complaint_type", "descriptor"]].drop_duplicates()
    complaint_id = 0
    for _, row in complaint_types.iterrows():
        cur.execute(
            """
            INSERT OR IGNORE INTO complaint (complaint_id, complaint_type, descriptor)
            VALUES (?, ?, ?);
            """,
            (complaint_id, row["complaint_type"], row["descriptor"]),
        )
        complaint_id += 1

    conn.commit()


def add_park(conn: sq3.Connection, df: pd.DataFrame) -> None:
    """
    park (
        park_id INTEGER PRIMARY KEY,
        park_facility_name TEXT,
        park_borough TEXT
    );
    """
    cur = conn.cursor()
    parks = df[["park_facility_name", "park_borough"]].drop_duplicates()
    park_id = 0
    for _, row in parks.iterrows():
        cur.execute(
            """
            INSERT OR IGNORE INTO park (park_id, park_facility_name, park_borough)
            VALUES (?, ?, ?);
            """,
            (park_id, row["park_facility_name"], row["park_borough"]),
        )
        park_id += 1

    conn.commit()


def add_location(conn: sq3.Connection, df: pd.DataFrame) -> None:
    """
    location (
        location_id INTEGER PRIMARY KEY,
        borough_id INTEGER,
        board_id TEXT,
        zip TEXT,
        city TEXT,
        latitude REAL,
        longitude REAL,
        x_coordinate_state_plane REAL,
        y_coordinate_state_plane REAL,
        incident_address TEXT,
        street_name TEXT,
        cross_street_1 TEXT,
        cross_street_2 TEXT,
        intersection_street_1 TEXT,
        intersection_street_2 TEXT,
        landmark TEXT,
        bbl TEXT,
        FOREIGN KEY (borough_id) REFERENCES borough(borough_id)
    );
    """
    cur = conn.cursor()
    locations = df[
        [
            "borough",
            "community_board",
            "incident_zip",
            "city",
            "latitude",
            "longitude",
            "x_coordinate_state_plane",
            "y_coordinate_state_plane",
            "incident_address",
            "street_name",
            "cross_street_1",
            "cross_street_2",
            "intersection_street_1",
            "intersection_street_2",
            "landmark",
            "bbl",
        ]
    ].drop_duplicates()

    borough_map = {}
    cur.execute("SELECT borough_id, borough_name FROM borough;")
    rows = cur.fetchall()
    for row in rows:
        borough_map[row[1]] = row[0]

    location_id = 0
    for _, row in locations.iterrows():
        borough_id = borough_map.get(row["borough"], None)
        cur.execute(
            """
            INSERT OR IGNORE INTO location (
                location_id, borough_id, board_id, zip, city, latitude, longitude,
                x_coordinate_state_plane, y_coordinate_state_plane, incident_address,
                street_name, cross_street_1, cross_street_2,
                intersection_street_1, intersection_street_2,
                landmark, bbl
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                location_id,
                borough_id,
                row["community_board"].split()[
                    0
                ],  # Extract board_id from "<board_id> <borough>" format
                row["incident_zip"],
                row["city"],
                row["latitude"],
                row["longitude"],
                row["x_coordinate_state_plane"],
                row["y_coordinate_state_plane"],
                row["incident_address"],
                row["street_name"],
                row["cross_street_1"],
                row["cross_street_2"],
                row["intersection_street_1"],
                row["intersection_street_2"],
                row["landmark"],
                row["bbl"],
            ),
        )
        location_id += 1

    conn.commit()


def add_service_request(conn: sq3.Connection, df: pd.DataFrame) -> None:
    """
    service_request (
        request_id INTEGER PRIMARY KEY,
        created_timestamp TEXT,
        closed_timestamp TEXT,
        resolution_action_updated_timestamp TEXT,
        status TEXT,
        channel TEXT,
        agency_id INTEGER,
        complaint_id INTEGER,
        location_id INTEGER,
        park_id INTEGER,
        resolution_description TEXT,
        FOREIGN KEY (agency_id) REFERENCES agency(agency_id),
        FOREIGN KEY (complaint_id) REFERENCES complaint(complaint_id),
        FOREIGN KEY (location_id) REFERENCES location(location_id),
        FOREIGN KEY (park_id) REFERENCES park(park_id)
    );
    """
    cur = conn.cursor()

    agency_map = {}
    cur.execute("SELECT agency_id, agency_code FROM agency;")
    rows = cur.fetchall()
    for row in rows:
        agency_map[row[1]] = row[0]

    complaint_map = {}
    cur.execute("SELECT complaint_id, complaint_type FROM complaint;")
    rows = cur.fetchall()
    for row in rows:
        complaint_map[row[1]] = row[0]

    park_map = {}
    cur.execute("SELECT park_id, park_facility_name FROM park;")
    rows = cur.fetchall()
    for row in rows:
        park_map[row[1]] = row[0]

    location_map = {}
    cur.execute("SELECT * FROM location;")
    rows = cur.fetchall()
    for row in rows:
        location_map["/".join(list(map(str, row[1:])))] = row[0]

    request_id = 0
    for _, row in df.iterrows():
        agency_id = agency_map.get(row["agency"], None)
        complaint_id = complaint_map.get(row["complaint_type"], None)
        park_id = park_map.get(row["park_facility_name"], None)
        location_id = location_map.get(
            "/".join(
                [
                    str(row["borough"]),
                    str(row["community_board"]),
                    str(row["incident_zip"]),
                    str(row["city"]),
                    str(row["latitude"]),
                    str(row["longitude"]),
                    str(row["x_coordinate_state_plane"]),
                    str(row["y_coordinate_state_plane"]),
                    str(row["incident_address"]),
                    str(row["street_name"]),
                    str(row["cross_street_1"]),
                    str(row["cross_street_2"]),
                    str(row["intersection_street_1"]),
                    str(row["intersection_street_2"]),
                    str(row["landmark"]),
                    str(row["bbl"]),
                ]
            ),
            None,
        )
        cur.execute(
            """
            INSERT OR IGNORE INTO service_request (
                request_id, created_timestamp, closed_timestamp,
                resolution_action_updated_timestamp, status, channel,
                agency_id, complaint_id, location_id, park_id, resolution_description
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                request_id,
                row["created_date"],
                row["closed_date"],
                row["resolution_action_updated_date"],
                row["status"],
                row["open_data_channel_type"],
                agency_id,
                complaint_id,
                location_id,
                park_id,
                row["resolution_description"],
            ),
        )
        request_id += 1

    conn.commit()


def add_contents(conn: sq3.Connection, df: pd.DataFrame):
    for table in OLTP_SCHEMA_TABLES:
        print(f"Adding contents to {table}...")
        match table:
            case "agency":
                add_agency(conn, df)
            case "borough":
                add_borough(conn, df)
            case "complaint_type":
                add_complaint_type(conn, df)
            case "park":
                add_park(conn, df)
            case "location":
                add_location(conn, df)
            case "service_request":
                add_service_request(conn, df)

    print("\x1b[32mSUCCESS: All contents added to database.\x1b[0m")


if __name__ == "__main__":
    base_dir = os.path.abspath(os.getcwd())
    db_dir = os.path.join(base_dir, "Data")
    db_path = os.path.join(db_dir, "oltp_311.db")
    conn = sq3.connect(db_path)

    print("Connection to oltp_311.db established.")

    create_schema(conn)

    print("OLTP Schema created.")
    print("Checking if schema is valid...")
    print()

    if not check_schema(conn):
        quit(1)

    print()

    clear = input("\x1b[33mClear all table contents?[y/N]: \x1b[0m")
    if clear.lower() == "y":
        clear_tables(conn)

    print()

    add = input("\x1b[33mClear all table contents?[Y/n]: \x1b[0m")
    if add.lower() == "n":
        print("Exiting without adding contents.")
        quit(0)

    print("Reading the data...")
    data_path = os.path.join(db_dir, "requests.csv")
    df = pd.read_csv(data_path)

    print("Adding dataframe contents to database...\n")
    add_contents(conn, df)
