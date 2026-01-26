import pandas as pd
import os
import sqlite3 as sq3

OLTP_SCHEMA_TABLES = [
    "agency",
    "complaint_type",
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

        CREATE TABLE IF NOT EXISTS complaint_type (
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
        FOREIGN KEY (complaint_id) REFERENCES complaint_type(complaint_id),
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


# TODO: Make a function to add the contents of the dataframe to the database.
def add_contents(conn: sq3.Connection, df: pd.DataFrame):
    pass


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

    print("Reading the data...")
    data_path = os.path.join(db_dir, "requests.csv")
    df = pd.read_csv(data_path)

    #! Not built yet.
    print("Adding contents to database...")
    add_contents(conn, df)
