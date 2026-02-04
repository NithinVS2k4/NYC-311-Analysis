import sqlite3 as sq3
import pandas as pd
import os


def channel_olap(conn: sq3.Connection):
    query = """
        SELECT
        COUNT(*) AS count,
        sqrt(
            AVG(wait_time_hours * wait_time_hours)
            - AVG(wait_time_hours) * AVG(wait_time_hours)
        ) AS stdev_waittime_hours,
        AVG(wait_time_hours) AS mean_waittime_hours,
        channel_name
        FROM fact_service_request f
        JOIN dim_channel c
        ON f.channel_key = c.channel_key
        GROUP BY channel_name
    """
    df = pd.read_sql_query(query, conn)
    return df


def location_type_olap(conn: sq3.Connection):
    query = """
        SELECT
        COUNT(*) AS count,
        sqrt(
            AVG(wait_time_hours * wait_time_hours)
            - AVG(wait_time_hours) * AVG(wait_time_hours)
        ) AS stdev_waittime_hours,
        AVG(wait_time_hours) AS mean_waittime_hours,
        location_type
        FROM fact_service_request f
        JOIN dim_location l
        ON f.location_key = l.location_key
        GROUP BY location_type
    """
    df = pd.read_sql_query(query, conn)
    return df


def borough_olap(conn: sq3.Connection):
    query = """
        SELECT
        COUNT(*) AS count,
        sqrt(
            AVG(wait_time_hours * wait_time_hours)
            - AVG(wait_time_hours) * AVG(wait_time_hours)
        ) AS stdev_waittime_hours,
        AVG(wait_time_hours) AS mean_waittime_hours,
        borough
        FROM fact_service_request f
        JOIN dim_location l
        ON f.location_key = l.location_key
        GROUP BY borough
    """
    df = pd.read_sql_query(query, conn)
    return df


def city_olap(conn: sq3.Connection):
    query = """
        SELECT
        COUNT(*) AS count,
        sqrt(
            AVG(wait_time_hours * wait_time_hours)
            - AVG(wait_time_hours) * AVG(wait_time_hours)
        ) AS stdev_waittime_hours,
        AVG(wait_time_hours) AS mean_waittime_hours,
        city
        FROM fact_service_request f
        JOIN dim_location l
        ON f.location_key = l.location_key
        GROUP BY city
    """
    df = pd.read_sql_query(query, conn)
    return df


def weekday_olap(conn: sq3.Connection):
    query = """
        SELECT
        COUNT(*) AS count,
        sqrt(
            AVG(wait_time_hours * wait_time_hours)
            - AVG(wait_time_hours) * AVG(wait_time_hours)
        ) AS stdev_waittime_hours,
        AVG(wait_time_hours) AS mean_waittime_hours,
        weekday,
        is_weekend
        FROM fact_service_request f
        JOIN dim_date d
        ON f.date_key = d.date_key
        GROUP BY weekday
    """
    df = pd.read_sql_query(query, conn)
    return df


def month_olap(conn: sq3.Connection):
    query = """
        SELECT
        COUNT(*) AS count,
        sqrt(
            AVG(wait_time_hours * wait_time_hours)
            - AVG(wait_time_hours) * AVG(wait_time_hours)
        ) AS stdev_waittime_hours,
        AVG(wait_time_hours) AS mean_waittime_hours,
        month
        FROM fact_service_request f
        JOIN dim_date d
        ON f.date_key = d.date_key
        GROUP BY month
    """
    df = pd.read_sql_query(query, conn)
    return df


def complaint_olap(conn: sq3.Connection):
    query = """
        SELECT
        COUNT(*) AS count,
        sqrt(
            AVG(wait_time_hours * wait_time_hours)
            - AVG(wait_time_hours) * AVG(wait_time_hours)
        ) AS stdev_waittime_hours,
        AVG(wait_time_hours) AS mean_waittime_hours,
        complaint_type
        FROM fact_service_request f
        JOIN dim_complaint c
        ON f.complaint_key = c.complaint_key
        GROUP BY complaint_type
    """
    df = pd.read_sql_query(query, conn)
    return df


if __name__ == "__main__":
    base_dir = os.path.abspath(os.getcwd())
    db_dir = os.path.join(base_dir, "Data")
    db_path = os.path.join(db_dir, "olap_311.db")
    conn = sq3.connect(db_path)

    sep_len = 28
    print()
    print("=" * sep_len, "channel", "=" * sep_len)
    print(channel_olap(conn))

    sep_len = 33
    print()
    print("=" * sep_len, "location_type", "=" * sep_len)
    print(location_type_olap(conn))

    sep_len = 29
    print()
    print("=" * sep_len, "borough", "=" * sep_len)
    print(borough_olap(conn))

    sep_len = 34
    print()
    print("=" * sep_len, "city", "=" * sep_len)
    print(city_olap(conn))

    sep_len = 32
    print()
    print("=" * sep_len, "weekday", "=" * sep_len)
    print(weekday_olap(conn))

    sep_len = 26
    print()
    print("=" * sep_len, "month", "=" * sep_len)
    print(month_olap(conn))

    sep_len = 36
    print()
    print("=" * sep_len, "complaint", "=" * sep_len)
    print(complaint_olap(conn))
