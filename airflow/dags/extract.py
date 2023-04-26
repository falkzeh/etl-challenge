import requests
import pandas as pd
from sqlalchemy import create_engine
from io import StringIO
import os
import datetime as dt
from dotenv import load_dotenv

load_dotenv()


def get_csv_data(url) -> pd.DataFrame:
    """
    Get csv data from url and return a pandas dataframe including the loadtime and source url.

    Args:
        url (str): url to csv file

    Returns:
        df (pd.DataFrame): pandas dataframe
    """
    response = requests.get(url)
    csv_content = StringIO(response.text)
    df = pd.read_csv(csv_content)
    df["LOADTIME"] = dt.datetime.now()
    df["SOURCE"] = url
    return df


def get_engine():
    """
    Get database engine.

    Returns:
        engine (sqlalchemy.engine): sqlalchemy engine
    """
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PW")
    host = os.getenv("MYSQL_HOST")
    database = os.getenv("MYSQL_DB")
    url = f"mysql+pymysql://{user}:{password}@{host}:3306/{database}"
    return create_engine(url)


with get_engine().connect() as conn:
    df = get_csv_data("https://covid.ourworldindata.org/data/owid-covid-data.csv")
    df.to_sql(name="owid_covid_data", con=conn, if_exists="replace", index=False)

    df = get_csv_data(
        "https://github.com/owid/covid-19-data/blob/master/public/data/hospitalizations/covid-hospitalizations.csv"
    )
    df.to_sql(name="covid_hospitalizations", con=conn, if_exists="replace", index=False)

    conn.close()
