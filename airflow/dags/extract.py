import requests
import pandas as pd
from sqlalchemy import create_engine
from io import StringIO
import os
import datetime as dt
from urllib.parse import urlparse
import logging
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)


class PopcoreChallenge:
    def __init__(
        self,
        url: str,
        date_column: str = "date",
        table_name: str = None,
        db_user: str = os.getenv("MYSQL_USER"),
        db_password: str = os.getenv("MYSQL_PW"),
        db_host: str = os.getenv("MYSQL_HOST"),
        db_database: str = os.getenv("MYSQL_DB"),
    ) -> None:
        """
        Initialize PopcoreChallenge class with MySQL database credentials and url to csv file.

        Args:
            url (str): url to csv file
            date_column (str): name of date column for data validation. Defaults to "date".
            table_name (str): name of table in database. Defaults to None.
            db_user (str, optional): database user. Defaults to os.getenv("MYSQL_USER").
            db_password (str, optional): database password. Defaults to os.getenv("MYSQL_PW").
            db_host (str, optional): database host. Defaults to os.getenv("MYSQL_HOST").
            db_database (str, optional): database name. Defaults to os.getenv("MYSQL_DB").
        """
        self.url = url
        self.date_column = date_column
        self.table_name = table_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.db_database = db_database

    def get_table_name(self, url):
        """
        Get table name from url in case no table name is provided.

        Args:
            url (str): url to csv file

        Returns:
            table_name (str): name of table in database
        """
        if self.table_name:
            return self.table_name

        path = urlparse(url).path
        filename = path.split("/")[-1]
        name_without_extension = filename.replace(".csv", "")
        table_name = name_without_extension.replace("-", "_")
        return table_name

    def get_csv_data(self, url) -> pd.DataFrame:
        """
        Get csv data from url and return a pandas dataframe including the loadtime and source url.

        Args:
            url (str): url to csv file

        Returns:
            df (pd.DataFrame): pandas dataframe
        """
        logging.info(f"Getting data from {url}")
        response = requests.get(url)
        csv_content = StringIO(response.text)
        df = pd.read_csv(csv_content)
        df["LOADTIME"] = dt.datetime.now()
        df["SOURCE"] = url
        logging.info(f"Data from {url} successfully loaded into dataframe")
        return df

    def get_engine(self):
        """
        Get database engine.

        Returns:
            engine (sqlalchemy.engine): sqlalchemy engine
        """
        logging.info("Getting database engine")
        url = f"mysql+pymysql://{self.db_user}:{self.db_password}@{self.db_host}:3306/{self.db_database}"
        logging.info("Database engine successfully loaded")
        return create_engine(url)

    def validate_data(self, df: pd.DataFrame) -> bool:
        """
        Validate the data quality and availability.

        Args:
            df (pd.DataFrame): pandas dataframe

        Returns:
            bool: True if data is valid, False otherwise
        """
        threshold = dt.timedelta(days=2)
        df[self.date_column] = pd.to_datetime(df[self.date_column])
        latest_date = df[self.date_column].max()
        if (dt.datetime.now() - latest_date) > threshold:
            logging.error("Data validation failed: Data is outdated")
            return False

        logging.info("Data validation passed")
        return True

    def execute(self):
        """
        Execute the ETL process.
        """
        engine = self.get_engine()
        table_name = self.get_table_name(self.url)
        df = self.get_csv_data(self.url)

        if not self.validate_data(df):
            logging.error("Data validation failed. ETL process aborted.")
            return

        with engine.connect() as conn:
            df.to_sql(
                name=table_name,
                con=conn,
                if_exists="replace",
                index=False,
                chunksize=5000,
            )
            conn.close()
            logging.info(
                f"Data from {self.url} successfully loaded into table {table_name}"
            )
