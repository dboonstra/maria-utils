# Module Import
import mariadb  # type: ignore
import sys
import sqlalchemy  # type: ignore
from sqlalchemy.orm import sessionmaker  # type: ignore
from sqlalchemy.exc import SQLAlchemyError  # type: ignore
import configparser  # Import the configparser module
import os  # Import the os module for environment variables
import pandas as pd  # type: ignore
from typing import Callable, Optional, Union  # Import Callable and Union
from datetime import (date, datetime)

def warn(*a):
    print(*a, file=sys.stderr)


class MyMaria:
    def __init__(self, verbose: bool = False, config_file: str = "", conf: str = "default"):
        # Use environment variable for default config file location
        self.verbose = verbose
        self.conn = None
        self.cursor = None
        self.engine = None
        if not config_file:
            self.config_file = os.path.join(os.path.expanduser("~"), ".config", "mymaria.ini")
        else:
            self.config_file = config_file
        self.conf = conf
        self.load_config()
        self.connect()

    def __str__(self) -> str:
        return "MyMaria:" + self.conf

    def __repr__(self) -> str:
        return self.__str__()

    def __del__(self):
        """Destructor - automatically close connections when the object is destroyed."""
        if self.verbose:
            warn("MyMaria object is being destroyed. Attempting to close connections.")
        self.close()

    def load_config(self):
        """Loads database connection parameters from a config file."""
        config = configparser.ConfigParser()
        try:
            config.read_file(open(self.config_file))
            db_config = config[self.conf]
            self.host = db_config.get('host', 'localhost')  # Default to localhost if not found
            self.port = db_config.getint('port', 3306)  # Default to 3306 if not found
            self.user = db_config.get('user', 'none')
            self.password = db_config.get('password', 'none')
            self.database = db_config.get('database', self.conf)
        except (FileNotFoundError, KeyError, configparser.Error) as e:
            raise ValueError(f"Error loading '{self.database}'config from {self.config_file}: {e}")

    def verb(self, *a):
        if self.verbose:
            warn(*a)

    def connect(self):
        """Establishes a database connection."""
        try:
            conn = mariadb.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database)
            self.conn = conn
            self.cursor = self.conn.cursor()
            self.engine = sqlalchemy.create_engine(
                f"mariadb+mariadbconnector://{self.user}:{self.password}@{self.host}/{self.database}")

        except mariadb.Error as e:
            raise ValueError(f"Error connecting to the database: {e}")

    def close(self):
        """Closes the database connection."""
        if self.conn:
            self.cursor.close()
            self.conn.close()
            if self.verbose:
                warn("Database connection closed.")
        if self.engine:
            self.engine.dispose()
            if self.verbose:
                warn("SQLAlchemy engine disposed")

    def exec(self, query):
        # execute simple direct sql
        if self.verbose:
            warn("exec query")
            warn(query)
        self.cursor.execute(query)
        self.conn.commit()

    def create_table_from_csv(
        self, csv_filepath: str, table_name: str,
        transform: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,  # Correct type hint
    ):
        """
        Creates a new table in the database based on the structure of a CSV file.

        Args:
            csv_filepath (str): The path to the CSV file.
            table_name (str): The name of the table to create.
        """
        df = pd.read_csv(csv_filepath, nrows=10)  # Read a few rows to infer types
        return self.create_table_from_df(df, table_name, transform=transform)


    def create_table_from_df(
        self, df: pd.DataFrame, table_name: str,
        transform: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,  # Correct type hint
    ):
        """
        Creates a new table in the database based on the structure of a DataFrame.

        Args:
            df (pd.DataFrame): The path to the CSV file.
            table_name (str): The name of the table to create.
        """
        try:
            if transform:
                df = transform(df)


            # Map pandas dtypes to SQLAlchemy types
            type_mapping = {
                "object": sqlalchemy.String(255),
                "int64": sqlalchemy.Integer,
                "float64": sqlalchemy.Float,
                "bool": sqlalchemy.Boolean,
                "datetime64[ns]": sqlalchemy.DateTime,
            }

            columns = []
            for col_name, dtype in df.dtypes.items():
                sample = df[col_name].iloc[0]
                column_type = None

                if str(dtype) in ["datetime64[ns]"]:
                    column_type = sqlalchemy.DateTime
                elif isinstance(sample, date):
                    column_type = sqlalchemy.Date
                elif isinstance(sample, datetime):
                    column_type = sqlalchemy.DateTime
                elif isinstance(sample, bool):
                    column_type = sqlalchemy.Boolean
                elif str(dtype) in type_mapping:
                    column_type = type_mapping[str(dtype)]
                else:
                    column_type = sqlalchemy.String(255)  # Default to String if type not recognized, ADD LENGTH
                
                if self.verbose:
                    
                    typ = str(type(sample))
                    print(f"____ set col {col_name} => {str(dtype)} => {column_type} [[ {typ} ]]")


                # Add a length to string columns
                if isinstance(column_type, sqlalchemy.String):
                  column_type = sqlalchemy.String(255)  # Assign a default length for string
                  # TODO: maybe use 2xmax len observed ?
                if self.verbose:
                    warn(f"New table column: {col_name} => {str(dtype)} => {column_type}")
                columns.append(sqlalchemy.Column(col_name, column_type))

            metadata = sqlalchemy.MetaData()
            table = sqlalchemy.Table(table_name, metadata, *columns)
            print(table)

            metadata.create_all(self.engine)
            self.verb(f"Table '{table_name}' created successfully.")

        except Exception as e:
            warn(f"An unexpected error occurred: {e}")


    def load_data_to_mariadb(
        self,
        data: Union[str, pd.DataFrame],  # Accept either a filepath or a DataFrame
        table_name: str,
        temp_table: str = None,
        create_table: bool = False,
        chunksize: int = 10000,
        transform: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,  # Correct type hint
    ):
        """
        Loads data into a MariaDB table, from either a CSV file or a pandas DataFrame.

        Args:
            data: Either a file path (str) to a CSV or a pandas DataFrame
            table_name (str): The name of the table to load the data into.
            temp_table (str): use temp table to stage insert data.
            chunksize (int): The number of rows to load at a time (for CSV).
            transform (Callable[[pd.DataFrame], pd.DataFrame], optional): A function to transform
            each DataFrame chunk before loading it into the database.
            Defaults to None.
        """
        insert_table = table_name
        try:
            # Create a session
            Session = sessionmaker(bind=self.engine)
            session = Session()

            # Check if the table exists
            inspector = sqlalchemy.inspect(self.engine)
            if not inspector.has_table(table_name):
                if create_table:
                    warn(f"Table '{table_name}' does not exist. Attempting to create it from data structure.")
                    if isinstance(data, str):
                       self.create_table_from_csv(data, table_name, transform=transform)
                    elif isinstance(data, pd.DataFrame):
                        self.create_table_from_df(data, table_name, transform=transform)

                    inspector = sqlalchemy.inspect(self.engine)
                    if not inspector.has_table(table_name):
                        warn(f"Failed to create table '{table_name}'")
                        return
                else:
                    warn(f"Table '{table_name}' does not exist. Set to create with create=True")
                    return

            self.verb(f"Loading data into table '{table_name}'")

            if temp_table:
                self.exec(f"CREATE TABLE IF NOT EXISTS {temp_table} as select * from {table_name} limit 0")
                self.exec(f"DELETE FROM {temp_table} where 1=1")
                insert_table = temp_table

            # Get table columns
            columns = [col['name'] for col in inspector.get_columns(table_name)]

            # Load and process data
            if isinstance(data, str):
              self.verb(f"Loading data from '{data}' into table '{table_name}'")

              # Infer data types from the first few rows of CSV
              # also read into a small dataframe
              dtype_from_data: dict = {}
              sample_df = pd.read_csv(data, nrows=10)
              if transform:
                  sample_df = transform(sample_df)

              for col in sample_df:
                  dtype_from_data[col] = sample_df[col].dtype

              # Use pandas to read the CSV in chunks and load into the database
              for chunk in pd.read_csv(data, chunksize=chunksize):
                  # filter columns not in table
                  chunk = chunk[[col for col in chunk.columns if col in columns]]
                  if transform:
                      chunk = transform(chunk)

                  self._insert_chunk(chunk, insert_table, session, dtype_from_data, columns)
            elif isinstance(data, pd.DataFrame):
              self.verb(f"Loading data from DataFrame into table '{table_name}'")
              dtype_from_data = {}
              for col in data:
                  dtype_from_data[col] = data[col].dtype

              # filter columns not in table
              data = data[[col for col in data.columns if col in columns]]
              if transform:
                  data = transform(data)
              self._insert_chunk(data, insert_table, session, dtype_from_data, columns)
            else:
                raise ValueError("Invalid data type. Must be a filepath (str) or a DataFrame.")

            self.verb(f"Successfully loaded data into table '{insert_table}'")
            if temp_table:
                self.exec(f"INSERT IGNORE INTO {table_name} (SELECT * FROM {temp_table})")
                self.verb(f"Successfully loaded data from '{temp_table}' into table '{table_name}'")
        except FileNotFoundError:
            warn(f"Error: CSV file not found at '{data}'")
        except ValueError as ve:
            warn(f"Value Error: {ve}")
        except Exception as e:
            warn(f"An unexpected error occurred: {e}")

    def load_csv_to_mariadb(self, *args, **kwargs):
      # alias with a warning
      warn("Warning: load_csv_to_mariadb is deprecated. Use load_data_to_mariadb")
      self.load_data_to_mariadb(*args, **kwargs)

    def _insert_chunk(self, chunk, insert_table, session, dtype_from_data, columns):
      """
      Helper function to insert a chunk of data into the database.
      """
      # Define dtype (SQLAlchemy data types for the table)
      dtype = {}
      for column in columns:
          if column in dtype_from_data:
              if dtype_from_data[column] == object:
                  dtype[column] = sqlalchemy.String(255) #Add length for load as well
              elif dtype_from_data[column] == int:
                  dtype[column] = sqlalchemy.Integer
              elif dtype_from_data[column] == float:
                  dtype[column] = sqlalchemy.Float
              elif column.lower() in ["quote_day", "expiration_date", "date"]:
                  dtype[column] = sqlalchemy.Date
              elif column.lower() in ["quote_time"]:
                  dtype[column] = sqlalchemy.DateTime
              else:
                  dtype[column] = sqlalchemy.String(255) #Add length for load as well

      # check for all columns
      for column in columns:
          if column not in chunk:
              warn(f"Warning: Column {column} found in db table, but not in data.  This will be ignored")
      try:
          chunk.to_sql(name=insert_table, con=self.engine, if_exists='append', index=False, dtype=dtype)
          session.commit()  # Commit each chunk for efficiency
          self.verb(f"Loaded {len(chunk)} rows into table '{insert_table}'")
      except SQLAlchemyError as e:
          warn(f"Error inserting data: {e}")
          session.rollback()  # Rollback the transaction in case of error
