# Module Import
import mariadb  # type: ignore
import sys
import sqlalchemy  # type: ignore
from sqlalchemy.orm import sessionmaker  # type: ignore
from sqlalchemy.exc import SQLAlchemyError  # type: ignore
import configparser  # Import the configparser module
import os  # Import the os module for environment variables
import pandas as pd  # type: ignore
from typing import Callable, Optional  # Import Callable

def warn(*a):
    print(*a, file=sys.stderr)


class MyMaria:
    def __init__(self, verbose: bool = False, config_file: str = "", database: str = ""):
        # Use environment variable for default config file location
        if not database:
            raise ValueError("MyMaria.database is not defined")
        if not config_file:
            self.config_file = os.path.join(os.path.expanduser("~"), ".config", "database.ini")
        else:
            self.config_file = config_file
        self.verbose = verbose
        self.database = database
        self.load_config()
        self.connect()

    def __repr__(self) -> str:
        return "MyMaria:" + self.database

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
            db_config = config[self.database]
            self.host = db_config.get('host', 'localhost')  # Default to localhost if not found
            self.port = db_config.getint('port', 3306)  # Default to 3306 if not found
            self.user = db_config.get('user', 'none')
            self.password = db_config.get('password', 'none')
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
        try:
            df = pd.read_csv(csv_filepath, nrows=10)  # Read a few rows to infer types
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
                if str(dtype) in ["datetime64[ns]"]:
                    column_type = sqlalchemy.DateTime
                elif str(dtype) in type_mapping:
                    column_type = type_mapping[str(dtype)]
                else:
                    column_type = sqlalchemy.String(255)  # Default to String if type not recognized, ADD LENGTH

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

        except FileNotFoundError:
            warn(f"Error: CSV file not found at '{csv_filepath}'")
        except Exception as e:
            warn(f"An unexpected error occurred: {e}")

    def load_csv_to_mariadb(
        self,
        csv_filepath: str,
        table_name: str,
        temp_table: str = None,
        create_table: bool = False,
        chunksize: int = 10000,
        transform: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,  # Correct type hint
    ):
        """
        Loads data from a CSV file into a MariaDB table using SQLAlchemy.

        Args:
            csv_filepath (str): The path to the CSV file.
            table_name (str): The name of the table to load the data into.
            temp_table (str): use temp table to stage insert data.
            chunksize (int): The number of rows to load at a time.
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
                    warn(f"Table '{table_name}' does not exist. Attempting to create it from CSV structure.")
                    self.create_table_from_csv(csv_filepath, table_name, transform=transform)
                    inspector = sqlalchemy.inspect(self.engine)
                    if not inspector.has_table(table_name):
                        warn(f"Failed to create table '{table_name}'")
                        return
                else:
                    warn(f"Table '{table_name}' does not exist. Set to create with create=True")
                    return
        
            self.verb(f"Loading data from '{csv_filepath}' into table '{table_name}'")

            if temp_table:
                self.exec(f"CREATE TABLE IF NOT EXISTS {temp_table} as select * from {table_name} limit 0")
                self.exec(f"DELETE FROM {temp_table} where 1=1")
                insert_table = temp_table

            # Get table columns
            columns = [col['name'] for col in inspector.get_columns(table_name)]

            # Infer data types from the first few rows of CSV
            # also read into a small dataframe
            dtype_from_csv: dict = {}
            sample_df = pd.read_csv(csv_filepath, nrows=10)
            if transform:
                sample_df = transform(sample_df)

            for col in sample_df:
                dtype_from_csv[col] = sample_df[col].dtype

            # Define dtype (SQLAlchemy data types for the table)
            dtype = {}
            for column in columns:
                if column in dtype_from_csv:
                    if dtype_from_csv[column] == object:
                        dtype[column] = sqlalchemy.String(255) #Add length for load as well
                    elif dtype_from_csv[column] == int:
                        dtype[column] = sqlalchemy.Integer
                    elif dtype_from_csv[column] == float:
                        dtype[column] = sqlalchemy.Float
                    elif column.lower() in ["quote_day", "expiration_date", "date"]:
                        dtype[column] = sqlalchemy.Date
                    elif column.lower() in ["quote_time"]:
                        dtype[column] = sqlalchemy.DateTime
                    else:
                        dtype[column] = sqlalchemy.String(255) #Add length for load as well

            # check for all columns
            for column in columns:
                if column not in sample_df:
                    warn(f"Warning: Column {column} found in db table, but not in csv.  This will be ignored")

            # Use pandas to read the CSV in chunks and load into the database
            for chunk in pd.read_csv(csv_filepath, chunksize=chunksize):
                # filter columns not in table
                chunk = chunk[[col for col in chunk.columns if col in columns]]
                if transform:
                    chunk = transform(chunk)

                try:
                    chunk.to_sql(name=insert_table, con=self.engine, if_exists='append', index=False,
                                 dtype=dtype)  # Pass dtype here instead of astype
                    session.commit()  # Commit each chunk for efficiency
                    self.verb(f"Loaded {len(chunk)} rows into table '{table_name}'")
                except SQLAlchemyError as e:
                    warn(f"Error inserting data: {e}")
                    session.rollback()  # Rollback the transaction in case of error

            self.verb(f"Successfully loaded data from '{csv_filepath}' into table '{insert_table}'")
            if temp_table:
                self.exec(f"INSERT IGNORE INTO {table_name} (SELECT * FROM {temp_table})")
                self.verb(f"Successfully loaded data from '{temp_table}' into table '{table_name}'")

        except FileNotFoundError:
            warn(f"Error: CSV file not found at '{csv_filepath}'")
        except Exception as e:
            warn(f"An unexpected error occurred: {e}")
