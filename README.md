# MariaIO: A Python Module for MariaDB Interaction and CSV Data Loading

MariaIO is a Python module designed to simplify interactions with MariaDB databases, particularly for loading data from CSV files. It leverages SQLAlchemy for robust database connectivity and pandas for efficient data manipulation. This utility aims to streamline the process of populating MariaDB tables with data from CSV sources, while also offering flexibility in data transformation and table management.

## Features

*   **Easy Connection Management:** Manages database connections, configuration loading, and connection pooling.
*   **CSV Data Loading:** Loads data from CSV files into MariaDB tables efficiently, with support for large files via chunking.
*   **Automatic Table Creation:** Creates new tables based on the structure of CSV files if they don't already exist.
*   **Data Transformation:** Supports custom data transformation functions to modify data before loading.
*   **Temporary Table Support:** Offers the option to use temporary tables for staging data before final insertion.
*   **Type Inference:** Automatically infers column data types from CSV files for seamless table creation and data loading.
*   **Robust String Handling:** Handles string (VARCHAR) data types by assigning a default length (255) to prevent errors during table creation.
*   **Verbose Mode:** Includes a verbose mode for debugging and observing the module's actions.

## Installation

```bash
pip install .
```

## Configuration
MariaIO uses a configuration file (mymaria.ini) to store database connection details. The default location is ~/.config/mymaria.ini, the default config name is 'default'. You can override this location by providing a config_file parameter when instantiating the MyMaria class.

### Example ~/.config/database.ini:
```ini
[default]
host = localhost
port = 3306
user = your_user
password = your_password
database = mydatabase

[otherdatabase]
host = other_host
port = 3306
user = your_other_user
password = your_other_password
database = mydatabase
```
The host, port, user, and password should be replaced with your MariaDB credentials.

## Usage

### Built in csv2table app

*csv2table()* is an app handler that may be used to for a quick cmdline tool development  
```
from mariaio import csv2table
if __name__ == "__main__":
    csv2table()
```

Example for special cases of required data transformation
```

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform a dataframe for insertion to database 

    This may enforce datatypes , add/rename columns, etc

    This function is passed to the load_data_to_mariadb function

    reset ms timestamp to seconds, 
    add new columns quote_time and quote_date
    """
    # Convert milliseconds to seconds *in place*
    df['time'] = (df['time'] / 1000).astype(int)  
    # Create datetime and date objects using the correct column
    df['quote_time'] = pd.to_datetime(df['time'], unit='s')
    df['quote_date'] = df['quote_time'].dt.date
    return df


if __name__ == "__main__":
    csv2table(transform_func=transform)

```

See dev/csv2table_chains.py  dev/csv2table_simple.py for testing

### Basic Connection and Table Operations

```
from mariaio import MyMaria

try:
    # Create a MyMaria object (verbose mode enabled)
    db = MyMaria(verbose=True, database="mydatabase")

    # Create a table (if it doesn't exist)
    db.exec("CREATE TABLE IF NOT EXISTS test (id INT, name VARCHAR(255), value FLOAT)")

    # Insert some data
    db.exec("INSERT INTO test (id, name, value) VALUES (1, 'Alice', 3.14)")
    db.exec("INSERT INTO test (id, name, value) VALUES (2, 'Bob', 2.71)")

except Exception as e:
    print(f"An unexpected error occurred: {e}")
```

### Loading Data from CSV into an Existing Table

```
from mariaio import MyMaria
import pandas as pd

try:
    # Create a MyMaria object
    db = MyMaria(verbose=True)

    # Ensure the table test exist
    db.exec("CREATE TABLE IF NOT EXISTS test (id INT, name VARCHAR(255), value FLOAT)")

    # Create a sample CSV for testing
    csv_data = {'id': [3, 4], 'name': ['Charlie', 'David'], 'value': [1.2, 2.2]}
    df = pd.DataFrame(csv_data)
    df.to_csv('/tmp/data.csv', index=False)

    # Load data from CSV into an existing table
    db.load_csv_to_mariadb("/tmp/data.csv", "test")

except Exception as e:
    print(f"An unexpected error occurred: {e}")
```

### Loading Data into a New Table from CSV

If a table for the CSV does not exist, this module may create one for you. 
This is convenient for expediency.  After table creation, you may admin the database for indexes of interest.
or datatype tuning. 

```
from mariaio import MyMaria
import pandas as pd

try:
    # Create a MyMaria object
    db = MyMaria(verbose=True)

    # Create a sample CSV for testing
    csv_data = {'symbol': ["A", "B", "C"], 'strike': [10.0, 12.0, 14.0], 'type': ["CALL", "PUT", "CALL"]}
    df = pd.DataFrame(csv_data)
    df.to_csv('/tmp/chains.csv', index=False)

    # Load data from CSV into a new table (table will be created)
    db.load_csv_to_mariadb("/tmp/chains.csv", "my_new_table", create_table=True)

except Exception as e:
    print(f"An unexpected error occurred: {e}")
```

### Loading Data with Transformation and Temporary Table

This is sometimes more efficient on large dataset indexes. 

This example refers to *./dev/20250227_chains.csv* in the repository.

The temporary table will be created if it does not exist.
The temporary table will be truncated if it does exist.

```
from mariaio import MyMaria
import pandas as pd

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms a DataFrame by converting time from milliseconds to seconds and creating
    'quote_time' (datetime) and 'quote_date' (date) columns.

    Args:
        df: The input DataFrame containing a 'time' column in milliseconds.

    Returns:
        A transformed DataFrame with added 'quote_time' and 'quote_date' columns.
    """
    # Convert milliseconds to seconds in place
    df['time'] = df['time'] / 1000  
    # Create datetime and date objects
    df['quote_time'] = pd.to_datetime(df['time'], unit='s')
    df['quote_date'] = df['quote_time'].dt.date
    return df

try:
    # Create a MyMaria object
    db = MyMaria(verbose=True)

    # Load data, applying transformation and using a temporary table
    db.load_csv_to_mariadb(
        "./dev/20250227_chains.csv",
        "chains",
        temp_table="chains_tmp",
        transform=transform,
        create_table=True  # if table does not exist, create it.
    )

except Exception as e:
    print(f"An unexpected error occurred: {e}")
```

## Class MyMaria Methods
* MyMaria(verbose: bool = False, config_file: str = "", config: str = "")
    * Initializes the MyMaria object and establishes a connection to the database.
    * Parameters:
        *  verbose: Enables verbose output for debugging.
        * config_file: Specifies the path to the configuration file.
        * database: Specifies the database section in the config file.
* exec(self, query)
    * Executes a raw SQL query.
* create_table_from_csv(self, csv_filepath: str, table_name: str, transform)
    * Creates a new table based on the structure of a CSV file.
    * Parameters:
        * csv_filepath: The path to the CSV file.
        * table_name: The name of the table to create.
        * transform: function to transform the dataframe before getting its types.
* load_csv_to_mariadb(self, csv_filepath: str, table_name: str, temp_table: str = None, create_table: bool = False, chunksize: int = 10000, transform: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None)
    * Loads data from a CSV file into a MariaDB table.
    * Parameters:
        * csv_filepath: The path to the CSV file.
        * table_name: The name of the table to load into.
        * temp_table: The name of a temporary table to use (optional).
        * create_table: if true, create table if it does not exist.
        * chunksize: The number of rows to load at a time.
        * transform: A function to transform each DataFrame chunk before loading (optional).

## Dependencies
* pandas
* sqlalchemy
* mariadb
* configparser (included in Python's standard library)

## License
MIT License

