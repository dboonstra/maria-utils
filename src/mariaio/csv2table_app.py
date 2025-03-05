import sys
import argparse
import pandas as pd
from typing import Callable, Optional  # Import Callable and Union
from mariaio import MyMaria


def getopts():
    parser = argparse.ArgumentParser(
        prog="csv2table",
        description="load csv file to mariadb table",
        epilog="csv2table is app module (mariaio.csv2table_app)",
    )
    parser.add_argument(
        "-i",
        "--infile",
        action="store",
        type=str,
        help="csv file to load",
    )
    parser.add_argument(
        "-t",
        "--table",
        action="store",
        type=str,
        help="mariadb table for load",
    )
    parser.add_argument(
        "-tt",
        "--temptable",
        action="store",
        type=str,
        help="mariadb table for load",
    )
    parser.add_argument(
        "--dbconfig",
        action="store",
        type=str,
        help="name of database configuration of mymaria.ini [default default]",
    )
    parser.add_argument(
        "-n",
        "--dbname",
        action="store",
        default="default",
        type=str,
        help="name of database configuration of mymaria.ini [default default]",
    )
    parser.add_argument(
        "-c",
        "--create",
        action="store_true",
        default=False,
        help="all table create if not existing",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        help="be verbose",
    )
    return parser.parse_args()




def transform_sample(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform a dataframe for insertion to database 

    This may enforce datatypes , add/rename columns, etc

    This function is passed to the load_data_to_mariadb function
    """
    # Convert milliseconds to seconds *in place*
    df['time'] = (df['time'] / 1000).astype(int)  
    # Create datetime and date objects using the correct column
    df['quote_time'] = pd.to_datetime(df['time'], unit='s')
    df['quote_date'] = df['quote_time'].dt.date
    return df

def transform(df: pd.DataFrame) -> pd.DataFrame:
    # Returns an empty dataframe as a stub
    # see transform_sample for example usage 
    return df # return unchanged

def csv2table(transform_func: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None):  # Correct type hint

    opts = getopts()
    db = MyMaria(verbose=opts.verbose, conf=opts.dbname, config_file=opts.dbconfig)
    db.load_data_to_mariadb(
        opts.infile, 
        opts.table, 
        temp_table=opts.temptable, 
        create_table=opts.create, 
        transform=transform_func,
        )



if __name__ == "__main__":
    sys.exit(csv2table(transform_func=transform))
