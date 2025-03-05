
"""
This is example of using transform functionality with csv2table 


python csv2table_chains.py -i 20250227_chains.csv -t test_chains -c -v



"""

import sys
sys.path.insert(0, '../src')
sys.path.insert(0, './src')


# from mariaio import CSVtoTable
from mariaio import csv2table
import pandas as pd




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
