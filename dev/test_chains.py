import sys
sys.path.insert(0, '../src')
sys.path.insert(0, './src')
from mariaio import MyMaria
import pandas as pd

testtable = 'test_chains'
temptable = 'test_chains_tmp'

def transform(df: pd.DataFrame) -> pd.DataFrame:
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


# Create a MyMaria object
# uses default config_file and config name
db = MyMaria(verbose=True)

try:
    # Example usage
    db.load_data_to_mariadb("20250227_chains.csv", testtable, temp_table=temptable, transform=transform)

except Exception as e:
    print(f"An unexpected error occurred: {e}")

