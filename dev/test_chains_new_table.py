import sys
sys.path.insert(0, '../src')
sys.path.insert(0, './src')
from mariaio import MyMaria
import pandas as pd


def transform(df: pd.DataFrame) -> pd.DataFrame:
    # Convert milliseconds to seconds *in place*
    df['time'] = df['time'] / 1000  
    # Create datetime and date objects using the correct column
    df['quote_time'] = pd.to_datetime(df['time'], unit='s')
    df['quote_date'] = df['quote_time'].dt.date
    return df


try:
    # Create a MyMaria object
    db = MyMaria(verbose=True, database="fin")

    # Example usage
    db.load_csv_to_mariadb("20250227_chains.csv", "chains_created", create_table=True, transform=transform)

except Exception as e:
    print(f"An unexpected error occurred: {e}")

