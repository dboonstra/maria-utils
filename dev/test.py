import sys
sys.path.insert(0,'../src')
sys.path.insert(0,'./src')
import os
from mariaio import MyMaria
import pandas as pd

tmpfile = '/tmp/test_data.csv'
testtable = 'test_csv'

# Create a MyMaria object
# uses default config_file and config name
db = MyMaria(verbose=True) 

# Example usage
db.exec(f"CREATE TABLE IF NOT EXISTS {testtable} (id INT, name VARCHAR(255), value FLOAT)")
db.exec(f"INSERT INTO {testtable} (id, name, value) VALUES (1, 'Alice', 3.14)")
db.exec(f"INSERT INTO {testtable} (id, name, value) VALUES (2, 'Bob', 2.71)")

# Load data from a CSV file
# Create a sample csv for testing
csv_data = {'id': [3, 4], 'name': ['Charlie', 'David'], 'value':[1.2, 2.2]}
df = pd.DataFrame(csv_data)
df.to_csv(tmpfile, index=False)

try:
    db.load_data_to_mariadb(tmpfile, testtable)

except Exception as e:
    print(f"An unexpected error occurred: {e}")

os.remove(tmpfile)
