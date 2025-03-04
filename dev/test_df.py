import sys
sys.path.insert(0,'../src')
sys.path.insert(0,'./src')
from mariaio import MyMaria
import pandas as pd

# table for data insertion
testtable = 'test_df'

# Create a MyMaria object
# uses default config_file and config name
db = MyMaria(verbose=True) 
db.exec(f"DROP table if exists {testtable}")
data = {'id': [3, 4], 'name': ['DF_lisa', 'DF_jett'], 'value':[1.2, 2.2]}
df = pd.DataFrame(data)

try:
    # Load data from a pd dataframe
    # set create_table=True 
    db.load_data_to_mariadb(df, testtable, create_table=True)

except Exception as e:
    print(f"An unexpected error occurred: {e}")
