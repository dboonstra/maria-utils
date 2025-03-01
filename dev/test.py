import sys
sys.path.insert(0,'../src')
sys.path.insert(0,'./src')
from mariaio import MyMaria
import pandas as pd

try:
    # Create a MyMaria object
    # uses default config_file and config name
    db = MyMaria(verbose=True) 

    # Example usage
    db.exec("CREATE TABLE IF NOT EXISTS test (id INT, name VARCHAR(255), value FLOAT)")
    db.exec("INSERT INTO test (id, name, value) VALUES (1, 'Alice', 3.14)")
    db.exec("INSERT INTO test (id, name, value) VALUES (2, 'Bob', 2.71)")

    # Load data from a CSV file
    # Create a sample csv for testing
    csv_data = {'id': [3, 4], 'name': ['Charlie', 'David'], 'value':[1.2, 2.2]}
    df = pd.DataFrame(csv_data)
    df.to_csv('/tmp/data.csv', index=False)


    db.load_csv_to_mariadb("/tmp/data.csv", "test")

except Exception as e:
    print(f"An unexpected error occurred: {e}")
