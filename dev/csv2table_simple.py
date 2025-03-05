

"""
This is simple from for using cvv2table mariaio.csv2table_app 


$ python csv2table_simple.py -i data.csv -t test_app -v -c 
    ^^
    reads data.csv , creates tablename test_app, inserts into test_app 


usage: csv2table [-h] [-i INFILE] [-t TABLE] [-tt TEMPTABLE] [--dbconfig DBCONFIG] [-n DBNAME] [-c] [-v]

load csv file to mariadb table

options:
  -h, --help            show this help message and exit
  -i INFILE, --infile INFILE
                        csv file to load
  -t TABLE, --table TABLE
                        mariadb table for load
  -tt TEMPTABLE, --temptable TEMPTABLE
                        mariadb table for load
  --dbconfig DBCONFIG   name of database configuration of mymaria.ini [default default]
  -n DBNAME, --dbname DBNAME
                        name of database configuration of mymaria.ini [default default]
  -c, --create          all table create if not existing
  -v, --verbose         be verbose

csv2table is app module (mariaio.csv2table_app)


"""
import sys
sys.path.insert(0, '../src')
sys.path.insert(0, './src')

from mariaio import csv2table

if __name__ == "__main__":
    csv2table()
