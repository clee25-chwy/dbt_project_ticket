import duckdb
import pandas as pd

sql_query = '''
show tables
'''

with duckdb.connect('data/nyc_parking_violations.db') as con:
    print(con.sql(sql_query).df())

sql_query_import_1 = '''
CREATE OR REPLACE TABLE parking_violation_codes AS
SELECT *
FROM read_csv_auto(
    'data/dof_parking_violation_codes.csv',
    normalize_names=True
)
'''

sql_query_import_2 = '''
CREATE OR REPLACE TABLE parking_violations_2023 AS
SELECT *
FROM read_csv_auto(
    'data/parking_violations_issued_fiscal_year_2023_sample.csv',
    normalize_names=True
)
'''

with duckdb.connect('data/nyc_parking_violations.db') as con:
    con.sql(sql_query_import_1)
    con.sql(sql_query_import_2)