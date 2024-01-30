from sqlalchemy import create_engine, MetaData
import urllib.parse
import pyodbc
import pandas as pd

server = "11.111.11.111"
database = "dbname_staging"
username = "db_username"
password = "db_password"

connection_string = ("Driver={SQL Server};" 
    "Server=" + server +";" 
    "Database=" + database +";" 
    "UID=" + username + ";" 
    "PWD=" + password)

url = urllib.parse.quote(connection_string)
engine = create_engine("mssql+pyodbc:///?odbc_connect=" + url, use_setinputsizes=False)

mask_columns = {
    'dbo.TableName1': ['ColumnName1', 'ColumnName2'],
    'dbo.TableName2': ['ColumnName1', 'ColumnName2']}

try: 
    connection = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine, schema="dbo")

    table_names = metadata.tables.keys()
    df_dict = dict()
    for count, table in enumerate(table_names):
        print(count)
        print(table)
        df = pd.DataFrame()
        df = pd.read_sql(f"SELECT * FROM {table}", connection)
        print(df.columns.tolist())
        
        if table in mask_columns:
            for column in mask_columns[table]:
                if column in df.columns:
                    df[column] = df[column].apply(lambda x: 'masked' if x is not None and x != '' else x)
        df_dict[table] = df

    with pd.ExcelWriter('tsbci_db_export.xlsx', engine='xlsxwriter') as writer:
        for sheet_name, df in df_dict.items():
            df.to_excel(writer, sheet_name=sheet_name[4:], index=False)

except Exception as e:
    print(f"Error: {e}")

finally:
    if connection:
        connection.close()