import pyodbc

uid = "benchmark"
pwd = "B3nch_mark"
connection_string = (
    r"DRIVER={ODBC Driver 18 for SQL Server};"
    r"SERVER=LAPTOP-DM-2.local;"
    r"DATABASE=vectordb;"
    f"UID={uid};PWD={pwd};TrustServerCertificate=Yes"
)
cnxn = pyodbc.connect(connection_string, autocommit=True)
crsr = cnxn.cursor()

print(crsr.execute("SELECT SCHEMA_NAME()").fetchval())  # default schema

line_items = [(1, "[1,2,3]"), (2,  '[1,2,3]')]
dummy = 1
sql = "EXEC dbo.stp_load_pippo @dummy=?, @payload=?"
params = (dummy, line_items)
crsr.execute(sql, params)
