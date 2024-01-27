import pyodbc
import struct
import binascii

uid = "benchmark"
pwd = "B3nch_mark"
connection_string = (
    r"DRIVER={ODBC Driver 18 for SQL Server};"
    r"SERVER=LAPTOP-DM-2.local;"
    r"DATABASE=vectordb;"
    f"UID={uid};PWD={pwd};TrustServerCertificate=Yes"
)

b = bytearray()
b.append([169, 170])

items:float = [100, 2000, 1, 0, -1, 0.3, 200]

b += bytearray(struct.pack("i", len(items)))

b.append([0,0])

for i in range(len(items)):
    b += bytearray(struct.pack("f", items[i]))
    

cnxn = pyodbc.connect(connection_string, autocommit=True)
crsr = cnxn.cursor()
crsr.execute("insert into dbo.test_vector_binary ([vector]) values (?)", b)
crsr.close()
