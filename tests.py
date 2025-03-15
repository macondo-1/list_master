import sqlite3
import os

os.chdir()

conn = sqlite3.connect("datafile.db")
cursor = conn.cursor()

"""#cursor.execute("create table data (id integer primary key, name text,email text, validation bool)")

cursor.execute("insert into data (name, email) values ('alberto', 'alberto@test.com')")

conn.commit()
conn.close()"""

#cursor.execute("insert into data (name, email, validation) values ('gabriel', 'gabriel@test.com', TRUE)")

cursor.execute("create table data (id integer primary key, name text,email text, validation bool)")
#result = cursor.execute("select * from data")
#print(result.fetchall())
conn.close()