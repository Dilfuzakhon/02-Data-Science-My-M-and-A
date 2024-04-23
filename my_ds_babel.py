import sqlite3

def csv_to_sql(my_m_and_a, db_name, table_name):
    conn = sqlite3.connect(db_name)
    my_m_and_a.to_sql(table_name, conn, if_exists='replace', index=False)
    print("Successfully converted csv to sql")
    conn.close()
