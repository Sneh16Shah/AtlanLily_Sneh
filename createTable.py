import psycopg2

db_params = {
    "host": "localhost",  
    "database": "metadata_db", 
    "user": "postgres", 
    "password": "123456"  
}

# SQL command to create the metadata_events table
create_table_sql = """
CREATE TABLE metadata_events (
    id SERIAL PRIMARY KEY,
    entity_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    metadata_info TEXT NOT NULL
);
"""

# SQL command to create the metadata_events table
create_table_sql1 = """
CREATE TABLE auth_table (
    id SERIAL PRIMARY KEY,
    uname TEXT NOT NULL,
    password TEXT NOT NULL,
    role TEXT NOT NULL
);
"""

#SQL command to select all data from metadata_events
# select_query="""
# SELECT * FROM metadata_events
# """
# Connect to the PostgreSQL database
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

# Execute the SQL command to create the table
cursor.execute(create_table_sql)
cursor.execute(create_table_sql1)
# cursor.execute(select_query)

# data = cursor.fetchall()
# print(data)

conn.commit()
cursor.close()
conn.close()

print("metadata_events table created.")
