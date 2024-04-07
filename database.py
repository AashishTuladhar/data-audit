import sqlite3
import services 

def create_table(conn, table_name, fields):
    cursor = conn.cursor()
    # Create the table
    if table_name and fields:
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(fields)})")
        print("Table created successfully:", table_name)
    else:
        print("Error: Table name or field definitions are missing.")

    # Commit changes and close connection
    conn.commit()



def insert_data(conn, table_name, data):
    cursor = conn.cursor()

    print(f"Inserting {len(data)} records into the table {table_name}...")
    # Insert data into the table
    cursor.executemany(f"INSERT INTO {table_name} VALUES ({', '.join(['?'] * len(data[0]))})", data)

    # Commit changes and close connection
    conn.commit()
    


def create_and_insert_data(validaion_file,product_file):
    # Read table name and field names/types from the file from validaion_file
    table_name = None
    fields = []
    with open(validaion_file, 'r') as file:
        for line in file:
            line = line.strip()
            if line.startswith("[") and line.endswith("]"):
                table_name = line[1:-1]
            elif line.startswith("Field"):
                field_name, field_type = line.split('=')[1].split(',')
                fields.append(f"{field_name.strip()} {field_type.strip()}")

    #Read data from product file to insert in db
    data = []
    with open(product_file, 'r') as file:
        for line in file:
            line = line.strip()
            if line:
                data.append(tuple(line.split(',')))

    # Connect to the database 
    conn = sqlite3.connect('database.db')
    #create table
    create_table(conn, table_name, fields)

    # Insert data
    insert_data(conn, table_name, data)

    #close the connection
    conn.close()
