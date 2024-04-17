import sqlite3 


def create_table(conn, table_name, fields):
    """
    Create a table in the database.

    Args:
        conn (sqlite3.Connection): SQLite database connection.
        table_name (str): Name of the table to create.
        fields (list): List of field definitions in the format "field_name field_type".

    Returns:
        None
    """
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
    """
    Insert data into the table in the database.

    Args:
        conn (sqlite3.Connection): SQLite database connection.
        table_name (str): Name of the table to insert data into.
        data (list): List of tuples representing data to be inserted into the table.

    Returns:
        None
    """
    cursor = conn.cursor()

    print(f"Inserting {len(data)} records into the table {table_name}...")

    # Retrieve column names from the table
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [column[1] for column in cursor.fetchall()]

    # Insert data into the table
    for row in data:
        # Replace empty values with None
        row_values = [value if value != '' else None for value in row]
        
        # Adjust the number of values to match the number of columns
        num_values = len(row_values)
        if num_values < len(columns):
            row_values.extend([None] * (len(columns) - num_values))
        
        cursor.execute(f"INSERT INTO {table_name}  VALUES ({', '.join(['?'] * len(columns))})", row_values)

    # Commit changes and close connection
    conn.commit()


def create_and_insert_data(validation_file, product_file):
    """
    Create a table in the database and insert data into it.

    Args:
        validation_file (str): Path to the validation file containing table name and field definitions.
        product_file (str): Path to the product file containing data to be inserted into the table.

    Returns:
        None
    """
    # Read table name and field names/types from the validation file
    table_name = None
    fields = []
    with open(validation_file, 'r') as file:
        for line in file:
            line = line.strip()
            if line.startswith("[") and line.endswith("]"):
                table_name = line[1:-1]
            elif line.startswith("Field"):
                field_name, field_type = line.split('=')[1].split(',')
                fields.append(f"{field_name.strip()} {field_type.strip()}")

    # Read data from product file to insert in the database
    data = []
    with open(product_file, 'r') as file:
        for line in file:
            line = line.strip()
            if line:
                data.append(tuple(line.split(',')))

    # Connect to the database
    conn = sqlite3.connect('database.db')
    # Create table
    create_table(conn, table_name, fields)

    # Insert data
    insert_data(conn, table_name, data)

    # Close the connection
    conn.close()