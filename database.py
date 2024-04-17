import sqlite3


def create_table(conn, app):
    """
    Create a table in the database.

    Args:
        conn (sqlite3.Connection): SQLite database connection.
        app (Validation class): Contains the validation file details which includes table details
        mapped into our Validation class.

    Returns:
        None
    """
    cursor = conn.cursor()
    # Create the table
    if app:
        query = f"CREATE TABLE IF NOT EXISTS {app.table_name} ("

        for field in app.fields.get_all_fields():
            query += (f"{field.field_name} {field.field_type} "
                      f"{'PRIMARY KEY' if field.field_name == app.fields.primary_key_field.field_name else ''} "
                      f"{'NOT NULL' if field.field_name in map(lambda x: x.field_name, app.fields.not_null_fields) else ''}, ")

        query = query.rstrip(', ') + ')'

        cursor.execute(query)
        print("Table created successfully:", app.table_name)
    else:
        print("Error: Table name or field definitions are missing.")

    # Commit changes and close connection
    conn.commit()


def insert_data(conn, app, data):
    """
    Insert data into the table in the database.

    Args:
        conn (sqlite3.Connection): SQLite database connection.
        app (Validation class): Contains the validation file details which includes table details
        mapped into our Validation class.
        data (list): List of tuples representing data to be inserted into the table.

    Returns:
        None
    """

    try:
        cursor = conn.cursor()

        print(f"Inserting into table {app.table_name}...\n")

        # Retrieve column names from the table
        cursor.execute(f"PRAGMA table_info({app.table_name})")
        columns = [column[1] for column in cursor.fetchall()]

        # Insert data into the table
        for row in data:
            # Replace empty values with None
            row_values = [value.replace("'", '') if value != '' else None for value in row]

            # Adjust the number of values to match the number of columns
            num_values = len(row_values)
            if num_values < len(columns):
                row_values.extend([None] * (len(columns) - num_values))

            cursor.execute(f"INSERT INTO {app.table_name}  VALUES ({', '.join(['?'] * len(columns))})", row_values)

        # Commit changes and close connection
        conn.commit()

        print(f"Successfully processed {len(data)} rows.\n")
        print('\nData import completed.')
    except sqlite3.Error as error:
        print(f"Error {error.sqlite_errorname}:", error.__str__())


def create_and_insert_data(app, product_file):
    """
    Create a table in the database and insert data into it.

    Args:
        app (Validation class): Contains the validation file details which includes table details
        mapped into our Validation class.
        product_file (str): Path to the product file containing data to be inserted into the table.

    Returns:
        None
    """

    try:
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
        create_table(conn, app)

        # Insert data
        insert_data(conn, app, data)

        # Close the connection
        conn.close()
    except sqlite3.Error as error:
        print(error)
