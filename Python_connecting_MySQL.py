import pandas as pd
import mysql.connector
import os

# List of CSV files and their corresponding table names
csv_files = [
    ('customers.csv', 'customers'),
    ('orders.csv', 'orders'),
    ('sellers.csv', 'sellers'),
    ('products.csv', 'products'),
    ('geolocation.csv', 'geolocation'),
    ('payments.csv', 'payments'),
    ('order_items.csv', 'order_items')
]

# Connect to the MySQL database with SSL disabled
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='@123456789#',
    database='ecommerce',
    ssl_disabled=True,
    autocommit=True
)
cursor = conn.cursor()

# Folder containing the CSV files
folder_path = r'F:\Project\archive'


def get_sql_type(dtype):
    """Returns appropriate SQL data type based on pandas dtype"""
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'


for csv_file, table_name in csv_files:
    file_path = os.path.join(folder_path, csv_file)

    # Check if file exists before proceeding
    if not os.path.exists(file_path):
        print(f"Warning: {csv_file} not found. Skipping...")
        continue

    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(file_path)

    # Replace NaN with None to handle SQL NULL values
    df = df.where(pd.notnull(df), None)

    # Debugging: Check for NaN values
    print(f"Processing {csv_file}")
    print(f"NaN values before replacement:\n{df.isnull().sum()}\n")

    # Clean column names
    df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]

    # Generate the CREATE TABLE statement with appropriate data types
    columns = ', '.join([f'`{col}` {get_sql_type(df[col].dtype)}' for col in df.columns])
    create_table_query = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})'
    cursor.execute(create_table_query)

    # Prepare SQL insert statement
    sql = f"INSERT INTO `{table_name}` ({', '.join(['`' + col + '`' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(df.columns))})"

    # Insert in batches to prevent large query overload
    batch_size = 1000
    for i in range(0, len(df), batch_size):
        chunk = df.iloc[i : i + batch_size]
        try:
            cursor.executemany(sql, [tuple(None if pd.isna(x) else x for x in row) for row in chunk.itertuples(index=False)])
        except mysql.connector.Error as err:
            print(f"❌ Error inserting into {table_name}: {err}")
            conn.rollback()

# Close the connection
conn.close()

