import sqlite3
import pandas as pd


connection = sqlite3.connect("shipment_database.db")
cursor = connection.cursor()

cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()

print("Database Schema:")
for table_name in tables:
    table_name = table_name[0]
    print(f"\nTable: {table_name}")
    cursor.execute(f"PRAGMA table_info({table_name});")
    columns = cursor.fetchall()
    for col in columns:
        # print(f"  Column: {column[1]}, Type: {column[2]}")
        print(f"  - {col[1]} (Type: {col[2]}, Primary Key: {'Yes' if col[5] == 1 else 'No'})")

    cursor.execute(f"PRAGMA foreign_key_list({table_name});")
    foreign_keys = cursor.fetchall()
    if foreign_keys:
        print("Foreign Keys:")
        for fk in foreign_keys:
            print(f"  - Column {fk[3]} references {fk[2]}({fk[4]})")
    else:
        print("Foreign Keys: None")





# Step 1: Create the new shipment table with the foreign key constraint
connection.execute("""
    CREATE TABLE shipment_new (
        id INTEGER PRIMARY KEY,
        product_id INTEGER,
        quantity INTEGER,
        origin TEXT,
        destination TEXT,
        FOREIGN KEY (product_id) REFERENCES product(id)
    )
""")

# Step 2: Copy data from the old shipment table to the new one
connection.execute("""
    INSERT INTO shipment_new (id, product_id, quantity, origin, destination)
    SELECT id, product_id, quantity, origin, destination FROM shipment
""")

# Step 3: Drop the old shipment table
connection.execute("DROP TABLE shipment")

# Step 4: Rename the new table to the original name
connection.execute("ALTER TABLE shipment_new RENAME TO shipment")

# Commit the changes and close the connection
connection.commit()

print("Foreign key constraint enforced successfully!")




df0 = pd.read_csv("data/shipping_data_0.csv")
df1 = pd.read_csv("data/shipping_data_1.csv")
df2 = pd.read_csv("data/shipping_data_2.csv")



unique_products = df0['product'].dropna().unique()



for product in unique_products:

    cursor.execute("""
        INSERT OR IGNORE INTO product (name) VALUES (?)
    """, (product,))


connection.commit()

print("Unique products added to the products table.")



for _, row in df0.iterrows():
    # Fetch product_id from the products table
    cursor.execute("SELECT id FROM product WHERE name = ?", (row['product'],))
    product_id = cursor.fetchone()
    
    if product_id:  # Ensure the product exists in the products table
        product_id = product_id[0]  # Extract the product_id from the tuple
        
        # Insert data into the shipment table
        cursor.execute("""
            INSERT INTO shipment (product_id, quantity, origin, destination)
            VALUES (?, ?, ?, ?)
        """, (product_id, row['product_quantity'], row['origin_warehouse'], row['destination_store']))
    else:
        print(f"Warning: Product '{row['product']}' not found in products table. Skipping.")

# Commit changes and close connection
connection.commit()

print("Data successfully inserted into the shipment table.")



df1['is_new_group'] = df1['product'].ne(df1['product'].shift()) | df1['shipment_identifier'].ne(df1['shipment_identifier'].shift())
df1['group_id'] = df1['is_new_group'].cumsum()
grouped_df = df1.groupby(['group_id', 'shipment_identifier', 'product']).size().reset_index(name='count')

# Step 2: Merge with df2 to add origin and destination
grouped_df = grouped_df.merge(df2, on='shipment_identifier', how='left')


for _, row in grouped_df.iterrows():
    # Fetch product_id from the products table or insert if not exists
    cursor.execute("SELECT id FROM product WHERE name = ?", (row['product'],))
    product_id = cursor.fetchone()
    
    if not product_id:
        # Insert the product if not found
        cursor.execute("INSERT INTO product (name) VALUES (?)", (row['product'],))
        product_id = cursor.lastrowid  # Get the new product_id
        print(product_id)
    else:
        product_id = product_id[0]  # Extract the product_id from the tuple
    
    # Insert the consolidated data into the shipment table
    cursor.execute("""
        INSERT INTO shipment (product_id, quantity, origin, destination)
        VALUES (?, ?, ?, ?)
    """, (product_id, row['count'], row['origin_warehouse'], row['destination_store']))

# Commit changes and close connection
connection.commit()

print("Consolidated data successfully inserted into the shipment table.")
