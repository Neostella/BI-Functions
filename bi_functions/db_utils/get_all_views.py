import psycopg2

def get_all_views(
            dbname = None ,
            user = None ,
            password = None ,
            host = None ,
            port = None ):
    
    # Establish a database connection
    try:

        db_params = {
            'dbname': dbname,
            'user': user,
            'password': password,
            'host': host,
            'port': port
        }


        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # SQL query to get all views
        query = """
        SELECT table_schema, table_name, view_definition
        FROM information_schema.views
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name;
        """
        
        cursor.execute(query)
        views = cursor.fetchall()
        
        # Process each view
        for schema, name, definition in views:
            # Define the filename
            filename = f"create_view_{name}.sql"
            
            # Write the view definition to the file
            with open(filename, 'w') as file:
                file.write(f"-- View: {name}\n")
                file.write(f"-- Schema: {schema}\n\n")
                file.write(f"{definition};\n")
            
            print(f"Created file: {filename}")
    
    except Exception as e:
        print(f"An error occurred: {e}")
    
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()