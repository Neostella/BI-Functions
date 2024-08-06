import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Database connection parameters from environment variables
db_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

def get_all_views():
    # Establish a database connection
    try:
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