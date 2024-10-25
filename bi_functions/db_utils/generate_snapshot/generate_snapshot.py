# export_views.py

import os
import psycopg2
from psycopg2 import sql
from settings import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, OUTPUT_DIR, DB_SCHEMA, VIEW_GRANT_USER

show_view_creation_header = False

def clear_output_directory(directory):
    """
    Clear all files in the output directory.
    """
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
        except Exception as e:
            print(f"Failed to delete {file_path}. Reason: {e}")

def get_views(cursor):
    """
    Fetch all views from the PostgreSQL database.
    """
    query = f"""
    SELECT table_name
    FROM information_schema.views
    WHERE table_schema IN ('{DB_SCHEMA}');
    """
    cursor.execute(query)
    return cursor.fetchall()

def view_record_to_dict(record):
    return {"schemaname" : record[0], "viewname"	:record[1] ,"viewowner" :record[2],	"definition" :record[3]}

def get_view_definition(cursor, view_name):
    """
    Fetch the definition of a specific view from the PostgreSQL database.
    """
    query = sql.SQL("SELECT schemaname , viewname ,viewowner , definition FROM pg_views WHERE viewname = %s and schemaname = %s ")
    cursor.execute(query, [view_name,DB_SCHEMA])
    result = cursor.fetchone()
    return view_record_to_dict(result) if result else None

def save_view_to_file(view_name, view_definition_record):
    """
    Save the view definition to a file.
    """
    if not view_definition_record:
        print(f"View '{view_name}' has no definition. Skipping...")
        return

    output_path = os.path.join(OUTPUT_DIR, f"{view_name}.sql")
    with open(output_path, "w") as file:
        if show_view_creation_header:
            view_content =f"""
            --
            -- Name: "{view_definition_record["viewname"]}"; Type: VIEW; Schema: {DB_SCHEMA}; Owner: "{view_definition_record["viewowner"]}"
            --
            DROP VIEW {DB_SCHEMA}."{view_definition_record["viewname"]}";

            CREATE VIEW {DB_SCHEMA}."{view_definition_record["viewname"]}" AS
            {view_definition_record["definition"]}

            ALTER VIEW {DB_SCHEMA}."{view_definition_record["viewname"]}" OWNER TO "{view_definition_record["viewowner"]}";

            GRANT ALL ON TABLE {DB_SCHEMA}."{view_definition_record["viewname"]}" TO "{VIEW_GRANT_USER}";
            """
        else :
            view_content =f"""
            {view_definition_record["definition"]}
            """        
        file.write(view_content)

def main():
    print("Snapshot sync started")
    # Ensure the output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Clear the output directory
    clear_output_directory(OUTPUT_DIR)
    print(f"Cleaning folder {OUTPUT_DIR}")

    try:
        print(f"Dumping snapshots into: {OUTPUT_DIR}")
        # Establish a connection to the PostgreSQL database
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = connection.cursor()

        # Get all views in the database
        views = get_views(cursor)

        if not views:
            print("No views found in the database.")
            return

        # Process each view
        for view in views:
            view_name = view[0]
            view_definition_record = get_view_definition(cursor, view_name)
            save_view_to_file(view_name, view_definition_record)

    except Exception as error:
        print(f"Error: {error}")
    finally:
        print(f"Snapshots sync end")
        if connection:
            cursor.close()
            connection.close()

if __name__ == "__main__":
    main()
